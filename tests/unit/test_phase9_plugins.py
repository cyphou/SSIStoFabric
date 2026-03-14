"""Phase 9 — Plugin Architecture tests.

Covers:
- TransformationStrategy protocol compliance
- ComponentRegistry register/unregister/get
- @component_handler decorator
- HookManager register/fire/unregister/clear
- @hook decorator
- ExpressionRule + custom rule injection
- Plugin discovery (entry_points mock)
- Generator integration (custom handler dispatch)
- Engine hook firing (pre_plan, post_plan, pre_generate, post_generate)
- reset_all helper
"""

from __future__ import annotations

import re
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from ssis_to_fabric.engine.plugin_registry import (
    ComponentRegistry,
    HookManager,
    TransformationStrategy,
    clear_expression_rules,
    component_handler,
    discover_plugins,
    get_component_registry,
    get_expression_rules,
    get_hook_manager,
    hook,
    register_expression_rule,
    reset_all,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clean_registry():
    """Ensure a clean plugin state for each test."""
    reset_all()
    yield
    reset_all()


class _DummyComponent:
    """Minimal stand-in for DataFlowComponent."""

    def __init__(self, component_type_value: str = "Custom Transform"):
        self.name = "test_comp"
        self.component_type = type("CT", (), {"value": component_type_value})()
        self.columns = []
        self.properties = {}


class _ValidHandler:
    """Handler that satisfies TransformationStrategy."""

    def can_handle(self, component: Any) -> bool:
        return True

    def generate_pyspark(self, component: Any, context: dict) -> str:
        return f"# Custom PySpark for {component.name}"

    def generate_m(self, component: Any, prev_step: str, context: dict) -> tuple[str, str]:
        return ("CustomStep", f"Table.Custom({prev_step})")


class _PartialHandler:
    """Missing generate_m — should NOT satisfy Protocol."""

    def can_handle(self, component: Any) -> bool:
        return True

    def generate_pyspark(self, component: Any, context: dict) -> str:
        return "# partial"


# ===================================================================
# TransformationStrategy Protocol
# ===================================================================


class TestTransformationStrategy:
    def test_valid_handler_is_instance(self):
        assert isinstance(_ValidHandler(), TransformationStrategy)

    def test_partial_handler_not_instance(self):
        assert not isinstance(_PartialHandler(), TransformationStrategy)

    def test_protocol_has_required_methods(self):
        methods = {"can_handle", "generate_pyspark", "generate_m"}
        for m in methods:
            assert hasattr(TransformationStrategy, m)


# ===================================================================
# ComponentRegistry
# ===================================================================


class TestComponentRegistry:
    def test_register_and_get(self):
        reg = ComponentRegistry()
        handler = _ValidHandler()
        reg.register("Custom Transform", handler)
        assert reg.get("Custom Transform") is handler

    def test_get_missing_returns_none(self):
        reg = ComponentRegistry()
        assert reg.get("NonExistent") is None

    def test_has(self):
        reg = ComponentRegistry()
        handler = _ValidHandler()
        reg.register("TestComp", handler)
        assert reg.has("TestComp")
        assert not reg.has("Other")

    def test_unregister(self):
        reg = ComponentRegistry()
        reg.register("TestComp", _ValidHandler())
        reg.unregister("TestComp")
        assert not reg.has("TestComp")

    def test_unregister_missing_is_noop(self):
        reg = ComponentRegistry()
        reg.unregister("NonExistent")  # no error

    def test_registered_types(self):
        reg = ComponentRegistry()
        reg.register("A", _ValidHandler())
        reg.register("B", _ValidHandler())
        assert sorted(reg.registered_types) == ["A", "B"]

    def test_clear(self):
        reg = ComponentRegistry()
        reg.register("A", _ValidHandler())
        reg.clear()
        assert reg.registered_types == []

    def test_register_invalid_handler_raises(self):
        reg = ComponentRegistry()
        with pytest.raises(TypeError, match="TransformationStrategy"):
            reg.register("Bad", _PartialHandler())

    def test_overwrite_handler(self):
        reg = ComponentRegistry()
        h1 = _ValidHandler()
        h2 = _ValidHandler()
        reg.register("Comp", h1)
        reg.register("Comp", h2)
        assert reg.get("Comp") is h2


# ===================================================================
# @component_handler decorator
# ===================================================================


class TestComponentHandlerDecorator:
    def test_decorator_registers_class(self):
        @component_handler("DecoratedComp")
        class MyHandler:
            def can_handle(self, component):
                return True

            def generate_pyspark(self, component, context):
                return "# decorated"

            def generate_m(self, component, prev_step, context):
                return ("Step", "expr")

        reg = get_component_registry()
        assert reg.has("DecoratedComp")
        handler = reg.get("DecoratedComp")
        assert handler.generate_pyspark(None, {}) == "# decorated"

    def test_decorator_returns_class(self):
        @component_handler("ReturnTest")
        class MyHandler:
            def can_handle(self, c):
                return True

            def generate_pyspark(self, c, ctx):
                return ""

            def generate_m(self, c, ps, ctx):
                return ("", "")

        assert MyHandler is not None
        assert isinstance(MyHandler(), TransformationStrategy)


# ===================================================================
# HookManager
# ===================================================================


class TestHookManager:
    def test_register_and_fire(self):
        hm = HookManager()
        results = []
        hm.register("pre_parse", lambda d: results.append(d))
        hm.fire("pre_parse", {"path": "/test"})
        assert results == [{"path": "/test"}]

    def test_fire_returns_data(self):
        hm = HookManager()
        result = hm.fire("pre_plan", {"count": 5})
        assert result == {"count": 5}

    def test_fire_chain_modification(self):
        hm = HookManager()
        hm.register("post_plan", lambda d: {**d, "extra": True})
        result = hm.fire("post_plan", {"items": 3})
        assert result == {"items": 3, "extra": True}

    def test_fire_multiple_hooks_in_order(self):
        hm = HookManager()
        order = []
        hm.register("pre_generate", lambda d: order.append("first"))
        hm.register("pre_generate", lambda d: order.append("second"))
        hm.fire("pre_generate")
        assert order == ["first", "second"]

    def test_register_unknown_event_raises(self):
        hm = HookManager()
        with pytest.raises(ValueError, match="Unknown lifecycle event"):
            hm.register("invalid_event", lambda d: None)

    def test_register_non_callable_raises(self):
        hm = HookManager()
        with pytest.raises(TypeError, match="callable"):
            hm.register("pre_parse", "not_callable")

    def test_unregister(self):
        hm = HookManager()
        cb = lambda d: None  # noqa: E731
        hm.register("pre_parse", cb)
        hm.unregister("pre_parse", cb)
        # Fire should do nothing
        result = hm.fire("pre_parse", {"x": 1})
        assert result == {"x": 1}

    def test_unregister_missing_is_noop(self):
        hm = HookManager()
        hm.unregister("pre_parse", lambda d: None)  # no error

    def test_clear_specific_event(self):
        hm = HookManager()
        hm.register("pre_parse", lambda d: None)
        hm.register("post_parse", lambda d: None)
        hm.clear("pre_parse")
        # pre_parse cleared, post_parse still has a hook
        assert len(hm._hooks["pre_parse"]) == 0
        assert len(hm._hooks["post_parse"]) == 1

    def test_clear_all(self):
        hm = HookManager()
        hm.register("pre_parse", lambda d: None)
        hm.register("post_parse", lambda d: None)
        hm.clear()
        for hooks in hm._hooks.values():
            assert len(hooks) == 0

    def test_events_property(self):
        hm = HookManager()
        events = hm.events
        assert "pre_parse" in events
        assert "post_generate" in events
        assert "pre_deploy" in events
        assert "post_deploy" in events
        assert len(events) == 8

    def test_fire_unknown_event_returns_empty(self):
        hm = HookManager()
        result = hm.fire("unknown_event")
        assert result == {}

    def test_fire_with_none_data(self):
        hm = HookManager()
        called = []
        hm.register("pre_parse", lambda d: called.append(d))
        hm.fire("pre_parse")
        assert called == [{}]

    def test_hook_exception_does_not_propagate(self):
        hm = HookManager()
        hm.register("pre_parse", lambda d: (_ for _ in ()).throw(RuntimeError("boom")))
        hm.register("pre_parse", lambda d: {**d, "ok": True})
        # The second hook should still run even if the first throws
        # (the exception is logged but not propagated via fire)
        # Since the lambda with throw is generator-based, test with a real callable
        hm.clear("pre_parse")

        def bad_hook(d):
            raise RuntimeError("boom")

        results = []
        hm.register("pre_parse", bad_hook)
        hm.register("pre_parse", lambda d: results.append("ran"))
        hm.fire("pre_parse", {"x": 1})
        assert results == ["ran"]


# ===================================================================
# @hook decorator
# ===================================================================


class TestHookDecorator:
    def test_decorator_registers_hook(self):
        calls = []

        @hook("post_generate")
        def my_hook(data):
            calls.append(data)

        hm = get_hook_manager()
        hm.fire("post_generate", {"status": "done"})
        assert calls == [{"status": "done"}]

    def test_decorator_returns_function(self):
        @hook("pre_deploy")
        def my_hook(data):
            pass

        assert callable(my_hook)


# ===================================================================
# ExpressionRule & Custom Rules
# ===================================================================


class TestExpressionRules:
    def test_register_and_retrieve(self):
        register_expression_rule(r"MY_FUNC\((.+?)\)", r"custom(\1)", target="pyspark")
        rules = get_expression_rules("pyspark")
        assert len(rules) == 1
        assert rules[0].target == "pyspark"

    def test_clear_rules(self):
        register_expression_rule(r"X", "Y")
        clear_expression_rules()
        assert get_expression_rules() == []

    def test_target_filtering(self):
        register_expression_rule(r"A", "B", target="pyspark")
        register_expression_rule(r"C", "D", target="m")
        register_expression_rule(r"E", "F", target="both")

        pyspark_rules = get_expression_rules("pyspark")
        m_rules = get_expression_rules("m")

        # pyspark gets pyspark + both
        assert len(pyspark_rules) == 2
        # m gets m + both
        assert len(m_rules) == 2

    def test_priority_ordering(self):
        register_expression_rule(r"A", "B", priority=1)
        register_expression_rule(r"C", "D", priority=10)
        register_expression_rule(r"E", "F", priority=5)

        rules = get_expression_rules()
        assert rules[0].priority == 10
        assert rules[1].priority == 5
        assert rules[2].priority == 1

    def test_rule_pattern_is_compiled(self):
        rule = register_expression_rule(r"MY_FUNC\((.+?)\)", r"result(\1)")
        assert isinstance(rule.pattern, re.Pattern)

    def test_rule_applied_in_pyspark_transpiler(self):
        register_expression_rule(
            r"CUSTOM_UPPER\((\w+)\)",
            r"F.upper(F.col(\"\1\"))",
            target="pyspark",
        )
        from ssis_to_fabric.engine.expression_transpiler import ExpressionTranspiler

        result = ExpressionTranspiler.to_pyspark("CUSTOM_UPPER(name)")
        assert "F.upper" in result

    def test_rule_applied_in_m_transpiler(self):
        register_expression_rule(
            r"CUSTOM_LOWER\((\w+)\)",
            r"Text.Lower([\1])",
            target="m",
        )
        from ssis_to_fabric.engine.expression_transpiler import ExpressionTranspiler

        result = ExpressionTranspiler.to_m("CUSTOM_LOWER(name)")
        assert "Text.Lower" in result

    def test_callable_replacement(self):
        def repl(m):
            return f"my_custom({m.group(1).upper()})"

        register_expression_rule(r"custom\((\w+)\)", repl, target="pyspark")
        rules = get_expression_rules("pyspark")
        assert len(rules) == 1
        test_str = "custom(hello)"
        result = rules[0].pattern.sub(rules[0].replacement, test_str)
        assert result == "my_custom(HELLO)"


# ===================================================================
# Generator Integration
# ===================================================================


class TestGeneratorIntegration:
    def test_spark_generator_uses_custom_handler(self):
        from ssis_to_fabric.analyzer.models import (
            DataFlowComponent,
            DataFlowComponentType,
        )
        from ssis_to_fabric.config import MigrationConfig

        # Register a custom handler for DERIVED_COLUMN
        handler = _ValidHandler()
        reg = get_component_registry()
        reg.register(DataFlowComponentType.DERIVED_COLUMN.value, handler)

        from ssis_to_fabric.engine.spark_generator import SparkNotebookGenerator

        gen = SparkNotebookGenerator(MigrationConfig())
        comp = DataFlowComponent(
            name="test_dc",
            component_type=DataFlowComponentType.DERIVED_COLUMN,
        )
        result = gen._generate_transformation(comp)
        assert "Custom PySpark for test_dc" in result

    def test_dataflow_generator_uses_custom_handler(self):
        from ssis_to_fabric.analyzer.models import (
            DataFlowComponent,
            DataFlowComponentType,
        )
        from ssis_to_fabric.config import MigrationConfig

        handler = _ValidHandler()
        reg = get_component_registry()
        reg.register(DataFlowComponentType.SORT.value, handler)

        from ssis_to_fabric.engine.dataflow_generator import DataflowGen2Generator

        gen = DataflowGen2Generator(MigrationConfig())
        comp = DataFlowComponent(
            name="test_sort",
            component_type=DataFlowComponentType.SORT,
        )
        step_name, expr = gen._transform_to_pq(comp, "PrevStep")
        assert step_name == "CustomStep"
        assert "Table.Custom(PrevStep)" in expr

    def test_builtin_handler_used_when_no_plugin(self):
        from ssis_to_fabric.analyzer.models import (
            DataFlowComponent,
            DataFlowComponentType,
        )
        from ssis_to_fabric.config import MigrationConfig
        from ssis_to_fabric.engine.spark_generator import SparkNotebookGenerator

        gen = SparkNotebookGenerator(MigrationConfig())
        comp = DataFlowComponent(
            name="builtin_sort",
            component_type=DataFlowComponentType.SORT,
        )
        result = gen._generate_transformation(comp)
        # Built-in sort handler should run (produces sort code or placeholder)
        assert isinstance(result, str)


# ===================================================================
# Engine Hook Integration
# ===================================================================


class TestEngineHookIntegration:
    def test_create_plan_fires_hooks(self):
        from ssis_to_fabric.analyzer.models import SSISPackage
        from ssis_to_fabric.config import MigrationConfig
        from ssis_to_fabric.engine.migration_engine import MigrationEngine

        events = []
        hm = get_hook_manager()
        hm.register("pre_plan", lambda d: events.append(("pre_plan", d)))
        hm.register("post_plan", lambda d: events.append(("post_plan", d)))

        engine = MigrationEngine(MigrationConfig())
        pkg = SSISPackage(name="TestPkg")
        engine.create_plan([pkg])

        assert len(events) == 2
        assert events[0][0] == "pre_plan"
        assert "TestPkg" in events[0][1]["packages"]
        assert events[1][0] == "post_plan"
        assert "plan" in events[1][1]

    def test_execute_fires_generate_hooks(self):
        from ssis_to_fabric.analyzer.models import SSISPackage
        from ssis_to_fabric.config import MigrationConfig
        from ssis_to_fabric.engine.migration_engine import MigrationEngine

        events = []
        hm = get_hook_manager()
        hm.register("pre_generate", lambda d: events.append("pre"))
        hm.register("post_generate", lambda d: events.append("post"))

        engine = MigrationEngine(MigrationConfig())
        pkg = SSISPackage(name="TestPkg2")
        engine.execute([pkg])

        assert "pre" in events
        assert "post" in events


# ===================================================================
# Plugin Discovery
# ===================================================================


class TestPluginDiscovery:
    def test_discover_no_plugins(self):
        loaded = discover_plugins(group="ssis_to_fabric.test_nonexistent")
        assert loaded == []

    def test_discover_loads_plugin(self):
        """Mock entry_points to simulate a plugin."""
        call_log = []

        def fake_plugin(registry, hooks, add_rule):
            call_log.append("loaded")
            registry.register("PluginComp", _ValidHandler())

        mock_ep = MagicMock()
        mock_ep.name = "test_plugin"
        mock_ep.value = "fake_module:fake_plugin"
        mock_ep.load.return_value = fake_plugin

        with patch("importlib.metadata.entry_points", return_value=[mock_ep]):
            loaded = discover_plugins()

        assert loaded == ["test_plugin"]
        assert call_log == ["loaded"]
        assert get_component_registry().has("PluginComp")

    def test_discover_handles_failing_plugin(self):
        mock_ep = MagicMock()
        mock_ep.name = "bad_plugin"
        mock_ep.value = "bad_module:bad_fn"
        mock_ep.load.return_value = MagicMock(side_effect=RuntimeError("fail"))

        with patch("importlib.metadata.entry_points", return_value=[mock_ep]):
            loaded = discover_plugins()

        assert loaded == []  # failed plugin not in list


# ===================================================================
# reset_all
# ===================================================================


class TestResetAll:
    def test_reset_clears_everything(self):
        reg = get_component_registry()
        reg.register("TestComp", _ValidHandler())
        hm = get_hook_manager()
        hm.register("pre_parse", lambda d: None)
        register_expression_rule(r"X", "Y")

        reset_all()

        assert reg.registered_types == []
        assert get_expression_rules() == []
        # All hooks cleared
        for hooks in hm._hooks.values():
            assert len(hooks) == 0


# ===================================================================
# Public API Exports
# ===================================================================


class TestPublicExports:
    def test_imports_from_package(self):
        from ssis_to_fabric import (
            ComponentRegistry,
            HookManager,
            TransformationStrategy,
            component_handler,
            discover_plugins,
            get_component_registry,
            get_hook_manager,
            hook,
            register_expression_rule,
        )

        assert ComponentRegistry is not None
        assert HookManager is not None
        assert TransformationStrategy is not None
        assert callable(component_handler)
        assert callable(hook)
        assert callable(discover_plugins)
        assert callable(get_component_registry)
        assert callable(get_hook_manager)
        assert callable(register_expression_rule)

    def test_entry_points_in_pyproject(self):
        """Verify pyproject.toml has the plugin entry_points group."""
        from pathlib import Path

        pyproject = Path(__file__).resolve().parents[2] / "pyproject.toml"
        content = pyproject.read_text()
        assert 'ssis_to_fabric.plugins' in content
