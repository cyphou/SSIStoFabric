"""
Plugin Registry & Hook System
==============================
Provides a component handler registry, lifecycle hook manager,
custom expression rule injection, and plugin discovery via
``entry_points``.

Usage::

    from ssis_to_fabric.engine.plugin_registry import (
        ComponentRegistry,
        HookManager,
        component_handler,
        hook,
    )

    # Register a custom transformation handler
    @component_handler("My Custom Transform")
    class MyHandler:
        def can_handle(self, component):
            return component.component_type.value == "My Custom Transform"

        def generate_pyspark(self, component, context):
            return "# Custom PySpark code"

        def generate_m(self, component, prev_step, context):
            return ("CustomStep", "Table.Custom(...)")

    # Register a lifecycle hook
    @hook("post_generate")
    def notify_on_complete(event_data):
        print(f"Generation complete: {event_data}")
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from ssis_to_fabric.analyzer.models import DataFlowComponent

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# TransformationStrategy Protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class TransformationStrategy(Protocol):
    """Protocol for custom component handlers.

    Implement this to provide generation logic for SSIS data-flow
    component types that the built-in generators do not support, or
    to override built-in handling with custom output.
    """

    def can_handle(self, component: DataFlowComponent) -> bool:
        """Return ``True`` if this strategy handles the given component."""
        ...

    def generate_pyspark(self, component: DataFlowComponent, context: dict[str, Any]) -> str:
        """Generate PySpark code for the component.

        *context* carries ``package_name``, ``task_name``, connections, etc.
        Return a multi-line Python/PySpark string.
        """
        ...

    def generate_m(
        self,
        component: DataFlowComponent,
        prev_step: str,
        context: dict[str, Any],
    ) -> tuple[str, str]:
        """Generate Power Query M for the component.

        Return ``(step_name, m_expression)``.
        """
        ...


# ---------------------------------------------------------------------------
# Expression Rule
# ---------------------------------------------------------------------------


@dataclass
class ExpressionRule:
    """A custom expression → target-code mapping rule.

    *pattern* is a compiled regex; *replacement* is either a static
    string or a ``Callable[[re.Match], str]``.  *target* selects which
    transpiler the rule applies to (``"pyspark"``, ``"m"``, or
    ``"both"``).
    """

    pattern: re.Pattern[str]
    replacement: str | Any  # str or Callable[[re.Match], str]
    target: str = "both"  # "pyspark", "m", or "both"
    priority: int = 0  # higher = applied first


# ---------------------------------------------------------------------------
# Component Registry
# ---------------------------------------------------------------------------


class ComponentRegistry:
    """Registry for custom component handlers.

    Handlers are looked up by the SSIS ``component_type.value`` string.
    Custom handlers take precedence over built-in generator logic.
    """

    def __init__(self) -> None:
        self._handlers: dict[str, TransformationStrategy] = {}

    def register(self, component_type: str, handler: TransformationStrategy) -> None:
        """Register *handler* for *component_type*.

        Raises ``TypeError`` if *handler* does not satisfy
        :class:`TransformationStrategy`.
        """
        if not isinstance(handler, TransformationStrategy):
            raise TypeError(
                f"Handler must implement TransformationStrategy protocol, "
                f"got {type(handler).__name__}"
            )
        self._handlers[component_type] = handler
        logger.info("Registered component handler for %r", component_type)

    def unregister(self, component_type: str) -> None:
        """Remove the handler for *component_type* (no-op if absent)."""
        self._handlers.pop(component_type, None)

    def get(self, component_type: str) -> TransformationStrategy | None:
        """Return the handler for *component_type*, or ``None``."""
        return self._handlers.get(component_type)

    def has(self, component_type: str) -> bool:
        """Check whether a custom handler is registered."""
        return component_type in self._handlers

    @property
    def registered_types(self) -> list[str]:
        """List all registered component type keys."""
        return list(self._handlers)

    def clear(self) -> None:
        """Remove all registered handlers."""
        self._handlers.clear()


# ---------------------------------------------------------------------------
# Hook Manager
# ---------------------------------------------------------------------------


_LIFECYCLE_EVENTS = frozenset({
    "pre_parse",
    "post_parse",
    "pre_plan",
    "post_plan",
    "pre_generate",
    "post_generate",
    "pre_deploy",
    "post_deploy",
})


class HookManager:
    """Lifecycle hook manager.

    Hooks are invoked in registration order.  Each callback receives
    ``(event_data: dict[str, Any])`` and may return a modified copy of
    *event_data* to pass to subsequent hooks (chain pattern).
    """

    def __init__(self) -> None:
        self._hooks: dict[str, list[Any]] = {e: [] for e in _LIFECYCLE_EVENTS}

    def register(self, event: str, callback: Any) -> None:
        """Register *callback* for *event*.

        Raises ``ValueError`` for unknown event names.
        """
        if event not in _LIFECYCLE_EVENTS:
            raise ValueError(
                f"Unknown lifecycle event {event!r}. "
                f"Valid events: {', '.join(sorted(_LIFECYCLE_EVENTS))}"
            )
        if not callable(callback):
            raise TypeError("Hook callback must be callable")
        self._hooks[event].append(callback)
        logger.debug("Registered hook for %r: %s", event, callback)

    def unregister(self, event: str, callback: Any) -> None:
        """Remove *callback* from *event* (no-op if absent)."""
        if event in self._hooks:
            import contextlib

            with contextlib.suppress(ValueError):
                self._hooks[event].remove(callback)

    def fire(self, event: str, data: dict[str, Any] | None = None) -> dict[str, Any]:
        """Invoke all hooks for *event* in order.

        Returns the (possibly modified) *data* dict.
        """
        if event not in self._hooks:
            return data or {}
        result = dict(data) if data else {}
        for cb in self._hooks[event]:
            try:
                ret = cb(result)
                if isinstance(ret, dict):
                    result = ret
            except Exception:
                logger.exception("Hook %r raised an error", event)
        return result

    def clear(self, event: str | None = None) -> None:
        """Clear hooks for a specific event, or all events."""
        if event:
            if event in self._hooks:
                self._hooks[event].clear()
        else:
            for lst in self._hooks.values():
                lst.clear()

    @property
    def events(self) -> list[str]:
        """List all valid lifecycle event names."""
        return sorted(_LIFECYCLE_EVENTS)


# ---------------------------------------------------------------------------
# Global Singletons
# ---------------------------------------------------------------------------

_component_registry = ComponentRegistry()
_hook_manager = HookManager()
_expression_rules: list[ExpressionRule] = []


def get_component_registry() -> ComponentRegistry:
    """Return the global component registry."""
    return _component_registry


def get_hook_manager() -> HookManager:
    """Return the global hook manager."""
    return _hook_manager


def get_expression_rules(target: str = "both") -> list[ExpressionRule]:
    """Return custom expression rules for *target* (``"pyspark"``, ``"m"``, ``"both"``)."""
    return sorted(
        [r for r in _expression_rules if r.target in (target, "both")],
        key=lambda r: -r.priority,
    )


def register_expression_rule(
    pattern: str,
    replacement: str | Any,
    *,
    target: str = "both",
    priority: int = 0,
) -> ExpressionRule:
    """Add a custom expression transpiler rule.

    Parameters
    ----------
    pattern : str
        Regex pattern to match in SSIS expressions.
    replacement : str or callable
        Replacement string (may use ``\\1`` back-references) or a
        callable ``(re.Match) -> str``.
    target : str
        ``"pyspark"``, ``"m"``, or ``"both"`` (default).
    priority : int
        Higher priority rules are applied first (default 0).

    Returns
    -------
    ExpressionRule
        The created rule object.
    """
    rule = ExpressionRule(
        pattern=re.compile(pattern, re.IGNORECASE),
        replacement=replacement,
        target=target,
        priority=priority,
    )
    _expression_rules.append(rule)
    logger.info("Registered expression rule: %s (target=%s)", pattern, target)
    return rule


def clear_expression_rules() -> None:
    """Remove all custom expression rules."""
    _expression_rules.clear()


# ---------------------------------------------------------------------------
# Decorator helpers
# ---------------------------------------------------------------------------


def component_handler(component_type: str):
    """Class decorator that registers a :class:`TransformationStrategy`.

    Usage::

        @component_handler("My Custom Transform")
        class MyHandler:
            def can_handle(self, component): ...
            def generate_pyspark(self, component, context): ...
            def generate_m(self, component, prev_step, context): ...
    """
    def decorator(cls):
        _component_registry.register(component_type, cls())
        return cls
    return decorator


def hook(event: str):
    """Function decorator that registers a lifecycle hook.

    Usage::

        @hook("post_generate")
        def my_hook(event_data):
            print(event_data)
    """
    def decorator(fn):
        _hook_manager.register(event, fn)
        return fn
    return decorator


# ---------------------------------------------------------------------------
# Plugin Discovery via entry_points
# ---------------------------------------------------------------------------


def discover_plugins(group: str = "ssis_to_fabric.plugins") -> list[str]:
    """Discover and load plugins via Python ``entry_points``.

    Each entry point should be a callable that accepts the
    component registry, hook manager, and expression rule registrar::

        def my_plugin(registry, hooks, add_expr_rule):
            registry.register("MyComp", MyHandler())
            hooks.register("post_generate", my_callback)
            add_expr_rule(r"MY_FUNC\\((.+?)\\)", "my_replacement(\\1)")

    Returns a list of loaded plugin names.
    """
    import sys

    loaded: list[str] = []

    if sys.version_info >= (3, 12):
        from importlib.metadata import entry_points
        eps = entry_points(group=group)
    else:
        from importlib.metadata import entry_points
        all_eps = entry_points()
        eps = all_eps.get(group, [])  # type: ignore[assignment]

    for ep in eps:
        try:
            plugin_fn = ep.load()
            plugin_fn(_component_registry, _hook_manager, register_expression_rule)
            loaded.append(ep.name)
            logger.info("Loaded plugin %r from %s", ep.name, ep.value)
        except Exception:
            logger.exception("Failed to load plugin %r", ep.name)

    return loaded


def reset_all() -> None:
    """Reset all registries to empty state (useful for testing)."""
    _component_registry.clear()
    _hook_manager.clear()
    clear_expression_rules()
