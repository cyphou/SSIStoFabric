"""Tests for SSIS parameter handling and parent-child package interactions."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from ssis_to_fabric.analyzer.dtsx_parser import DTSXParser
from ssis_to_fabric.config import MigrationConfig
from ssis_to_fabric.engine.data_factory_generator import DataFactoryGenerator
from ssis_to_fabric.engine.migration_engine import TargetArtifact

if TYPE_CHECKING:
    from ssis_to_fabric.analyzer.models import (
        SSISPackage,
    )

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "sample_packages"


@pytest.fixture
def parser() -> DTSXParser:
    return DTSXParser()


@pytest.fixture
def parent_child_package(parser: DTSXParser) -> SSISPackage:
    return parser.parse(FIXTURES_DIR / "parent_child.dtsx")


@pytest.fixture
def generator() -> DataFactoryGenerator:
    return DataFactoryGenerator(MigrationConfig())


# =========================================================================
# Parameter Parsing
# =========================================================================


class TestParameterParsing:
    """Tests for SSIS parameter parsing including container format."""

    @pytest.mark.unit
    def test_package_parameters_from_container(self, parent_child_package: SSISPackage) -> None:
        """Parameters inside <DTS:PackageParameters> container are parsed."""
        param_names = [p.name for p in parent_child_package.parameters]
        assert "Environment" in param_names
        assert "ServerName" in param_names
        assert "BatchSize" in param_names

    @pytest.mark.unit
    def test_parameter_values(self, parent_child_package: SSISPackage) -> None:
        """Parameter default values are extracted correctly."""
        params = {p.name: p for p in parent_child_package.parameters}
        assert params["Environment"].value == "DEV"
        assert params["ServerName"].value == "localhost"
        assert params["BatchSize"].value == "5000"

    @pytest.mark.unit
    def test_parameter_data_types(self, parent_child_package: SSISPackage) -> None:
        """Parameter data types are preserved."""
        params = {p.name: p for p in parent_child_package.parameters}
        assert params["Environment"].data_type == "String"
        assert params["BatchSize"].data_type == "Int32"

    @pytest.mark.unit
    def test_parameters_are_flagged(self, parent_child_package: SSISPackage) -> None:
        """All parsed parameters have is_parameter=True."""
        for p in parent_child_package.parameters:
            assert p.is_parameter is True

    @pytest.mark.unit
    def test_loose_parameter_format(self, parser: DTSXParser) -> None:
        """Parameters in loose format (direct children of root) are also parsed."""
        package = parser.parse(FIXTURES_DIR / "complex_etl.dtsx")
        param_names = [p.name for p in package.parameters]
        assert "Environment" in param_names

    @pytest.mark.unit
    def test_variables_parsed(self, parent_child_package: SSISPackage) -> None:
        """Package variables are parsed alongside parameters."""
        var_names = [v.name for v in parent_child_package.variables]
        assert "BatchID" in var_names
        assert "RunDate" in var_names


# =========================================================================
# Project-Level Parameters
# =========================================================================


class TestProjectParameters:
    """Tests for Project.params file parsing."""

    @pytest.mark.unit
    def test_project_params_parsed(self, parser: DTSXParser) -> None:
        """parse_directory picks up Project.params and attaches to packages."""
        packages = parser.parse_directory(FIXTURES_DIR)
        # At least one package should have project parameters
        pkgs_with_project_params = [p for p in packages if p.project_parameters]
        assert len(pkgs_with_project_params) > 0

    @pytest.mark.unit
    def test_project_param_names(self, parser: DTSXParser) -> None:
        """All expected project parameters are parsed."""
        packages = parser.parse_directory(FIXTURES_DIR)
        pkg = packages[0]  # Any package — project params are shared
        proj_param_names = [p.name for p in pkg.project_parameters]
        assert "Environment" in proj_param_names
        assert "ServerName" in proj_param_names
        assert "BatchSize" in proj_param_names

    @pytest.mark.unit
    def test_project_param_metadata(self, parser: DTSXParser) -> None:
        """Project parameter metadata (required, description) is captured."""
        packages = parser.parse_directory(FIXTURES_DIR)
        proj_params = {p.name: p for p in packages[0].project_parameters}
        assert proj_params["Environment"].required is True
        assert proj_params["Environment"].description == "Target environment"
        assert proj_params["BatchSize"].required is False

    @pytest.mark.unit
    def test_project_param_default_values(self, parser: DTSXParser) -> None:
        """Project parameter default values come from DesignDefaultValue."""
        packages = parser.parse_directory(FIXTURES_DIR)
        proj_params = {p.name: p for p in packages[0].project_parameters}
        assert proj_params["Environment"].value == "DEV"
        assert proj_params["BatchSize"].value == "10000"


# =========================================================================
# Execute Package Task — Parameter Bindings
# =========================================================================


class TestExecutePackageBindings:
    """Tests for Execute Package Task parameter binding extraction."""

    @pytest.mark.unit
    def test_parameter_bindings_parsed(self, parent_child_package: SSISPackage) -> None:
        """Parameter bindings are parsed from ParameterAssignment elements."""
        # Find "Extract Customers" task (nested in Parallel Extracts container)
        extract_cust = None
        for task in parent_child_package.control_flow_tasks:
            for child in task.child_tasks:
                if child.name == "Extract Customers":
                    extract_cust = child
                    break
        assert extract_cust is not None, "Could not find 'Extract Customers' task"
        assert len(extract_cust.parameter_bindings) == 2
        assert "Environment" in extract_cust.parameter_bindings
        assert "BatchID" in extract_cust.parameter_bindings

    @pytest.mark.unit
    def test_binding_variable_references(self, parent_child_package: SSISPackage) -> None:
        """Binding values correctly reference parent variables."""
        extract_cust = None
        for task in parent_child_package.control_flow_tasks:
            for child in task.child_tasks:
                if child.name == "Extract Customers":
                    extract_cust = child
                    break
        assert extract_cust is not None

        # $Package::Environment reference
        assert extract_cust.parameter_bindings["Environment"] == "$Package::Environment"
        # Simple variable reference
        assert extract_cust.parameter_bindings["BatchID"] == "BatchID"

    @pytest.mark.unit
    def test_partial_bindings(self, parent_child_package: SSISPackage) -> None:
        """Tasks with fewer bindings have only those bindings."""
        extract_orders = None
        for task in parent_child_package.control_flow_tasks:
            for child in task.child_tasks:
                if child.name == "Extract Orders":
                    extract_orders = child
                    break
        assert extract_orders is not None
        assert len(extract_orders.parameter_bindings) == 1
        assert "Environment" in extract_orders.parameter_bindings

    @pytest.mark.unit
    def test_no_bindings_when_absent(self, parent_child_package: SSISPackage) -> None:
        """Tasks without ParameterAssignment have empty bindings."""
        dim_loads = None
        for task in parent_child_package.control_flow_tasks:
            if task.name == "Dimension Loads":
                dim_loads = task
                break
        assert dim_loads is not None
        for child in dim_loads.child_tasks:
            assert len(child.parameter_bindings) == 0


# =========================================================================
# Container Precedence Constraints
# =========================================================================


class TestContainerPrecedenceConstraints:
    """Tests for precedence constraint parsing within containers."""

    @pytest.mark.unit
    def test_container_has_child_constraints(self, parent_child_package: SSISPackage) -> None:
        """Sequence containers parse their internal precedence constraints."""
        dim_loads = None
        for task in parent_child_package.control_flow_tasks:
            if task.name == "Dimension Loads":
                dim_loads = task
                break
        assert dim_loads is not None
        assert len(dim_loads.child_precedence_constraints) == 1

    @pytest.mark.unit
    def test_parallel_container_has_no_constraints(self, parent_child_package: SSISPackage) -> None:
        """Containers without constraints have empty child_precedence_constraints."""
        parallel = None
        for task in parent_child_package.control_flow_tasks:
            if task.name == "Parallel Extracts":
                parallel = task
                break
        assert parallel is not None
        assert len(parallel.child_precedence_constraints) == 0


# =========================================================================
# Pipeline Generation — Parameters & Variables
# =========================================================================


class TestPipelineParameterGeneration:
    """Tests for parameter and variable emission in generated pipelines."""

    @pytest.mark.unit
    def test_pipeline_has_parameters(
        self,
        generator: DataFactoryGenerator,
        parent_child_package: SSISPackage,
        tmp_path: Path,
    ) -> None:
        """Generated pipeline has parameters section from package parameters."""
        task_routing: dict[str, TargetArtifact] = {}
        for task in parent_child_package.control_flow_tasks:
            task_routing[task.name] = TargetArtifact.DATA_FACTORY_PIPELINE
            for child in task.child_tasks:
                task_routing[child.name] = TargetArtifact.DATA_FACTORY_PIPELINE

        output = generator.generate_package_pipeline(parent_child_package, task_routing, tmp_path)

        with open(output) as f:
            pipeline = json.load(f)

        assert "parameters" in pipeline["properties"]
        params = pipeline["properties"]["parameters"]
        assert "Environment" in params
        assert "ServerName" in params
        assert "BatchSize" in params

    @pytest.mark.unit
    def test_pipeline_has_variables(
        self,
        generator: DataFactoryGenerator,
        parent_child_package: SSISPackage,
        tmp_path: Path,
    ) -> None:
        """Generated pipeline has variables section from package variables."""
        task_routing: dict[str, TargetArtifact] = {}
        for task in parent_child_package.control_flow_tasks:
            task_routing[task.name] = TargetArtifact.DATA_FACTORY_PIPELINE
            for child in task.child_tasks:
                task_routing[child.name] = TargetArtifact.DATA_FACTORY_PIPELINE

        output = generator.generate_package_pipeline(parent_child_package, task_routing, tmp_path)

        with open(output) as f:
            pipeline = json.load(f)

        assert "variables" in pipeline["properties"]
        variables = pipeline["properties"]["variables"]
        assert "BatchID" in variables
        assert "RunDate" in variables

    @pytest.mark.unit
    def test_project_params_merged_into_pipeline(
        self,
        generator: DataFactoryGenerator,
        parser: DTSXParser,
        tmp_path: Path,
    ) -> None:
        """Project parameters are included in the pipeline parameters."""
        packages = parser.parse_directory(FIXTURES_DIR)
        # Find the parent_child package
        pkg = next(p for p in packages if p.name == "ParentChildPackage")

        task_routing: dict[str, TargetArtifact] = {}
        for task in pkg.control_flow_tasks:
            task_routing[task.name] = TargetArtifact.DATA_FACTORY_PIPELINE
            for child in task.child_tasks:
                task_routing[child.name] = TargetArtifact.DATA_FACTORY_PIPELINE

        output = generator.generate_package_pipeline(pkg, task_routing, tmp_path)

        with open(output) as f:
            pipeline = json.load(f)

        params = pipeline["properties"]["parameters"]
        # Project params should be present (even if overridden by package params)
        assert "Environment" in params
        assert "ServerName" in params
        assert "BatchSize" in params


# =========================================================================
# Pipeline Generation — InvokePipeline Parameter Passing
# =========================================================================


class TestInvokePipelineParameters:
    """Tests for parameter passing in InvokePipeline activities."""

    @pytest.mark.unit
    def test_execute_pipeline_has_parameters(
        self,
        generator: DataFactoryGenerator,
        parent_child_package: SSISPackage,
        tmp_path: Path,
    ) -> None:
        """InvokePipeline activities include parameters from bindings."""
        task_routing: dict[str, TargetArtifact] = {}
        for task in parent_child_package.control_flow_tasks:
            task_routing[task.name] = TargetArtifact.DATA_FACTORY_PIPELINE
            for child in task.child_tasks:
                task_routing[child.name] = TargetArtifact.DATA_FACTORY_PIPELINE

        output = generator.generate_package_pipeline(parent_child_package, task_routing, tmp_path)

        with open(output) as f:
            pipeline = json.load(f)

        activities = pipeline["properties"]["activities"]
        # Find the Extract Customers activity
        extract_cust = next(a for a in activities if a["name"] == "Extract_Customers")
        assert extract_cust["type"] == "ExecutePipeline"
        assert "parameters" in extract_cust["typeProperties"]
        params = extract_cust["typeProperties"]["parameters"]
        assert "Environment" in params
        assert "BatchID" in params

    @pytest.mark.unit
    def test_parameter_expressions_use_pipeline_params(
        self,
        generator: DataFactoryGenerator,
        parent_child_package: SSISPackage,
        tmp_path: Path,
    ) -> None:
        """Bound parameters use @pipeline().parameters expressions."""
        task_routing: dict[str, TargetArtifact] = {}
        for task in parent_child_package.control_flow_tasks:
            task_routing[task.name] = TargetArtifact.DATA_FACTORY_PIPELINE
            for child in task.child_tasks:
                task_routing[child.name] = TargetArtifact.DATA_FACTORY_PIPELINE

        output = generator.generate_package_pipeline(parent_child_package, task_routing, tmp_path)

        with open(output) as f:
            pipeline = json.load(f)

        activities = pipeline["properties"]["activities"]
        extract_cust = next(a for a in activities if a["name"] == "Extract_Customers")
        params = extract_cust["typeProperties"]["parameters"]

        assert params["Environment"]["value"] == "@pipeline().parameters.Environment"
        assert params["Environment"]["type"] == "Expression"
        assert params["BatchID"]["value"] == "@pipeline().parameters.BatchID"

    @pytest.mark.unit
    def test_no_parameters_when_no_bindings(
        self,
        generator: DataFactoryGenerator,
        parent_child_package: SSISPackage,
        tmp_path: Path,
    ) -> None:
        """ExecutePipeline without bindings still forwards parent parameters."""
        task_routing: dict[str, TargetArtifact] = {}
        for task in parent_child_package.control_flow_tasks:
            task_routing[task.name] = TargetArtifact.DATA_FACTORY_PIPELINE
            for child in task.child_tasks:
                task_routing[child.name] = TargetArtifact.DATA_FACTORY_PIPELINE

        output = generator.generate_package_pipeline(parent_child_package, task_routing, tmp_path)

        with open(output) as f:
            pipeline = json.load(f)

        activities = pipeline["properties"]["activities"]
        # Load DimDate has no explicit SSIS parameter bindings, but should
        # still receive forwarded parent pipeline parameters.
        load_dim = next(a for a in activities if a["name"] == "Load_DimDate")
        params = load_dim["typeProperties"].get("parameters", {})
        # Should have at least the project-level parameters forwarded
        assert len(params) > 0, "Parent parameters should be forwarded"
        # Each forwarded param should use the @pipeline().parameters expression
        for _p_name, p_val in params.items():
            assert p_val["type"] == "Expression"
            assert "@pipeline().parameters." in p_val["value"]


# =========================================================================
# Pipeline Generation — Parallel Semantics
# =========================================================================


class TestParallelSemantics:
    """Tests for parallel execution preservation when flattening containers."""

    @pytest.mark.unit
    def test_parallel_container_no_inter_dependencies(
        self,
        generator: DataFactoryGenerator,
        parent_child_package: SSISPackage,
        tmp_path: Path,
    ) -> None:
        """Children of a parallel container should NOT depend on each other."""
        task_routing: dict[str, TargetArtifact] = {}
        for task in parent_child_package.control_flow_tasks:
            task_routing[task.name] = TargetArtifact.DATA_FACTORY_PIPELINE
            for child in task.child_tasks:
                task_routing[child.name] = TargetArtifact.DATA_FACTORY_PIPELINE

        output = generator.generate_package_pipeline(parent_child_package, task_routing, tmp_path)

        with open(output) as f:
            pipeline = json.load(f)

        activities = pipeline["properties"]["activities"]
        extract_cust = next(a for a in activities if a["name"] == "Extract_Customers")
        extract_orders = next(a for a in activities if a["name"] == "Extract_Orders")

        # Both should depend on Initialize_Batch but NOT on each other
        cust_deps = {d["activity"] for d in extract_cust.get("dependsOn", [])}
        orders_deps = {d["activity"] for d in extract_orders.get("dependsOn", [])}

        assert "Initialize_Batch" in cust_deps
        assert "Initialize_Batch" in orders_deps
        assert "Extract_Orders" not in cust_deps
        assert "Extract_Customers" not in orders_deps

    @pytest.mark.unit
    def test_sequential_container_has_chain(
        self,
        generator: DataFactoryGenerator,
        parent_child_package: SSISPackage,
        tmp_path: Path,
    ) -> None:
        """Children with precedence constraints maintain sequential ordering."""
        task_routing: dict[str, TargetArtifact] = {}
        for task in parent_child_package.control_flow_tasks:
            task_routing[task.name] = TargetArtifact.DATA_FACTORY_PIPELINE
            for child in task.child_tasks:
                task_routing[child.name] = TargetArtifact.DATA_FACTORY_PIPELINE

        output = generator.generate_package_pipeline(parent_child_package, task_routing, tmp_path)

        with open(output) as f:
            pipeline = json.load(f)

        activities = pipeline["properties"]["activities"]
        load_customer = next(a for a in activities if a["name"] == "Load_DimCustomer")

        # Should depend on Load_DimDate (via precedence constraint)
        deps = {d["activity"] for d in load_customer.get("dependsOn", [])}
        assert "Load_DimDate" in deps

    @pytest.mark.unit
    def test_next_phase_depends_on_all_parallel_children(
        self,
        generator: DataFactoryGenerator,
        parent_child_package: SSISPackage,
        tmp_path: Path,
    ) -> None:
        """Tasks after a parallel container depend on ALL children."""
        task_routing: dict[str, TargetArtifact] = {}
        for task in parent_child_package.control_flow_tasks:
            task_routing[task.name] = TargetArtifact.DATA_FACTORY_PIPELINE
            for child in task.child_tasks:
                task_routing[child.name] = TargetArtifact.DATA_FACTORY_PIPELINE

        output = generator.generate_package_pipeline(parent_child_package, task_routing, tmp_path)

        with open(output) as f:
            pipeline = json.load(f)

        activities = pipeline["properties"]["activities"]
        # Load_DimDate is the first activity in "Dimension Loads" container
        # which comes after "Parallel Extracts" container
        load_dim_date = next(a for a in activities if a["name"] == "Load_DimDate")
        deps = {d["activity"] for d in load_dim_date.get("dependsOn", [])}
        # Should depend on BOTH parallel children
        assert "Extract_Customers" in deps
        assert "Extract_Orders" in deps


# =========================================================================
# Notebook Parameter Passing
# =========================================================================


class TestNotebookParameterPassing:
    """Tests for parameter passing from pipeline to TridentNotebook activities."""

    @pytest.mark.unit
    def test_notebook_activity_has_parameters(
        self,
        generator: DataFactoryGenerator,
        parent_child_package: SSISPackage,
        tmp_path: Path,
    ) -> None:
        """TridentNotebook activities receive pipeline parameters."""
        task_routing: dict[str, TargetArtifact] = {}
        for task in parent_child_package.control_flow_tasks:
            task_routing[task.name] = TargetArtifact.SPARK_NOTEBOOK
            for child in task.child_tasks:
                task_routing[child.name] = TargetArtifact.SPARK_NOTEBOOK

        output = generator.generate_package_pipeline(parent_child_package, task_routing, tmp_path)

        with open(output) as f:
            pipeline = json.load(f)

        notebooks = [a for a in pipeline["properties"]["activities"] if a.get("type") == "TridentNotebook"]
        if notebooks:
            nb = notebooks[0]
            params = nb["typeProperties"].get("parameters", {})
            assert len(params) > 0, "Notebook should receive pipeline parameters"

    @pytest.mark.unit
    def test_notebook_params_use_expressions(
        self,
        generator: DataFactoryGenerator,
        parent_child_package: SSISPackage,
        tmp_path: Path,
    ) -> None:
        """Forwarded notebook parameters use @pipeline expression syntax."""
        task_routing: dict[str, TargetArtifact] = {}
        for task in parent_child_package.control_flow_tasks:
            task_routing[task.name] = TargetArtifact.SPARK_NOTEBOOK
            for child in task.child_tasks:
                task_routing[child.name] = TargetArtifact.SPARK_NOTEBOOK

        output = generator.generate_package_pipeline(parent_child_package, task_routing, tmp_path)

        with open(output) as f:
            pipeline = json.load(f)

        notebooks = [a for a in pipeline["properties"]["activities"] if a.get("type") == "TridentNotebook"]
        if notebooks:
            params = notebooks[0]["typeProperties"].get("parameters", {})
            for _name, val in params.items():
                assert val["type"] == "Expression"
                assert "@pipeline().parameters." in val["value"] or "@variables(" in val["value"]
