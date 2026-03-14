"""Tests for the 6 features implemented in batch 2.

A. DeployAgent
B. C# transpiler expansion (new line rules + classify_risk)
C. Parallel deployment (deploy_all_parallel, _deploy_batch_parallel)
D. Post-deploy verification (post_deploy_check)
E. LakehouseProvisioner CLI + API integration
F. Rollback strategy (DeploymentReport.rollback, --on-error flag)
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ssis_to_fabric.config import MigrationConfig, MigrationStrategy
from ssis_to_fabric.engine.agents import AgentResult, AgentRole, DeployAgent
from ssis_to_fabric.engine.csharp_transpiler import CSharpTranspiler
from ssis_to_fabric.engine.fabric_deployer import (
    DeploymentReport,
    DeploymentResult,
)


# =========================================================================
# Fixtures
# =========================================================================


@pytest.fixture()
def config(tmp_path: Path) -> MigrationConfig:
    return MigrationConfig(output_dir=tmp_path / "output", strategy=MigrationStrategy.HYBRID)


@pytest.fixture()
def transpiler() -> CSharpTranspiler:
    return CSharpTranspiler()


@pytest.fixture()
def mock_deployer() -> MagicMock:
    deployer = MagicMock()
    deployer.workspace_id = "test-ws-id"
    return deployer


# =========================================================================
# A. DeployAgent
# =========================================================================


class TestDeployAgent:
    @pytest.mark.unit
    def test_role(self, config: MigrationConfig) -> None:
        agent = DeployAgent(config)
        assert agent.role == AgentRole.DEPLOY

    @pytest.mark.unit
    def test_process_returns_error(self, config: MigrationConfig) -> None:
        """process() should instruct callers to use deploy_item instead."""
        agent = DeployAgent(config)
        pkg = MagicMock()
        pkg.name = "test_pkg"
        result = agent.process(pkg, None, config.output_dir)
        assert result.status == "error"
        assert "deploy_item" in result.error

    @pytest.mark.unit
    def test_deploy_item_no_deployer(self, config: MigrationConfig, tmp_path: Path) -> None:
        agent = DeployAgent(config)
        result = agent.deploy_item(tmp_path / "test.json", "DataPipeline")
        assert result.status == "error"
        assert "set_deployer" in result.error

    @pytest.mark.unit
    def test_deploy_item_success(self, config: MigrationConfig, tmp_path: Path) -> None:
        agent = DeployAgent(config)
        mock = MagicMock()
        deploy_result = DeploymentResult(
            name="my_pipeline",
            item_type="DataPipeline",
            status="success",
            item_id="id-123",
        )
        mock.deploy_pipeline.return_value = deploy_result
        agent.set_deployer(mock)

        artifact = tmp_path / "my_pipeline.json"
        artifact.write_text("{}")
        result = agent.deploy_item(artifact, "DataPipeline")

        assert result.status == "completed"
        assert result.output_path == "id-123"
        mock.deploy_pipeline.assert_called_once_with(artifact)

    @pytest.mark.unit
    def test_deploy_item_notebook(self, config: MigrationConfig, tmp_path: Path) -> None:
        agent = DeployAgent(config)
        mock = MagicMock()
        deploy_result = DeploymentResult(
            name="nb", item_type="Notebook", status="success", item_id="nb-1",
        )
        mock.deploy_notebook.return_value = deploy_result
        agent.set_deployer(mock)

        result = agent.deploy_item(tmp_path / "nb.py", "Notebook")
        assert result.status == "completed"
        mock.deploy_notebook.assert_called_once()

    @pytest.mark.unit
    def test_deploy_item_dataflow(self, config: MigrationConfig, tmp_path: Path) -> None:
        agent = DeployAgent(config)
        mock = MagicMock()
        deploy_result = DeploymentResult(
            name="df", item_type="Dataflow", status="success", item_id="df-1",
        )
        mock.deploy_dataflow.return_value = deploy_result
        agent.set_deployer(mock)

        result = agent.deploy_item(tmp_path / "df.json", "Dataflow")
        assert result.status == "completed"
        mock.deploy_dataflow.assert_called_once()

    @pytest.mark.unit
    def test_deploy_item_unknown_type(self, config: MigrationConfig, tmp_path: Path) -> None:
        agent = DeployAgent(config)
        agent.set_deployer(MagicMock())
        result = agent.deploy_item(tmp_path / "x.json", "UnknownType")
        assert result.status == "error"
        assert "Unknown artifact type" in result.error

    @pytest.mark.unit
    def test_deploy_item_exception(self, config: MigrationConfig, tmp_path: Path) -> None:
        agent = DeployAgent(config)
        mock = MagicMock()
        mock.deploy_pipeline.side_effect = RuntimeError("API down")
        agent.set_deployer(mock)

        result = agent.deploy_item(tmp_path / "p.json", "DataPipeline")
        assert result.status == "error"
        assert "API down" in result.error

    @pytest.mark.unit
    def test_deploy_item_error_status(self, config: MigrationConfig, tmp_path: Path) -> None:
        """When the deployer returns an error status, the agent should propagate it."""
        agent = DeployAgent(config)
        mock = MagicMock()
        deploy_result = DeploymentResult(
            name="p", item_type="DataPipeline", status="error", error="quota",
        )
        mock.deploy_pipeline.return_value = deploy_result
        agent.set_deployer(mock)

        result = agent.deploy_item(tmp_path / "p.json", "DataPipeline")
        assert result.status == "error"

    @pytest.mark.unit
    def test_repr(self, config: MigrationConfig) -> None:
        agent = DeployAgent(config)
        assert "DeployAgent" in repr(agent)

    @pytest.mark.unit
    def test_import_from_init(self) -> None:
        from ssis_to_fabric import DeployAgent as DA
        assert DA is DeployAgent


# =========================================================================
# B. C# Transpiler Expansion — new line rules
# =========================================================================


class TestCSharpTranspilerNewRules:
    """Tests for the ~60 new line rules added to the transpiler."""

    # --- Control flow ---
    @pytest.mark.unit
    def test_while_loop(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile("while (x < 10) { x++; }")
        assert "while" in result

    @pytest.mark.unit
    def test_if_statement(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile("if (x > 0) { doSomething(); }")
        assert "if" in result
        assert ":" in result

    @pytest.mark.unit
    def test_elif_statement(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile("else if (x < 0) { other(); }")
        assert "elif" in result

    @pytest.mark.unit
    def test_else_statement(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile("else { fallback(); }")
        assert "else:" in result

    @pytest.mark.unit
    def test_return_statement(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile("return result;")
        assert "return " in result

    # --- Collections ---
    @pytest.mark.unit
    def test_new_list(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile("var items = new List<string>();")
        assert "[]" in result

    @pytest.mark.unit
    def test_new_dictionary(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile("var dict = new Dictionary<string, int>();")
        assert "{}" in result

    @pytest.mark.unit
    def test_add(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile('items.Add("hello");')
        assert ".append(" in result

    @pytest.mark.unit
    def test_remove(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile('items.Remove("hello");')
        assert ".remove(" in result

    @pytest.mark.unit
    def test_count(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile("var n = items.Count;")
        assert "__len__" in result

    @pytest.mark.unit
    def test_clear(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile("items.Clear();")
        assert ".clear()" in result

    # --- Regex ---
    @pytest.mark.unit
    def test_regex_ismatch(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile('Regex.IsMatch(input, "\\\\d+");')
        assert "re.search" in result

    @pytest.mark.unit
    def test_regex_match(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile('Regex.Match(input, "\\\\w+");')
        assert "re.search" in result

    @pytest.mark.unit
    def test_regex_replace(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile('Regex.Replace(input, "\\\\s+", " ");')
        assert "re.sub" in result

    @pytest.mark.unit
    def test_regex_split(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile('Regex.Split(input, ",");')
        assert "re.split" in result

    @pytest.mark.unit
    def test_new_regex(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile('var rx = new Regex("pattern");')
        assert "re.compile" in result

    # --- StringBuilder ---
    @pytest.mark.unit
    def test_stringbuilder_new(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile("var sb = new StringBuilder();")
        assert '""' in result

    @pytest.mark.unit
    def test_stringbuilder_append(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile('sb.Append("hello");')
        assert "+=" in result

    @pytest.mark.unit
    def test_stringbuilder_appendline(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile('sb.AppendLine("hello");')
        assert "+=" in result
        assert "\\n" in result

    # --- DataTable ---
    @pytest.mark.unit
    def test_datatable_new(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile("var dt = new DataTable();")
        assert "list of dicts" in result

    # --- Extended File I/O ---
    @pytest.mark.unit
    def test_file_readalllines(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile('File.ReadAllLines("data.txt");')
        assert "read_text().splitlines()" in result

    @pytest.mark.unit
    def test_file_copy(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile('File.Copy("a.txt", "b.txt");')
        assert "shutil.copy" in result

    @pytest.mark.unit
    def test_file_move(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile('File.Move("a.txt", "b.txt");')
        assert "shutil.move" in result

    @pytest.mark.unit
    def test_file_appendalltext(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile('File.AppendAllText("log.txt", "data");')
        assert "open(" in result
        assert "'a'" in result

    # --- Directory ---
    @pytest.mark.unit
    def test_directory_create(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile('Directory.CreateDirectory("output");')
        assert "os.makedirs" in result

    @pytest.mark.unit
    def test_directory_exists(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile('Directory.Exists("output");')
        assert "os.path.isdir" in result

    @pytest.mark.unit
    def test_directory_getfiles(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile('Directory.GetFiles("output");')
        assert "os.listdir" in result

    # --- Path ---
    @pytest.mark.unit
    def test_path_combine(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile('Path.Combine("a", "b");')
        assert "os.path.join" in result

    @pytest.mark.unit
    def test_path_getfilename(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile('Path.GetFileName("a/b.txt");')
        assert "os.path.basename" in result

    @pytest.mark.unit
    def test_path_getextension(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile('Path.GetExtension("file.txt");')
        assert "os.path.splitext" in result

    # --- Stream patterns ---
    @pytest.mark.unit
    def test_streamwriter(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile('var sw = new StreamWriter("out.txt");')
        assert "open(" in result
        assert "'w'" in result

    @pytest.mark.unit
    def test_streamreader(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile('var sr = new StreamReader("in.txt");')
        assert "open(" in result
        assert "'r'" in result

    @pytest.mark.unit
    def test_readline(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile("var line = sr.ReadLine();")
        assert ".readline()" in result

    @pytest.mark.unit
    def test_readtoend(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile("var text = sr.ReadToEnd();")
        assert ".read()" in result

    @pytest.mark.unit
    def test_close(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile("sr.Close();")
        assert ".close()" in result

    @pytest.mark.unit
    def test_dispose(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile("sr.Dispose();")
        assert ".close()" in result

    # --- Type conversions ---
    @pytest.mark.unit
    def test_int_parse(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile('int.Parse("42");')
        assert 'int("42")' in result

    @pytest.mark.unit
    def test_convert_toint32(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile("Convert.ToInt32(val);")
        assert "int(val)" in result

    @pytest.mark.unit
    def test_convert_todouble(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile("Convert.ToDouble(val);")
        assert "float(val)" in result

    @pytest.mark.unit
    def test_convert_tostring(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile("Convert.ToString(val);")
        assert "str(val)" in result

    @pytest.mark.unit
    def test_convert_tobool(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile("Convert.ToBoolean(val);")
        assert "bool(val)" in result

    @pytest.mark.unit
    def test_convert_todatetime(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile("Convert.ToDateTime(val);")
        assert "datetime.fromisoformat" in result

    # --- String helpers ---
    @pytest.mark.unit
    def test_string_format(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile('string.Format("{0} {1}", a, b);')
        assert ".format(" in result

    @pytest.mark.unit
    def test_string_join(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile('string.Join(",", items);')
        assert ".join(" in result

    @pytest.mark.unit
    def test_string_concat(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile('string.Concat(a, b, c);')
        assert '".join(' in result

    @pytest.mark.unit
    def test_string_empty(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile("var x = string.Empty;")
        assert '""' in result

    # --- Exception handling ---
    @pytest.mark.unit
    def test_try_catch(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile("try { doWork(); }")
        assert "try:" in result

    @pytest.mark.unit
    def test_catch_specific(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile("catch (IOException ex) {")
        assert "except" in result
        assert "ex" in result

    @pytest.mark.unit
    def test_catch_generic(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile("catch {")
        assert "except Exception" in result

    @pytest.mark.unit
    def test_finally(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile("finally { cleanup(); }")
        assert "finally:" in result

    @pytest.mark.unit
    def test_throw_new(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile('throw new InvalidOperationException("oops");')
        assert "raise Exception" in result

    @pytest.mark.unit
    def test_throw_bare(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile("throw;")
        assert "raise" in result

    # --- Unsupported patterns ---
    @pytest.mark.unit
    def test_unsupported_wmi(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile("var obj = new ManagementObject();")
        assert "TODO" in result

    @pytest.mark.unit
    def test_unsupported_registry(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile("RegistryKey key = Registry.LocalMachine;")
        assert "TODO" in result

    @pytest.mark.unit
    def test_unsupported_process(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile('Process.Start("cmd");')
        assert "TODO" in result

    @pytest.mark.unit
    def test_unsupported_smtp(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile("var smtp = new SmtpClient();")
        assert "TODO" in result

    @pytest.mark.unit
    def test_unsupported_oledb(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile("var conn = new OleDbConnection();")
        assert "TODO" in result

    @pytest.mark.unit
    def test_unsupported_unsafe(self, transpiler: CSharpTranspiler) -> None:
        result = transpiler.transpile("unsafe { int* p = &x; }")
        assert "TODO" in result


# =========================================================================
# B2. classify_risk
# =========================================================================


class TestClassifyRisk:
    @pytest.mark.unit
    def test_empty_is_low(self, transpiler: CSharpTranspiler) -> None:
        assert transpiler.classify_risk("") == "low"

    @pytest.mark.unit
    def test_whitespace_only_is_low(self, transpiler: CSharpTranspiler) -> None:
        assert transpiler.classify_risk("   ") == "low"

    @pytest.mark.unit
    def test_simple_string_ops_are_low(self, transpiler: CSharpTranspiler) -> None:
        code = """
        var x = name.ToUpper();
        var y = name.Trim();
        for (int i = 0; i < 10; i++) { }
        """
        assert transpiler.classify_risk(code) == "low"

    @pytest.mark.unit
    def test_database_code_is_high(self, transpiler: CSharpTranspiler) -> None:
        code = "var conn = new SqlConnection(cs);"
        assert transpiler.classify_risk(code) == "high"

    @pytest.mark.unit
    def test_http_client_is_high(self, transpiler: CSharpTranspiler) -> None:
        code = "var client = new HttpClient();"
        assert transpiler.classify_risk(code) == "high"

    @pytest.mark.unit
    def test_task_run_is_high(self, transpiler: CSharpTranspiler) -> None:
        code = "Task.Run(() => doWork());"
        assert transpiler.classify_risk(code) == "high"

    @pytest.mark.unit
    def test_unsafe_is_high(self, transpiler: CSharpTranspiler) -> None:
        code = "unsafe { int* p = &x; }"
        assert transpiler.classify_risk(code) == "high"

    @pytest.mark.unit
    def test_unknown_patterns_are_medium(self, transpiler: CSharpTranspiler) -> None:
        """Code with no recognizable patterns -> medium."""
        code = "SomeCustomClass.DoWork();"
        assert transpiler.classify_risk(code) == "medium"

    @pytest.mark.unit
    def test_single_low_pattern_is_medium(self, transpiler: CSharpTranspiler) -> None:
        """A single low-risk pattern hit requires 2+ for 'low', so 1 hit -> medium."""
        code = "var upper = x.ToUpper();"
        assert transpiler.classify_risk(code) == "medium"

    @pytest.mark.unit
    def test_file_io_with_loops_is_low(self, transpiler: CSharpTranspiler) -> None:
        code = """
        var text = File.ReadAllText("data.txt");
        foreach (var line in text) { Console.WriteLine(line); }
        """
        assert transpiler.classify_risk(code) == "low"


# =========================================================================
# C. Parallel deployment (_deploy_batch_parallel, deploy_all_parallel)
# =========================================================================


class TestDeployBatchParallel:
    @pytest.mark.unit
    def test_batch_parallel_calls_deploy_fn(self, tmp_path: Path) -> None:
        """_deploy_batch_parallel should call the named method for each item."""
        deployer = MagicMock()
        deployer.workspace_id = "ws-1"
        deployer._move_result_to_folder = MagicMock()

        result1 = DeploymentResult(name="a", item_type="Dataflow", status="success")
        result2 = DeploymentResult(name="b", item_type="Dataflow", status="success")
        deployer.deploy_dataflow = MagicMock(side_effect=[result1, result2])

        from ssis_to_fabric.engine.fabric_deployer import FabricDeployer

        method = FabricDeployer._deploy_batch_parallel
        items = [("a", tmp_path / "a.json"), ("b", tmp_path / "b.json")]

        results = method(deployer, items, "deploy_dataflow", max_workers=2)
        assert len(results) == 2
        assert deployer.deploy_dataflow.call_count == 2


class TestDeployAllParallel:
    @pytest.mark.unit
    def test_empty_directory(self, tmp_path: Path) -> None:
        """deploy_all_parallel on empty dir should return empty report."""
        deployer = MagicMock()
        deployer.workspace_id = "ws-1"
        deployer.dry_run = False
        deployer._resolve_connections = MagicMock()
        deployer._precreate_folders = MagicMock()
        deployer._move_remaining_to_folders = MagicMock()

        from ssis_to_fabric.engine.fabric_deployer import FabricDeployer

        method = FabricDeployer.deploy_all_parallel
        report = method(deployer, tmp_path, max_workers=2)
        assert report.total == 0
        assert report.succeeded == 0

    @pytest.mark.unit
    def test_parallel_deploys_dataflows(self, tmp_path: Path) -> None:
        """Dataflow files should be deployed via _deploy_batch_parallel."""
        df_dir = tmp_path / "dataflows"
        df_dir.mkdir()
        (df_dir / "df1.json").write_text("{}")
        (df_dir / "df2.json").write_text("{}")
        # destinations file should be excluded
        (df_dir / "df1.destinations.json").write_text("{}")

        deployer = MagicMock()
        deployer.workspace_id = "ws-1"
        deployer.dry_run = False
        deployer._resolve_connections = MagicMock()
        deployer._precreate_folders = MagicMock()
        deployer._move_remaining_to_folders = MagicMock()

        batch_results = [
            DeploymentResult(name="df1", item_type="Dataflow", status="success"),
            DeploymentResult(name="df2", item_type="Dataflow", status="success"),
        ]
        deployer._deploy_batch_parallel = MagicMock(return_value=batch_results)

        from ssis_to_fabric.engine.fabric_deployer import FabricDeployer

        report = FabricDeployer.deploy_all_parallel(
            deployer, tmp_path, max_workers=2,
        )
        assert report.succeeded == 2
        deployer._deploy_batch_parallel.assert_called()

    @pytest.mark.unit
    def test_rollback_on_error(self, tmp_path: Path) -> None:
        """on_error='rollback' should call rollback when failures occur."""
        df_dir = tmp_path / "dataflows"
        df_dir.mkdir()
        (df_dir / "fail.json").write_text("{}")

        deployer = MagicMock()
        deployer.workspace_id = "ws-1"
        deployer.dry_run = False
        deployer._resolve_connections = MagicMock()
        deployer._precreate_folders = MagicMock()

        error_results = [
            DeploymentResult(name="fail", item_type="Dataflow", status="error", error="oops"),
        ]
        deployer._deploy_batch_parallel = MagicMock(return_value=error_results)
        deployer.delete_item = MagicMock(return_value=True)

        from ssis_to_fabric.engine.fabric_deployer import FabricDeployer

        report = FabricDeployer.deploy_all_parallel(
            deployer, tmp_path, max_workers=1, on_error="rollback",
        )
        assert report.failed > 0


# =========================================================================
# D. Post-deploy verification (post_deploy_check)
# =========================================================================


class TestPostDeployCheck:
    @pytest.mark.unit
    def test_dry_run_returns_skipped(self) -> None:
        deployer = MagicMock()
        deployer.dry_run = True

        report = DeploymentReport(workspace_id="ws-1")
        report.results = [
            DeploymentResult(name="p1", item_type="DataPipeline", status="success"),
        ]

        from ssis_to_fabric.engine.fabric_deployer import FabricDeployer

        checks = FabricDeployer.post_deploy_check(deployer, report)
        assert len(checks) == 1
        assert checks[0]["status"] == "skipped"

    @pytest.mark.unit
    def test_all_found_on_first_poll(self) -> None:
        deployer = MagicMock()
        deployer.dry_run = False
        deployer._load_existing_items.return_value = {
            "DataPipeline::p1": "id-1",
            "Notebook::nb1": "id-2",
        }

        report = DeploymentReport(workspace_id="ws-1")
        report.results = [
            DeploymentResult(name="p1", item_type="DataPipeline", status="success"),
            DeploymentResult(name="nb1", item_type="Notebook", status="success"),
        ]

        from ssis_to_fabric.engine.fabric_deployer import FabricDeployer

        checks = FabricDeployer.post_deploy_check(
            deployer, report, max_polls=1, poll_interval=0.01,
        )
        assert len(checks) == 2
        assert all(c["status"] == "ok" for c in checks)

    @pytest.mark.unit
    def test_missing_item_reported(self) -> None:
        deployer = MagicMock()
        deployer.dry_run = False
        deployer._load_existing_items.return_value = {
            "DataPipeline::p1": "id-1",
        }

        report = DeploymentReport(workspace_id="ws-1")
        report.results = [
            DeploymentResult(name="p1", item_type="DataPipeline", status="success"),
            DeploymentResult(name="missing", item_type="Notebook", status="success"),
        ]

        from ssis_to_fabric.engine.fabric_deployer import FabricDeployer

        checks = FabricDeployer.post_deploy_check(
            deployer, report, max_polls=1, poll_interval=0.01,
        )
        statuses = {c["name"]: c["status"] for c in checks}
        assert statuses["p1"] == "ok"
        assert statuses["missing"] == "missing"

    @pytest.mark.unit
    def test_empty_report_returns_empty(self) -> None:
        deployer = MagicMock()
        deployer.dry_run = False

        report = DeploymentReport(workspace_id="ws-1")

        from ssis_to_fabric.engine.fabric_deployer import FabricDeployer

        checks = FabricDeployer.post_deploy_check(deployer, report)
        assert checks == []

    @pytest.mark.unit
    def test_error_items_excluded(self) -> None:
        """post_deploy_check only checks items with status='success'."""
        deployer = MagicMock()
        deployer.dry_run = False
        deployer._load_existing_items.return_value = {}

        report = DeploymentReport(workspace_id="ws-1")
        report.results = [
            DeploymentResult(name="err", item_type="DataPipeline", status="error", error="x"),
            DeploymentResult(name="skip", item_type="Notebook", status="skipped"),
        ]

        from ssis_to_fabric.engine.fabric_deployer import FabricDeployer

        checks = FabricDeployer.post_deploy_check(
            deployer, report, max_polls=1, poll_interval=0.01,
        )
        assert checks == []


# =========================================================================
# E. LakehouseProvisioner CLI + API integration
# =========================================================================


class TestProvisionSchemaCLI:
    @pytest.mark.unit
    def test_provision_command_no_manifests(self, tmp_path: Path) -> None:
        """provision command with no manifests should print warning."""
        from click.testing import CliRunner

        from ssis_to_fabric.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["provision", str(tmp_path)])
        assert result.exit_code == 0
        assert "No destination manifests found" in result.output

    @pytest.mark.unit
    def test_provision_command_with_manifest(self, tmp_path: Path) -> None:
        """provision command should generate DDL for destination manifests."""
        # Create a sidecar destinations file
        manifest = {
            "tables": [
                {
                    "schema": "dbo",
                    "name": "FactSales",
                    "columns": [
                        {"name": "id", "type": "int"},
                        {"name": "amount", "type": "decimal(18,2)"},
                    ],
                }
            ]
        }
        (tmp_path / "etl.destinations.json").write_text(json.dumps(manifest))

        from click.testing import CliRunner

        from ssis_to_fabric.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["provision", str(tmp_path)])
        assert result.exit_code == 0
        assert "DDL" in result.output


class TestProvisionSchemaAPI:
    @pytest.mark.unit
    def test_provision_schema_default(self, tmp_path: Path) -> None:
        from ssis_to_fabric.api import SSISMigrator

        migrator = SSISMigrator(output_dir=tmp_path)
        # No manifests → empty result
        result = migrator.provision_schema()
        assert result == []

    @pytest.mark.unit
    def test_provision_schema_custom_dir(self, tmp_path: Path) -> None:
        from ssis_to_fabric.api import SSISMigrator

        migrator = SSISMigrator(output_dir=tmp_path / "default")
        custom = tmp_path / "custom"
        custom.mkdir()
        result = migrator.provision_schema(output_dir=custom)
        assert result == []

    @pytest.mark.unit
    def test_provision_schema_with_manifest(self, tmp_path: Path) -> None:
        from ssis_to_fabric.api import SSISMigrator

        manifest = {
            "tables": [
                {
                    "schema": "dbo",
                    "name": "DimDate",
                    "columns": [
                        {"name": "DateKey", "type": "int"},
                    ],
                }
            ]
        }
        (tmp_path / "pkg.destinations.json").write_text(json.dumps(manifest))

        migrator = SSISMigrator(output_dir=tmp_path)
        result = migrator.provision_schema()
        assert len(result) >= 1
        # Provisioner names output files from the manifest filename stem
        assert any(p.suffix == ".sql" for p in result)


# =========================================================================
# F. Rollback strategy
# =========================================================================


class TestDeploymentReportRollback:
    @pytest.mark.unit
    def test_created_item_ids(self) -> None:
        report = DeploymentReport(workspace_id="ws")
        report.results = [
            DeploymentResult(name="a", item_type="DataPipeline", status="success", item_id="id-a"),
            DeploymentResult(name="b", item_type="Notebook", status="error", error="fail"),
            DeploymentResult(name="c", item_type="Dataflow", status="skipped"),
            DeploymentResult(name="d", item_type="Notebook", status="success", item_id="id-d"),
        ]
        ids = report.created_item_ids
        assert len(ids) == 2
        assert ("id-a", "DataPipeline", "a") in ids
        assert ("id-d", "Notebook", "d") in ids

    @pytest.mark.unit
    def test_created_item_ids_empty(self) -> None:
        report = DeploymentReport(workspace_id="ws")
        assert report.created_item_ids == []

    @pytest.mark.unit
    def test_rollback_deletes_created_items(self) -> None:
        report = DeploymentReport(workspace_id="ws")
        report.results = [
            DeploymentResult(name="a", item_type="DataPipeline", status="success", item_id="id-a"),
            DeploymentResult(name="b", item_type="Notebook", status="success", item_id="id-b"),
        ]
        deployer = MagicMock()
        deployer.delete_item = MagicMock(return_value=True)

        deleted, errors = report.rollback(deployer)
        assert deleted == 2
        assert errors == 0
        assert deployer.delete_item.call_count == 2
        deployer.delete_item.assert_any_call("id-a", "DataPipeline", "a")
        deployer.delete_item.assert_any_call("id-b", "Notebook", "b")

    @pytest.mark.unit
    def test_rollback_with_delete_errors(self) -> None:
        report = DeploymentReport(workspace_id="ws")
        report.results = [
            DeploymentResult(name="a", item_type="DataPipeline", status="success", item_id="id-a"),
            DeploymentResult(name="b", item_type="Notebook", status="success", item_id="id-b"),
        ]
        deployer = MagicMock()
        deployer.delete_item = MagicMock(side_effect=[True, False])

        deleted, errors = report.rollback(deployer)
        assert deleted == 1
        assert errors == 1

    @pytest.mark.unit
    def test_rollback_no_items(self) -> None:
        report = DeploymentReport(workspace_id="ws")
        deployer = MagicMock()
        deleted, errors = report.rollback(deployer)
        assert (deleted, errors) == (0, 0)
        deployer.delete_item.assert_not_called()

    @pytest.mark.unit
    def test_rollback_skips_non_success(self) -> None:
        report = DeploymentReport(workspace_id="ws")
        report.results = [
            DeploymentResult(name="a", item_type="DataPipeline", status="error"),
            DeploymentResult(name="b", item_type="Notebook", status="skipped"),
        ]
        deployer = MagicMock()
        deleted, errors = report.rollback(deployer)
        assert (deleted, errors) == (0, 0)


# =========================================================================
# F2. CLI --on-error and --verify flags (integration-level)
# =========================================================================


class TestCLIDeployFlags:
    @pytest.mark.unit
    def test_deploy_help_contains_flags(self) -> None:
        from click.testing import CliRunner

        from ssis_to_fabric.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["deploy", "--help"])
        assert "--deploy-workers" in result.output
        assert "--on-error" in result.output
        assert "--verify" in result.output
        assert "--no-verify" in result.output
