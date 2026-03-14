"""Tests for Phase 22 — Orchestration & Scheduling."""

from __future__ import annotations

from typing import TYPE_CHECKING

from click.testing import CliRunner

from ssis_to_fabric.cli import main
from ssis_to_fabric.engine.orchestration import (
    ExtractedSchedule,
    FabricTrigger,
    FrequencyType,
    PipelineDependency,
    SLADefinition,
    build_dependency_graph,
    generate_airflow_dag,
    generate_logic_app,
    parse_agent_schedule,
    schedule_to_trigger,
    topological_order,
    write_schedule_report,
)

if TYPE_CHECKING:
    from pathlib import Path


class TestScheduleExtraction:
    def test_cron_daily(self):
        s = ExtractedSchedule(job_name="j", frequency=FrequencyType.DAILY, start_time="06:00")
        assert s.cron_expression == "00 06 */1 * *"

    def test_cron_weekly(self):
        s = ExtractedSchedule(job_name="j", frequency=FrequencyType.WEEKLY, days_of_week=["1", "3", "5"])
        assert "1,3,5" in s.cron_expression

    def test_cron_hourly(self):
        s = ExtractedSchedule(job_name="j", frequency=FrequencyType.HOURLY, interval=2, start_time="00:30")
        assert "*/2" in s.cron_expression

    def test_cron_minute(self):
        s = ExtractedSchedule(job_name="j", frequency=FrequencyType.MINUTE, interval=15)
        assert "*/15" in s.cron_expression

    def test_parse_agent_schedule(self):
        xml = "job_name = 'ETL_Nightly'\nfreq_type = 4\nactive_start_time = 060000"
        schedules = parse_agent_schedule(xml)
        assert len(schedules) == 1
        assert schedules[0].job_name == "ETL_Nightly"
        assert schedules[0].frequency == FrequencyType.DAILY

    def test_to_dict(self):
        s = ExtractedSchedule(job_name="j", frequency=FrequencyType.DAILY)
        d = s.to_dict()
        assert d["job_name"] == "j"
        assert d["frequency"] == "daily"


class TestFabricTrigger:
    def test_schedule_to_trigger(self):
        s = ExtractedSchedule(job_name="j", package_name="pkg.dtsx", frequency=FrequencyType.DAILY)
        t = schedule_to_trigger(s)
        assert t.pipeline_name == "pkg"
        assert t.enabled

    def test_trigger_to_dict(self):
        t = FabricTrigger(name="t1", pipeline_name="p1", cron_expression="0 6 * * *")
        d = t.to_dict()
        assert d["type"] == "ScheduleTrigger"
        assert d["properties"]["pipelines"][0]["pipelineReference"]["referenceName"] == "p1"


class TestDependencyGraph:
    def test_build_graph(self):
        deps = [
            PipelineDependency(upstream="A", downstream="B"),
            PipelineDependency(upstream="A", downstream="C"),
        ]
        graph = build_dependency_graph(deps)
        assert "B" in graph["A"]
        assert "C" in graph["A"]

    def test_topological_order(self):
        graph = {"A": ["B", "C"], "B": ["D"], "C": ["D"], "D": []}
        order = topological_order(graph)
        assert order.index("A") < order.index("B")
        assert order.index("A") < order.index("C")
        assert order.index("B") < order.index("D")

    def test_sla_definition(self):
        sla = SLADefinition(pipeline_name="p1", max_duration_minutes=30, priority="high")
        d = sla.to_dict()
        assert d["max_duration_minutes"] == 30


class TestExternalAdapters:
    def test_airflow_dag(self):
        schedules = [ExtractedSchedule(job_name="etl", package_name="load.dtsx")]
        dag = generate_airflow_dag(schedules)
        assert "from airflow import DAG" in dag
        assert "BashOperator" in dag

    def test_logic_app(self):
        s = ExtractedSchedule(job_name="j", frequency=FrequencyType.DAILY, package_name="p.dtsx")
        la = generate_logic_app(s)
        assert "triggers" in la["definition"]
        assert "Recurrence" in la["definition"]["triggers"]


class TestScheduleReport:
    def test_write_report(self, tmp_path: Path):
        s = ExtractedSchedule(job_name="j")
        t = FabricTrigger(name="t", pipeline_name="p")
        path = write_schedule_report([s], [t], tmp_path / "report.json")
        assert path.exists()


class TestCLI:
    def test_schedule_export_registered(self):
        runner = CliRunner()
        result = runner.invoke(main, ["schedule-export", "--help"])
        assert result.exit_code == 0
