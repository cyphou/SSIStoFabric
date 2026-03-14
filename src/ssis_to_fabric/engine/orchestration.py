"""
Orchestration & Scheduling
===========================
Phase 22: SQL Agent schedule extraction, Fabric trigger generation,
cross-pipeline dependencies, and external scheduler adapters.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any

from ssis_to_fabric.logging_config import get_logger

if TYPE_CHECKING:
    from pathlib import Path

logger = get_logger(__name__)


# =====================================================================
# Schedule Extraction
# =====================================================================


class FrequencyType(str, Enum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    HOURLY = "hourly"
    MINUTE = "minute"
    ON_DEMAND = "on_demand"
    CONTINUOUS = "continuous"


@dataclass
class ExtractedSchedule:
    """A schedule extracted from SQL Server Agent or SSIS."""

    job_name: str
    step_name: str = ""
    package_name: str = ""
    frequency: FrequencyType = FrequencyType.DAILY
    interval: int = 1
    start_time: str = "00:00"  # HH:MM
    days_of_week: list[str] = field(default_factory=list)
    day_of_month: int = 0
    enabled: bool = True
    cron_expression: str = ""

    def __post_init__(self) -> None:
        if not self.cron_expression:
            self.cron_expression = self._to_cron()

    def _to_cron(self) -> str:
        """Convert to cron expression."""
        parts = self.start_time.split(":")
        hour = parts[0] if len(parts) > 0 else "0"
        minute = parts[1] if len(parts) > 1 else "0"

        if self.frequency == FrequencyType.MINUTE:
            return f"*/{self.interval} * * * *"
        elif self.frequency == FrequencyType.HOURLY:
            return f"{minute} */{self.interval} * * *"
        elif self.frequency == FrequencyType.DAILY:
            return f"{minute} {hour} */{self.interval} * *"
        elif self.frequency == FrequencyType.WEEKLY:
            dow = ",".join(self.days_of_week) if self.days_of_week else "1"
            return f"{minute} {hour} * * {dow}"
        elif self.frequency == FrequencyType.MONTHLY:
            dom = str(self.day_of_month) if self.day_of_month else "1"
            return f"{minute} {hour} {dom} * *"
        return f"{minute} {hour} * * *"

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_name": self.job_name,
            "step_name": self.step_name,
            "package_name": self.package_name,
            "frequency": self.frequency.value,
            "interval": self.interval,
            "start_time": self.start_time,
            "days_of_week": self.days_of_week,
            "cron_expression": self.cron_expression,
            "enabled": self.enabled,
        }


def parse_agent_schedule(job_xml: str) -> list[ExtractedSchedule]:
    """Parse SQL Server Agent job schedule from XML or text.

    Supports common patterns found in msdb job definitions.
    """
    schedules: list[ExtractedSchedule] = []

    # Simple pattern matching for job definitions
    job_pattern = re.compile(r"job_name\s*[=:]\s*['\"]?([^'\";\n]+)", re.IGNORECASE)
    freq_pattern = re.compile(r"freq_type\s*[=:]\s*(\d+)", re.IGNORECASE)
    time_pattern = re.compile(r"active_start_time\s*[=:]\s*(\d+)", re.IGNORECASE)
    package_pattern = re.compile(r"package\s*[=:]\s*['\"]?([^'\";\n]+\.dtsx)", re.IGNORECASE)

    job_name = "unknown"
    m = job_pattern.search(job_xml)
    if m:
        job_name = m.group(1).strip()

    package_name = ""
    m = package_pattern.search(job_xml)
    if m:
        package_name = m.group(1).strip()

    freq = FrequencyType.DAILY
    m = freq_pattern.search(job_xml)
    if m:
        freq_code = int(m.group(1))
        freq_map = {1: FrequencyType.ON_DEMAND, 4: FrequencyType.DAILY,
                    8: FrequencyType.WEEKLY, 16: FrequencyType.MONTHLY,
                    32: FrequencyType.MONTHLY, 64: FrequencyType.ON_DEMAND}
        freq = freq_map.get(freq_code, FrequencyType.DAILY)

    start_time = "00:00"
    m = time_pattern.search(job_xml)
    if m:
        raw = m.group(1).zfill(6)
        start_time = f"{raw[:2]}:{raw[2:4]}"

    schedules.append(ExtractedSchedule(
        job_name=job_name,
        package_name=package_name,
        frequency=freq,
        start_time=start_time,
    ))
    return schedules


# =====================================================================
# Fabric Trigger Generation
# =====================================================================


@dataclass
class FabricTrigger:
    """A Fabric scheduled trigger definition."""

    name: str
    pipeline_name: str
    schedule_type: str = "ScheduleTrigger"
    cron_expression: str = ""
    recurrence: dict[str, Any] = field(default_factory=dict)
    enabled: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "type": self.schedule_type,
            "properties": {
                "pipelines": [{"pipelineReference": {"referenceName": self.pipeline_name}}],
                "recurrence": self.recurrence or {"schedule": self.cron_expression},
                "runtimeState": "Started" if self.enabled else "Stopped",
            },
        }


def schedule_to_trigger(schedule: ExtractedSchedule, pipeline_name: str = "") -> FabricTrigger:
    """Convert an extracted schedule to a Fabric trigger."""
    p_name = pipeline_name or schedule.package_name.replace(".dtsx", "")
    recurrence: dict[str, Any] = {"frequency": schedule.frequency.value, "interval": schedule.interval}

    if schedule.frequency == FrequencyType.WEEKLY and schedule.days_of_week:
        recurrence["schedule"] = {"weekDays": schedule.days_of_week}
    if schedule.frequency == FrequencyType.MONTHLY and schedule.day_of_month:
        recurrence["schedule"] = {"monthDays": [schedule.day_of_month]}

    return FabricTrigger(
        name=f"trigger_{p_name}",
        pipeline_name=p_name,
        cron_expression=schedule.cron_expression,
        recurrence=recurrence,
        enabled=schedule.enabled,
    )


# =====================================================================
# Cross-Pipeline Dependencies & SLAs
# =====================================================================


@dataclass
class PipelineDependency:
    """A dependency between two pipelines."""

    upstream: str
    downstream: str
    dependency_type: str = "completion"  # completion, success, failure

    def to_dict(self) -> dict[str, Any]:
        return {
            "upstream": self.upstream,
            "downstream": self.downstream,
            "dependency_type": self.dependency_type,
        }


@dataclass
class SLADefinition:
    """Service Level Agreement for a pipeline."""

    pipeline_name: str
    max_duration_minutes: int = 60
    must_complete_by: str = ""  # HH:MM
    alert_channels: list[str] = field(default_factory=list)
    priority: str = "medium"  # low, medium, high, critical

    def to_dict(self) -> dict[str, Any]:
        return {
            "pipeline_name": self.pipeline_name,
            "max_duration_minutes": self.max_duration_minutes,
            "must_complete_by": self.must_complete_by,
            "alert_channels": self.alert_channels,
            "priority": self.priority,
        }


def build_dependency_graph(
    dependencies: list[PipelineDependency],
) -> dict[str, list[str]]:
    """Build an adjacency list from pipeline dependencies."""
    graph: dict[str, list[str]] = {}
    for dep in dependencies:
        graph.setdefault(dep.upstream, [])
        graph.setdefault(dep.downstream, [])
        graph[dep.upstream].append(dep.downstream)
    return graph


def topological_order(graph: dict[str, list[str]]) -> list[str]:
    """Return pipelines in topological (execution) order."""
    in_degree: dict[str, int] = {n: 0 for n in graph}
    for children in graph.values():
        for c in children:
            in_degree[c] = in_degree.get(c, 0) + 1

    queue = [n for n, d in in_degree.items() if d == 0]
    order: list[str] = []
    while queue:
        node = queue.pop(0)
        order.append(node)
        for child in graph.get(node, []):
            in_degree[child] -= 1
            if in_degree[child] == 0:
                queue.append(child)
    return order


# =====================================================================
# External Scheduler Adapters
# =====================================================================


def generate_airflow_dag(schedules: list[ExtractedSchedule], dag_id: str = "ssis_migration") -> str:
    """Generate an Apache Airflow DAG definition from schedules."""
    lines = [
        '"""Auto-generated Airflow DAG from SSIS Agent schedules."""',
        "from airflow import DAG",
        "from airflow.operators.bash import BashOperator",
        "from datetime import datetime",
        "",
        f'with DAG("{dag_id}",',
        '        start_date=datetime(2026, 1, 1),',
        f'        schedule_interval="{schedules[0].cron_expression if schedules else "0 0 * * *"}",',
        "        catchup=False) as dag:",
        "",
    ]
    for _i, sched in enumerate(schedules):
        task_id = re.sub(r"[^a-zA-Z0-9_]", "_", sched.job_name.lower())
        lines.append(f"    {task_id} = BashOperator(")
        lines.append(f'        task_id="{task_id}",')
        lines.append(f'        bash_command="ssis2fabric migrate --package {sched.package_name}",')
        lines.append("    )")
        lines.append("")
    return "\n".join(lines)


def generate_logic_app(schedule: ExtractedSchedule) -> dict[str, Any]:
    """Generate Azure Logic App workflow JSON for a schedule."""
    return {
        "definition": {
            "$schema": "https://schema.management.azure.com/providers/Microsoft.Logic/schemas/2016-06-01/workflowdefinition.json",
            "triggers": {
                "Recurrence": {
                    "type": "Recurrence",
                    "recurrence": {
                        "frequency": schedule.frequency.value.capitalize(),
                        "interval": schedule.interval,
                    },
                },
            },
            "actions": {
                "Run_Migration": {
                    "type": "Http",
                    "inputs": {
                        "method": "POST",
                        "uri": "https://<fabric-api>/api/v1/migrate",
                        "body": {"package": schedule.package_name},
                    },
                },
            },
        },
    }


# =====================================================================
# Report
# =====================================================================


def write_schedule_report(
    schedules: list[ExtractedSchedule],
    triggers: list[FabricTrigger],
    output_path: Path,
) -> Path:
    """Write schedule extraction report."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "schedules_extracted": len(schedules),
        "triggers_generated": len(triggers),
        "schedules": [s.to_dict() for s in schedules],
        "triggers": [t.to_dict() for t in triggers],
    }
    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    logger.info("schedule_report_written", path=str(output_path))
    return output_path
