"""
Performance Optimization
=========================
Phase 24: Migration profiler, Spark optimizer, parallelism advisor,
benchmarking, and capacity auto-tuning.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any

from ssis_to_fabric.logging_config import get_logger

if TYPE_CHECKING:
    from pathlib import Path

logger = get_logger(__name__)


# =====================================================================
# Migration Profiler
# =====================================================================


@dataclass
class ProfileMetrics:
    """Resource metrics captured during a migration operation."""

    operation: str
    package_name: str = ""
    start_time: float = 0.0
    end_time: float = 0.0
    duration_seconds: float = 0.0
    peak_memory_mb: float = 0.0
    artifacts_generated: int = 0
    lines_of_code: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "operation": self.operation,
            "package_name": self.package_name,
            "duration_seconds": round(self.duration_seconds, 3),
            "peak_memory_mb": round(self.peak_memory_mb, 1),
            "artifacts_generated": self.artifacts_generated,
            "lines_of_code": self.lines_of_code,
        }


class MigrationProfiler:
    """Profiles migration operations for performance analysis."""

    def __init__(self) -> None:
        self.metrics: list[ProfileMetrics] = []
        self._active: ProfileMetrics | None = None

    def start(self, operation: str, package_name: str = "") -> None:
        self._active = ProfileMetrics(
            operation=operation,
            package_name=package_name,
            start_time=time.monotonic(),
        )

    def stop(self, artifacts: int = 0, lines: int = 0) -> ProfileMetrics:
        if not self._active:
            return ProfileMetrics(operation="unknown")
        self._active.end_time = time.monotonic()
        self._active.duration_seconds = self._active.end_time - self._active.start_time
        self._active.artifacts_generated = artifacts
        self._active.lines_of_code = lines
        self.metrics.append(self._active)
        result = self._active
        self._active = None
        return result

    def summary(self) -> dict[str, Any]:
        total_duration = sum(m.duration_seconds for m in self.metrics)
        return {
            "total_operations": len(self.metrics),
            "total_duration_seconds": round(total_duration, 3),
            "total_artifacts": sum(m.artifacts_generated for m in self.metrics),
            "total_lines": sum(m.lines_of_code for m in self.metrics),
            "operations": [m.to_dict() for m in self.metrics],
        }


# =====================================================================
# Spark Notebook Optimizer
# =====================================================================


class OptimizationType(str, Enum):
    PARTITION_HINT = "partition_hint"
    BROADCAST_JOIN = "broadcast_join"
    CACHE = "cache"
    COALESCE = "coalesce"
    PERSIST = "persist"
    REPARTITION = "repartition"


@dataclass
class OptimizationRecommendation:
    """A Spark optimization recommendation."""

    optimization_type: OptimizationType
    description: str
    code_suggestion: str = ""
    estimated_improvement: str = ""  # e.g., "30-50% faster"
    applicable_to: str = ""  # notebook or line reference

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": self.optimization_type.value,
            "description": self.description,
            "code_suggestion": self.code_suggestion,
            "estimated_improvement": self.estimated_improvement,
            "applicable_to": self.applicable_to,
        }


def analyze_notebook_for_optimizations(code: str, notebook_name: str = "") -> list[OptimizationRecommendation]:
    """Analyze a Spark notebook and suggest optimizations."""
    recommendations: list[OptimizationRecommendation] = []

    # Check for joins without broadcast hints
    if ".join(" in code and "broadcast" not in code:
        lines_with_joins = [
            i + 1 for i, line in enumerate(code.split("\n")) if ".join(" in line
        ]
        if lines_with_joins:
            recommendations.append(OptimizationRecommendation(
                optimization_type=OptimizationType.BROADCAST_JOIN,
                description="Join detected without broadcast hint. For small lookup tables, use broadcast join.",
                code_suggestion="from pyspark.sql.functions import broadcast\ndf.join(broadcast(lookup_df), ...)",
                estimated_improvement="30-50% faster for small dimension tables",
                applicable_to=notebook_name,
            ))

    # Check for missing cache/persist
    if ".count()" in code or ".show()" in code:
        action_count = code.count(".count()") + code.count(".show()") + code.count(".collect()")
        if action_count > 1 and ".cache()" not in code and ".persist()" not in code:
            recommendations.append(OptimizationRecommendation(
                optimization_type=OptimizationType.CACHE,
                description=f"Multiple actions ({action_count}) detected without caching. Cache intermediate results.",
                code_suggestion="df = df.cache()  # before repeated actions",
                estimated_improvement="Avoids recomputation; significant for complex DAGs",
                applicable_to=notebook_name,
            ))

    # Check for writes without repartition/coalesce
    if (".write." in code or ".save(" in code) and ".coalesce(" not in code and ".repartition(" not in code:
        recommendations.append(OptimizationRecommendation(
            optimization_type=OptimizationType.COALESCE,
            description="Write operation without partition control. Use coalesce/repartition for optimal file sizes.",
            code_suggestion="df.coalesce(4).write.format('delta').save(path)",
            estimated_improvement="Better file sizes; fewer small files",
            applicable_to=notebook_name,
        ))

    # Check for partition hints in groupBy
    if ".groupBy(" in code and ".repartition(" not in code:
        recommendations.append(OptimizationRecommendation(
            optimization_type=OptimizationType.REPARTITION,
            description="GroupBy without repartition. Pre-repartition on group key for better shuffles.",
            code_suggestion="df.repartition('group_key').groupBy('group_key').agg(...)",
            estimated_improvement="15-30% faster for large shuffles",
            applicable_to=notebook_name,
        ))

    return recommendations


# =====================================================================
# Pipeline Parallelism Advisor
# =====================================================================


@dataclass
class ParallelismAdvice:
    """Advice on pipeline parallelism opportunities."""

    pipeline_name: str
    independent_branches: list[list[str]] = field(default_factory=list)
    max_parallelism: int = 1
    current_parallelism: int = 1
    recommendation: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "pipeline_name": self.pipeline_name,
            "independent_branches": self.independent_branches,
            "max_parallelism": self.max_parallelism,
            "current_parallelism": self.current_parallelism,
            "recommendation": self.recommendation,
        }


def analyze_pipeline_parallelism(pipeline_json: dict[str, Any]) -> ParallelismAdvice:
    """Analyze a Data Factory pipeline for parallelism opportunities."""
    name = pipeline_json.get("name", "unknown")
    activities = pipeline_json.get("properties", {}).get("activities", [])

    # Build dependency map
    deps: dict[str, set[str]] = {}
    for act in activities:
        act_name = act.get("name", "")
        dep_on = set()
        for dep in act.get("dependsOn", []):
            dep_on.add(dep.get("activity", ""))
        deps[act_name] = dep_on

    # Find independent activities (no shared dependencies)
    independent: list[list[str]] = []
    roots = [a for a, d in deps.items() if not d]
    if len(roots) > 1:
        independent.append(roots)

    # Find parallel-capable groups at each level
    remaining = {a for a in deps if a not in roots}
    while remaining:
        layer = [a for a in remaining if deps[a].issubset(set().union(*independent) if independent else set())]
        if not layer:
            break
        if len(layer) > 1:
            independent.append(layer)
        remaining -= set(layer)

    max_parallel = max((len(branch) for branch in independent), default=1)

    advice = ParallelismAdvice(
        pipeline_name=name,
        independent_branches=independent,
        max_parallelism=max_parallel,
        current_parallelism=1,
    )

    if max_parallel > 1:
        advice.recommendation = (
            f"Up to {max_parallel} activities can run concurrently. "
            f"Wrap independent activities in parallel branches."
        )
    else:
        advice.recommendation = "Pipeline is already sequential; no parallelism opportunities."

    return advice


# =====================================================================
# Capacity Auto-Tuning
# =====================================================================


@dataclass
class CapacityRecommendation:
    """Fabric capacity SKU recommendation."""

    workload_type: str
    estimated_cu_per_hour: float = 0.0
    recommended_sku: str = "F2"
    monthly_cost_estimate: float = 0.0
    reasoning: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "workload_type": self.workload_type,
            "estimated_cu_per_hour": round(self.estimated_cu_per_hour, 2),
            "recommended_sku": self.recommended_sku,
            "monthly_cost_estimate": round(self.monthly_cost_estimate, 2),
            "reasoning": self.reasoning,
        }


_SKU_CU_MAP = {
    "F2": 2, "F4": 4, "F8": 8, "F16": 16, "F32": 32, "F64": 64, "F128": 128,
}

_SKU_MONTHLY_COST = {
    "F2": 262, "F4": 525, "F8": 1050, "F16": 2100, "F32": 4200, "F64": 8400, "F128": 16800,
}


def recommend_capacity(
    pipeline_count: int,
    notebook_count: int,
    daily_executions: int = 1,
    avg_notebook_lines: int = 100,
) -> CapacityRecommendation:
    """Recommend a Fabric capacity SKU based on workload analysis."""
    # Estimate CU consumption
    pipeline_cu = pipeline_count * 0.02 * daily_executions
    notebook_cu = notebook_count * (avg_notebook_lines / 100) * 0.1 * daily_executions
    total_cu = pipeline_cu + notebook_cu

    # Find smallest SKU that fits
    recommended = "F2"
    for sku, cu in sorted(_SKU_CU_MAP.items(), key=lambda x: x[1]):
        if cu >= total_cu:
            recommended = sku
            break
    else:
        recommended = "F128"

    return CapacityRecommendation(
        workload_type=(
            "hybrid" if pipeline_count > 0 and notebook_count > 0
            else "pipelines" if notebook_count == 0
            else "notebooks"
        ),
        estimated_cu_per_hour=total_cu,
        recommended_sku=recommended,
        monthly_cost_estimate=_SKU_MONTHLY_COST.get(recommended, 0),
        reasoning=(
            f"{pipeline_count} pipelines + {notebook_count} notebooks"
            f" × {daily_executions} daily = {total_cu:.1f} CU/hour"
        ),
    )


# =====================================================================
# Benchmark Report
# =====================================================================


@dataclass
class BenchmarkResult:
    """Result of a performance benchmark."""

    name: str
    metric: str
    value: float
    unit: str = "seconds"
    baseline: float = 0.0
    improvement_pct: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "metric": self.metric,
            "value": round(self.value, 3),
            "unit": self.unit,
            "baseline": round(self.baseline, 3),
            "improvement_pct": round(self.improvement_pct, 1),
        }


def write_benchmark_report(
    profiler: MigrationProfiler,
    optimizations: list[OptimizationRecommendation],
    capacity: CapacityRecommendation | None,
    output_path: Path,
) -> Path:
    """Write a comprehensive benchmark/performance report."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "profiler_summary": profiler.summary(),
        "optimization_recommendations": [o.to_dict() for o in optimizations],
        "capacity_recommendation": capacity.to_dict() if capacity else None,
    }
    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    logger.info("benchmark_report_written", path=str(output_path))
    return output_path
