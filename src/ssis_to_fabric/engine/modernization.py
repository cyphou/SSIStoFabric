"""
Legacy Modernization Patterns — Phase 38
==========================================
Analyses SSIS packages and migration artefacts to recommend modern data platform
patterns and produce actionable migration wave plans.

Key capabilities:

- **Medallion architecture**: classifies SSIS components into Bronze/Silver/Gold
  layers and generates a Lakehouse blueprint.
- **Pattern recommendation**: scores packages against five modernization patterns
  (Medallion, Star Schema, Hub-and-Spoke, Event-Driven, Data-Mesh).
- **Technical-debt scoring**: quantifies migration risk per package using
  complexity, TODO count, manual-review proportion, and unsupported components.
- **Wave planner**: clusters packages into dependency-aware migration waves
  ordered by debt score.
"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

import structlog

if TYPE_CHECKING:
    from ssis_to_fabric.analyzer.models import SSISPackage
    from ssis_to_fabric.engine.migration_engine import MigrationPlan, PackageAssessment

logger = structlog.get_logger(__name__)


# =====================================================================
# Enums & Data Structures
# =====================================================================


class MedallionLayer(str, Enum):
    """Medallion architecture data layer."""

    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    UNCLASSIFIED = "unclassified"


class ModernizationPattern(str, Enum):
    """High-level modernization pattern."""

    MEDALLION = "medallion"
    STAR_SCHEMA = "star_schema"
    HUB_AND_SPOKE = "hub_and_spoke"
    EVENT_DRIVEN = "event_driven"
    DATA_MESH = "data_mesh"


@dataclass
class LayerClassification:
    """Classification of a single SSIS component into a medallion layer."""

    component_name: str
    component_type: str
    layer: MedallionLayer
    reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "component_name": self.component_name,
            "component_type": self.component_type,
            "layer": self.layer.value,
            "reason": self.reason,
        }


@dataclass
class MedallionBlueprint:
    """Medallion architecture blueprint for a set of packages."""

    bronze: list[LayerClassification] = field(default_factory=list)
    silver: list[LayerClassification] = field(default_factory=list)
    gold: list[LayerClassification] = field(default_factory=list)
    unclassified: list[LayerClassification] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.bronze) + len(self.silver) + len(self.gold) + len(self.unclassified)

    def to_dict(self) -> dict[str, Any]:
        return {
            "bronze": [c.to_dict() for c in self.bronze],
            "silver": [c.to_dict() for c in self.silver],
            "gold": [c.to_dict() for c in self.gold],
            "unclassified": [c.to_dict() for c in self.unclassified],
            "summary": {
                "bronze": len(self.bronze),
                "silver": len(self.silver),
                "gold": len(self.gold),
                "unclassified": len(self.unclassified),
            },
        }


@dataclass
class DebtScore:
    """Technical-debt score for a single package."""

    package_name: str
    complexity_score: float = 0.0
    todo_score: float = 0.0
    manual_review_score: float = 0.0
    unsupported_score: float = 0.0
    total: float = 0.0
    risk_level: str = "LOW"

    def to_dict(self) -> dict[str, Any]:
        return {
            "package_name": self.package_name,
            "complexity_score": round(self.complexity_score, 2),
            "todo_score": round(self.todo_score, 2),
            "manual_review_score": round(self.manual_review_score, 2),
            "unsupported_score": round(self.unsupported_score, 2),
            "total": round(self.total, 2),
            "risk_level": self.risk_level,
        }


@dataclass
class PatternScore:
    """Suitability score for a modernization pattern."""

    pattern: ModernizationPattern
    score: float = 0.0
    rationale: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "pattern": self.pattern.value,
            "score": round(self.score, 2),
            "rationale": self.rationale,
        }


@dataclass
class MigrationWave:
    """A group of packages to migrate together."""

    wave_number: int
    packages: list[str] = field(default_factory=list)
    total_debt: float = 0.0
    avg_debt: float = 0.0
    estimated_hours: float = 0.0
    description: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "wave_number": self.wave_number,
            "packages": self.packages,
            "total_debt": round(self.total_debt, 2),
            "avg_debt": round(self.avg_debt, 2),
            "estimated_hours": round(self.estimated_hours, 2),
            "description": self.description,
        }


@dataclass
class ModernizationReport:
    """Complete modernization analysis report."""

    recommended_pattern: ModernizationPattern = ModernizationPattern.MEDALLION
    pattern_scores: list[PatternScore] = field(default_factory=list)
    blueprint: MedallionBlueprint = field(default_factory=MedallionBlueprint)
    debt_scores: list[DebtScore] = field(default_factory=list)
    waves: list[MigrationWave] = field(default_factory=list)
    total_packages: int = 0
    avg_debt: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "recommended_pattern": self.recommended_pattern.value,
            "pattern_scores": [p.to_dict() for p in self.pattern_scores],
            "blueprint": self.blueprint.to_dict(),
            "debt_scores": [d.to_dict() for d in self.debt_scores],
            "waves": [w.to_dict() for w in self.waves],
            "total_packages": self.total_packages,
            "avg_debt": round(self.avg_debt, 2),
        }


# =====================================================================
# Bronze / Silver / Gold classifier
# =====================================================================

# Component types typically at each layer
_BRONZE_TYPES = {
    "OLE DB Source", "ADO NET Source", "Flat File Source", "Excel Source",
    "ODBC Source", "Raw File Source", "XML Source", "CDC Source",
    "OLEDB", "ADO.NET", "FLATFILE", "FILE", "Source",
}
_SILVER_TYPES = {
    "Derived Column", "Conditional Split", "Lookup", "Data Conversion",
    "Merge Join", "Union All", "Aggregate", "Sort", "Script Component",
    "Character Map", "Multicast", "Merge", "Slowly Changing Dimension",
    "Fuzzy Lookup", "Term Lookup", "Audit", "Row Count",
}
_GOLD_TYPES = {
    "OLE DB Destination", "ADO NET Destination", "Flat File Destination",
    "Excel Destination", "OLE DB Command", "SQL Server Destination",
    "Destination", "Fact", "Dimension",
}

# Task types at each layer
_BRONZE_TASK_TYPES = {"Execute SQL Task", "Bulk Insert Task", "Data Flow Task"}
_GOLD_TASK_TYPES = {"Analysis Services Processing Task", "Execute SQL Task"}


def classify_component(name: str, comp_type: str) -> LayerClassification:
    """Classify a single SSIS component into a medallion layer."""
    name_lower = name.lower()

    # Name heuristics
    if any(kw in name_lower for kw in ("staging", "stg_", "raw_", "ingest", "extract", "source", "src_")):
        return LayerClassification(name, comp_type, MedallionLayer.BRONZE, "Name suggests ingestion/staging")
    if any(kw in name_lower for kw in ("clean", "transform", "valid", "enrich", "lookup", "join", "merge")):
        return LayerClassification(name, comp_type, MedallionLayer.SILVER, "Name suggests transformation/cleansing")
    if any(kw in name_lower for kw in ("fact_", "dim_", "report", "agg_", "summary", "gold", "mart")):
        return LayerClassification(name, comp_type, MedallionLayer.GOLD, "Name suggests analytics/serving layer")

    # Type heuristics
    if comp_type in _BRONZE_TYPES:
        return LayerClassification(name, comp_type, MedallionLayer.BRONZE, f"Type '{comp_type}' is a source/ingestion component")
    if comp_type in _SILVER_TYPES:
        return LayerClassification(name, comp_type, MedallionLayer.SILVER, f"Type '{comp_type}' is a transformation component")
    if comp_type in _GOLD_TYPES:
        return LayerClassification(name, comp_type, MedallionLayer.GOLD, f"Type '{comp_type}' is a destination/serving component")

    return LayerClassification(name, comp_type, MedallionLayer.UNCLASSIFIED, "Could not classify")


def generate_medallion_blueprint(packages: list[SSISPackage]) -> MedallionBlueprint:
    """Classify all components from a set of packages into medallion layers."""
    bp = MedallionBlueprint()
    for pkg in packages:
        _classify_tasks_recursive(bp, pkg.control_flow_tasks)
    return bp


def _classify_tasks_recursive(bp: MedallionBlueprint, tasks: list) -> None:
    """Recursively classify tasks and their data flow components."""
    for task in tasks:
        cls = classify_component(task.name, task.task_type if isinstance(task.task_type, str) else task.task_type.value)
        _add_to_blueprint(bp, cls)
        # Classify data flow components nested within the task
        for comp in task.data_flow_components:
            comp_type = comp.component_type if isinstance(comp.component_type, str) else comp.component_type.value
            cls = classify_component(comp.name, comp_type)
            _add_to_blueprint(bp, cls)
        # Recurse into child tasks (containers)
        _classify_tasks_recursive(bp, task.child_tasks)


def _add_to_blueprint(bp: MedallionBlueprint, cls: LayerClassification) -> None:
    """Add a classification to the right layer bucket."""
    if cls.layer == MedallionLayer.BRONZE:
        bp.bronze.append(cls)
    elif cls.layer == MedallionLayer.SILVER:
        bp.silver.append(cls)
    elif cls.layer == MedallionLayer.GOLD:
        bp.gold.append(cls)
    else:
        bp.unclassified.append(cls)


# =====================================================================
# Technical-Debt Scoring
# =====================================================================

_COMPLEXITY_WEIGHTS = {"LOW": 1.0, "MEDIUM": 3.0, "HIGH": 5.0, "MANUAL": 8.0}


def score_technical_debt(
    assessment: PackageAssessment,
    *,
    todo_count: int = 0,
    manual_review_count: int = 0,
    unsupported_count: int = 0,
) -> DebtScore:
    """Score the technical debt of a single package.

    Combines complexity, TODO count, manual-review items, and unsupported
    components into a single 0–100 score.
    """
    ds = DebtScore(package_name=assessment.name)

    # Complexity component (0-40 pts)
    ds.complexity_score = min(_COMPLEXITY_WEIGHTS.get(assessment.complexity, 1.0) * 5, 40.0)

    # TODO stub component (0-25 pts)
    ds.todo_score = min(todo_count * 2.5, 25.0)

    # Manual review proportion (0-20 pts)
    total_items = max(assessment.tasks + assessment.data_flows, 1)
    ds.manual_review_score = min((manual_review_count / total_items) * 100, 20.0)

    # Unsupported components (0-15 pts)
    ds.unsupported_score = min(unsupported_count * 5, 15.0)

    ds.total = ds.complexity_score + ds.todo_score + ds.manual_review_score + ds.unsupported_score

    if ds.total >= 60:
        ds.risk_level = "HIGH"
    elif ds.total >= 30:
        ds.risk_level = "MEDIUM"
    else:
        ds.risk_level = "LOW"

    return ds


# =====================================================================
# Pattern Recommendation
# =====================================================================


def recommend_pattern(
    packages: list[SSISPackage],
    blueprint: MedallionBlueprint | None = None,
) -> list[PatternScore]:
    """Score five modernization patterns and return them sorted by score.

    Heuristics are based on package characteristics:
    - Medallion: works for almost everything (good default).
    - Star Schema: favoured when dimension/fact tasks are present.
    - Hub-and-Spoke: favoured with many connections.
    - Event-Driven: favoured with CDC or streaming components.
    - Data Mesh: favoured with domain-separated packages.
    """
    scores: dict[ModernizationPattern, PatternScore] = {
        p: PatternScore(pattern=p) for p in ModernizationPattern
    }

    total_connections = sum(len(p.connection_managers) for p in packages)

    # Medallion — universal default, boosted by having clear layers
    s = scores[ModernizationPattern.MEDALLION]
    s.score = 60.0  # strong default
    if blueprint and blueprint.total > 0:
        classified_pct = (len(blueprint.bronze) + len(blueprint.silver) + len(blueprint.gold)) / blueprint.total * 100
        s.score += classified_pct * 0.3
        s.rationale.append(f"{classified_pct:.0f}% of components fit medallion layers")
    else:
        s.rationale.append("Good default pattern for most SSIS-to-Fabric migrations")

    # Star Schema — look for dimension/fact naming patterns
    star_hits = 0
    for pkg in packages:
        for task in pkg.control_flow_tasks:
            if any(kw in task.name.lower() for kw in ("dim_", "fact_", "dimension", "scd")):
                star_hits += 1
    s = scores[ModernizationPattern.STAR_SCHEMA]
    s.score = min(star_hits * 15, 80.0)
    if star_hits:
        s.rationale.append(f"{star_hits} dimension/fact tasks detected")
    else:
        s.rationale.append("No star-schema indicators found")

    # Hub-and-Spoke — many distinct connections
    s = scores[ModernizationPattern.HUB_AND_SPOKE]
    if total_connections >= 5:
        s.score = min(total_connections * 8, 75.0)
        s.rationale.append(f"{total_connections} connections favour hub-and-spoke topology")
    else:
        s.score = total_connections * 5
        s.rationale.append(f"Only {total_connections} connections; hub-and-spoke less compelling")

    # Event-Driven — CDC, streaming, real-time patterns
    cdc_hits = 0
    for pkg in packages:
        for task in pkg.control_flow_tasks:
            if any(kw in task.name.lower() for kw in ("cdc", "stream", "event", "realtime", "queue")):
                cdc_hits += 1
    s = scores[ModernizationPattern.EVENT_DRIVEN]
    s.score = min(cdc_hits * 20, 80.0)
    if cdc_hits:
        s.rationale.append(f"{cdc_hits} CDC/streaming tasks detected")
    else:
        s.rationale.append("No event-driven indicators found")

    # Data Mesh — many packages with distinct naming prefixes (domain separation)
    s = scores[ModernizationPattern.DATA_MESH]
    if len(packages) >= 5:
        prefixes = {p.name.split("_")[0].lower() for p in packages if "_" in p.name}
        s.score = min(len(prefixes) * 10, 70.0)
        if prefixes:
            s.rationale.append(f"{len(prefixes)} domain prefixes: {', '.join(sorted(prefixes)[:5])}")
        else:
            s.rationale.append("Packages lack domain-prefix naming convention")
    else:
        s.score = 10.0
        s.rationale.append("Too few packages for data-mesh topology")

    sorted_scores = sorted(scores.values(), key=lambda x: x.score, reverse=True)
    return sorted_scores


# =====================================================================
# Wave Planner
# =====================================================================


def plan_migration_waves(
    debt_scores: list[DebtScore],
    *,
    max_wave_size: int = 5,
    max_wave_debt: float = 100.0,
) -> list[MigrationWave]:
    """Cluster packages into migration waves ordered by increasing debt.

    Packages with the lowest debt are in Wave 1 (quick wins), while
    high-debt packages are staged into later waves.
    """
    if not debt_scores:
        return []

    sorted_scores = sorted(debt_scores, key=lambda d: d.total)
    waves: list[MigrationWave] = []
    current_wave = MigrationWave(wave_number=1)

    for ds in sorted_scores:
        # Start a new wave if the current one is full
        if (
            len(current_wave.packages) >= max_wave_size
            or (current_wave.total_debt + ds.total > max_wave_debt and current_wave.packages)
        ):
            _finalise_wave(current_wave)
            waves.append(current_wave)
            current_wave = MigrationWave(wave_number=len(waves) + 1)

        current_wave.packages.append(ds.package_name)
        current_wave.total_debt += ds.total

    if current_wave.packages:
        _finalise_wave(current_wave)
        waves.append(current_wave)

    # Assign descriptions
    for w in waves:
        if w.wave_number == 1:
            w.description = "Quick wins — lowest debt, highest confidence"
        elif w.avg_debt < 30:
            w.description = "Low-risk packages with minor manual adjustments"
        elif w.avg_debt < 60:
            w.description = "Moderate complexity — requires remediation review"
        else:
            w.description = "High complexity — needs significant manual attention"

    return waves


def _finalise_wave(wave: MigrationWave) -> None:
    """Compute summary stats for a wave."""
    if wave.packages:
        wave.avg_debt = wave.total_debt / len(wave.packages)


# =====================================================================
# Modernization Engine
# =====================================================================


class ModernizationEngine:
    """Analyse packages and produce a modernization report."""

    def __init__(
        self,
        *,
        pattern: ModernizationPattern | None = None,
        max_wave_size: int = 5,
        max_wave_debt: float = 100.0,
    ):
        self.forced_pattern = pattern
        self.max_wave_size = max_wave_size
        self.max_wave_debt = max_wave_debt

    def analyze(
        self,
        packages: list[SSISPackage],
        assessments: list[PackageAssessment] | None = None,
        plan: MigrationPlan | None = None,
    ) -> ModernizationReport:
        """Run the full modernization analysis.

        Parameters
        ----------
        packages : list[SSISPackage]
            Parsed SSIS packages.
        assessments : list[PackageAssessment], optional
            Pre-computed assessments (used for debt scoring).
        plan : MigrationPlan, optional
            Completed migration plan (used to count TODOs/manual items).
        """
        report = ModernizationReport(total_packages=len(packages))

        # 1. Medallion blueprint
        report.blueprint = generate_medallion_blueprint(packages)

        # 2. Pattern recommendation
        report.pattern_scores = recommend_pattern(packages, report.blueprint)
        if self.forced_pattern:
            report.recommended_pattern = self.forced_pattern
        elif report.pattern_scores:
            report.recommended_pattern = report.pattern_scores[0].pattern

        # 3. Debt scoring
        assess_map: dict[str, PackageAssessment] = {}
        if assessments:
            assess_map = {a.name: a for a in assessments}

        plan_stats = _extract_plan_stats(plan) if plan else {}

        for pkg in packages:
            assessment = assess_map.get(pkg.name)
            stats = plan_stats.get(pkg.name, {})
            if assessment:
                ds = score_technical_debt(
                    assessment,
                    todo_count=stats.get("todo_count", 0),
                    manual_review_count=stats.get("manual_count", 0),
                    unsupported_count=stats.get("unsupported_count", 0),
                )
            else:
                # Synthesise a minimal assessment
                from ssis_to_fabric.engine.migration_engine import PackageAssessment as PA
                synth = PA(
                    name=pkg.name,
                    tasks=pkg.total_tasks,
                    data_flows=pkg.total_data_flows,
                    connections=len(pkg.connection_managers),
                    complexity=pkg.overall_complexity.value,
                )
                ds = score_technical_debt(
                    synth,
                    todo_count=stats.get("todo_count", 0),
                    manual_review_count=stats.get("manual_count", 0),
                    unsupported_count=stats.get("unsupported_count", 0),
                )
            report.debt_scores.append(ds)

        if report.debt_scores:
            report.avg_debt = sum(d.total for d in report.debt_scores) / len(report.debt_scores)

        # 4. Wave planning
        report.waves = plan_migration_waves(
            report.debt_scores,
            max_wave_size=self.max_wave_size,
            max_wave_debt=self.max_wave_debt,
        )

        logger.info(
            "modernization_analysis_complete",
            packages=len(packages),
            pattern=report.recommended_pattern.value,
            waves=len(report.waves),
            avg_debt=round(report.avg_debt, 1),
        )
        return report

    def write_report(self, report: ModernizationReport, output_path: Path) -> Path:
        """Write modernization report as JSON."""
        report_file = output_path / "modernization_report.json"
        report_file.write_text(
            json.dumps(report.to_dict(), indent=2),
            encoding="utf-8",
        )
        return report_file


# =====================================================================
# Helpers
# =====================================================================


def _extract_plan_stats(plan: MigrationPlan) -> dict[str, dict[str, int]]:
    """Extract per-package TODO/manual/unsupported counts from a plan."""
    stats: dict[str, dict[str, int]] = defaultdict(lambda: {"todo_count": 0, "manual_count": 0, "unsupported_count": 0})
    for item in plan.items:
        s = stats[item.source_package]
        if item.target_artifact.value == "manual_review":
            s["manual_count"] += 1
        for note in item.notes:
            if "TODO" in note.upper():
                s["todo_count"] += 1
            if "unsupported" in note.lower():
                s["unsupported_count"] += 1
    return dict(stats)
