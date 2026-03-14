"""
Streaming & Real-Time
======================
Phase 20: Event Hub/Kafka source detection, Fabric Eventstream generation,
CDC-to-push conversion, and streaming readiness assessment.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any

from ssis_to_fabric.logging_config import get_logger

if TYPE_CHECKING:
    from pathlib import Path

logger = get_logger(__name__)


# =====================================================================
# Streaming Source Detection
# =====================================================================


class StreamingSourceType(str, Enum):
    EVENT_HUB = "event_hub"
    KAFKA = "kafka"
    CDC = "cdc"
    SERVICE_BUS = "service_bus"
    MQTT = "mqtt"
    CUSTOM = "custom"


@dataclass
class StreamingSource:
    """A detected streaming source in an SSIS package."""

    name: str
    source_type: StreamingSourceType
    connection_string_hint: str = ""
    topic_or_entity: str = ""
    package_name: str = ""
    task_name: str = ""
    confidence: float = 0.0  # 0.0 - 1.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "source_type": self.source_type.value,
            "connection_string_hint": self.connection_string_hint,
            "topic_or_entity": self.topic_or_entity,
            "package_name": self.package_name,
            "task_name": self.task_name,
            "confidence": round(self.confidence, 2),
        }


def detect_streaming_sources(package: Any) -> list[StreamingSource]:
    """Detect potential streaming sources in an SSIS package.

    Inspects connection managers and task types for streaming patterns:
    - CDC Source components
    - Event Hub / Service Bus connection strings
    - Kafka-related connection managers
    """
    sources: list[StreamingSource] = []
    pkg_name = getattr(package, "name", "unknown")

    # Check connection managers for streaming hints
    for conn in getattr(package, "connection_managers", []):
        conn_str = getattr(conn, "connection_string", "")
        conn_name = getattr(conn, "name", "")
        conn_type = getattr(conn, "connection_type", "")

        if "servicebus" in conn_str.lower() or "servicebus" in conn_type.lower():
            sources.append(StreamingSource(
                name=conn_name,
                source_type=StreamingSourceType.SERVICE_BUS,
                connection_string_hint="***masked***",
                package_name=pkg_name,
                confidence=0.9,
            ))
        elif "eventhub" in conn_str.lower() or "kafka" in conn_str.lower():
            st = StreamingSourceType.KAFKA if "kafka" in conn_str.lower() else StreamingSourceType.EVENT_HUB
            sources.append(StreamingSource(
                name=conn_name,
                source_type=st,
                connection_string_hint="***masked***",
                package_name=pkg_name,
                confidence=0.85,
            ))

    # Check for CDC tasks
    for task in getattr(package, "control_flow_tasks", []):
        task_type = getattr(task, "task_type", "")
        task_name = getattr(task, "name", "")
        if "cdc" in task_type.lower() or "cdc" in task_name.lower():
            sources.append(StreamingSource(
                name=f"CDC:{task_name}",
                source_type=StreamingSourceType.CDC,
                task_name=task_name,
                package_name=pkg_name,
                confidence=0.95,
            ))

    return sources


# =====================================================================
# Fabric Eventstream Generation
# =====================================================================


@dataclass
class EventstreamConfig:
    """Configuration for a Fabric Eventstream artifact."""

    name: str
    source_type: str
    source_config: dict[str, Any] = field(default_factory=dict)
    destinations: list[dict[str, Any]] = field(default_factory=list)
    transformations: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "type": "Eventstream",
            "properties": {
                "source": {
                    "type": self.source_type,
                    "config": self.source_config,
                },
                "destinations": self.destinations,
                "transformations": self.transformations,
            },
        }


def generate_eventstream(source: StreamingSource) -> EventstreamConfig:
    """Generate a Fabric Eventstream config from a detected streaming source."""
    source_config: dict[str, Any] = {}
    destinations: list[dict[str, Any]] = []

    if source.source_type == StreamingSourceType.EVENT_HUB:
        source_config = {
            "connectionString": "${EVENT_HUB_CONNECTION}",
            "consumerGroup": "$Default",
            "eventHubName": source.topic_or_entity or source.name,
        }
    elif source.source_type == StreamingSourceType.KAFKA:
        source_config = {
            "bootstrapServers": "${KAFKA_BOOTSTRAP_SERVERS}",
            "topic": source.topic_or_entity or source.name,
            "groupId": f"ssis2fabric-{source.name}",
        }
    elif source.source_type == StreamingSourceType.CDC:
        source_config = {
            "type": "CDC",
            "sourceTable": source.topic_or_entity,
            "trackingMode": "push",
            "note": "Converted from SSIS CDC polling to push-based Eventstream",
        }
    elif source.source_type == StreamingSourceType.SERVICE_BUS:
        source_config = {
            "connectionString": "${SERVICE_BUS_CONNECTION}",
            "queueOrTopic": source.topic_or_entity or source.name,
        }

    # Default Lakehouse destination
    destinations.append({
        "type": "Lakehouse",
        "tableName": f"streaming_{source.name.lower().replace(' ', '_')}",
        "format": "delta",
    })

    return EventstreamConfig(
        name=f"es_{source.name}",
        source_type=source.source_type.value,
        source_config=source_config,
        destinations=destinations,
    )


# =====================================================================
# Streaming Readiness Assessment
# =====================================================================


@dataclass
class StreamingReadiness:
    """Assessment of streaming migration readiness."""

    package_name: str
    has_streaming_sources: bool = False
    streaming_sources_count: int = 0
    cdc_sources_count: int = 0
    readiness_score: float = 0.0  # 0-100
    recommendations: list[str] = field(default_factory=list)
    eventstreams: list[EventstreamConfig] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "package_name": self.package_name,
            "has_streaming_sources": self.has_streaming_sources,
            "streaming_sources_count": self.streaming_sources_count,
            "cdc_sources_count": self.cdc_sources_count,
            "readiness_score": round(self.readiness_score, 1),
            "recommendations": self.recommendations,
            "eventstreams": [e.to_dict() for e in self.eventstreams],
        }


def assess_streaming_readiness(package: Any) -> StreamingReadiness:
    """Assess a package's readiness for streaming migration."""
    pkg_name = getattr(package, "name", "unknown")
    sources = detect_streaming_sources(package)
    cdc_count = sum(1 for s in sources if s.source_type == StreamingSourceType.CDC)

    readiness = StreamingReadiness(
        package_name=pkg_name,
        has_streaming_sources=len(sources) > 0,
        streaming_sources_count=len(sources),
        cdc_sources_count=cdc_count,
    )

    if not sources:
        readiness.readiness_score = 0.0
        readiness.recommendations.append("No streaming sources detected; batch migration recommended.")
        return readiness

    # Generate eventstreams
    for src in sources:
        readiness.eventstreams.append(generate_eventstream(src))

    # Score based on confidence
    avg_confidence = sum(s.confidence for s in sources) / len(sources)
    readiness.readiness_score = avg_confidence * 100

    if cdc_count > 0:
        readiness.recommendations.append(
            f"{cdc_count} CDC source(s) can be converted to push-based Eventstreams."
        )
    if any(s.source_type in (StreamingSourceType.EVENT_HUB, StreamingSourceType.KAFKA) for s in sources):
        readiness.recommendations.append("Event streaming sources detected; Fabric Eventstream recommended.")

    return readiness


def write_streaming_report(assessments: list[StreamingReadiness], output_path: Path) -> Path:
    """Write streaming readiness assessment report."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "total_packages": len(assessments),
        "packages_with_streaming": sum(1 for a in assessments if a.has_streaming_sources),
        "total_eventstreams": sum(len(a.eventstreams) for a in assessments),
        "assessments": [a.to_dict() for a in assessments],
    }
    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    logger.info("streaming_report_written", path=str(output_path))
    return output_path
