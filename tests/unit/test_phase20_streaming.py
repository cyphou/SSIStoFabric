"""Tests for Phase 20 — Streaming & Real-Time."""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import TYPE_CHECKING

from click.testing import CliRunner

from ssis_to_fabric.cli import main
from ssis_to_fabric.engine.streaming import (
    StreamingReadiness,
    StreamingSource,
    StreamingSourceType,
    assess_streaming_readiness,
    detect_streaming_sources,
    generate_eventstream,
    write_streaming_report,
)

if TYPE_CHECKING:
    from pathlib import Path


def _make_package(name="test_pkg", connections=None, tasks=None):
    return SimpleNamespace(
        name=name,
        connection_managers=connections or [],
        control_flow_tasks=tasks or [],
    )


def _make_conn(name="conn1", connection_string="", connection_type=""):
    return SimpleNamespace(name=name, connection_string=connection_string, connection_type=connection_type)


def _make_task(name="task1", task_type="ExecuteSQLTask"):
    return SimpleNamespace(name=name, task_type=task_type)


class TestStreamingDetection:
    def test_detect_cdc(self):
        pkg = _make_package(tasks=[_make_task("CDC_Load", "CDCSourceTask")])
        sources = detect_streaming_sources(pkg)
        assert len(sources) == 1
        assert sources[0].source_type == StreamingSourceType.CDC

    def test_detect_event_hub(self):
        pkg = _make_package(connections=[_make_conn("eh", "Endpoint=sb://test.eventhub.net")])
        sources = detect_streaming_sources(pkg)
        assert len(sources) == 1
        assert sources[0].source_type == StreamingSourceType.EVENT_HUB

    def test_detect_kafka(self):
        pkg = _make_package(connections=[_make_conn("k", "bootstrap.servers=kafka:9092")])
        sources = detect_streaming_sources(pkg)
        assert len(sources) == 1
        assert sources[0].source_type == StreamingSourceType.KAFKA

    def test_detect_service_bus(self):
        pkg = _make_package(connections=[_make_conn("sb", "Endpoint=sb://test.servicebus.windows.net")])
        sources = detect_streaming_sources(pkg)
        assert any(s.source_type == StreamingSourceType.SERVICE_BUS for s in sources)

    def test_no_streaming(self):
        pkg = _make_package(connections=[_make_conn("sql", "Server=db;Database=test")])
        sources = detect_streaming_sources(pkg)
        assert len(sources) == 0


class TestEventstreamGeneration:
    def test_event_hub_eventstream(self):
        src = StreamingSource(name="myeh", source_type=StreamingSourceType.EVENT_HUB)
        es = generate_eventstream(src)
        assert es.source_type == "event_hub"
        assert len(es.destinations) > 0

    def test_cdc_eventstream(self):
        src = StreamingSource(name="cdc_src", source_type=StreamingSourceType.CDC)
        es = generate_eventstream(src)
        d = es.to_dict()
        assert d["type"] == "Eventstream"
        assert "CDC" in str(d["properties"]["source"]["config"].get("type", ""))

    def test_kafka_eventstream(self):
        src = StreamingSource(name="k", source_type=StreamingSourceType.KAFKA)
        es = generate_eventstream(src)
        assert "bootstrapServers" in es.source_config


class TestStreamingReadiness:
    def test_no_streaming_score_zero(self):
        pkg = _make_package()
        r = assess_streaming_readiness(pkg)
        assert r.readiness_score == 0.0
        assert not r.has_streaming_sources

    def test_cdc_readiness(self):
        pkg = _make_package(tasks=[_make_task("CDC_Task", "CDCSourceTask")])
        r = assess_streaming_readiness(pkg)
        assert r.has_streaming_sources
        assert r.cdc_sources_count == 1
        assert r.readiness_score > 0

    def test_recommendations(self):
        pkg = _make_package(connections=[_make_conn("eh", "eventhub connection")])
        r = assess_streaming_readiness(pkg)
        assert len(r.recommendations) > 0


class TestStreamingReport:
    def test_write_report(self, tmp_path: Path):
        assessments = [StreamingReadiness(package_name="p1", has_streaming_sources=True)]
        path = write_streaming_report(assessments, tmp_path / "report.json")
        assert path.exists()
        data = json.loads(path.read_text(encoding="utf-8"))
        assert data["packages_with_streaming"] == 1


class TestCLI:
    def test_streaming_assess_registered(self):
        runner = CliRunner()
        result = runner.invoke(main, ["streaming-assess", "--help"])
        assert result.exit_code == 0
