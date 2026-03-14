"""Tests for Phase 19 — Web Dashboard."""

from __future__ import annotations

from typing import TYPE_CHECKING

from click.testing import CliRunner

from ssis_to_fabric.cli import main
from ssis_to_fabric.engine.web_dashboard import (
    API_ROUTES,
    PackageView,
    ProgressStream,
    SSEEvent,
    generate_dashboard_html,
    get_api_spec,
    write_dashboard,
)

if TYPE_CHECKING:
    from pathlib import Path


class TestAPIRoutes:
    def test_routes_defined(self):
        assert len(API_ROUTES) >= 10

    def test_health_check_no_auth(self):
        health = next(r for r in API_ROUTES if r.handler == "health_check")
        assert not health.auth_required

    def test_api_spec(self):
        spec = get_api_spec()
        assert spec["openapi"] == "3.0.3"
        assert "paths" in spec
        assert "/api/v1/health" in spec["paths"]


class TestSSEProgress:
    def test_emit_event(self):
        stream = ProgressStream()
        evt = stream.emit("test", key="value")
        assert evt.event == "test"
        assert evt.id == "1"

    def test_format(self):
        evt = SSEEvent(event="progress", data={"pct": 50}, id="1")
        formatted = evt.format()
        assert "id: 1" in formatted
        assert "event: progress" in formatted
        assert "data:" in formatted

    def test_progress(self):
        stream = ProgressStream()
        evt = stream.progress("pkg1", "parsing", 50.0)
        assert evt.data["package"] == "pkg1"
        assert evt.data["percent"] == 50.0

    def test_complete(self):
        stream = ProgressStream()
        evt = stream.complete({"total": 5})
        assert evt.event == "complete"

    def test_get_stream(self):
        stream = ProgressStream()
        stream.status("starting")
        stream.progress("p", "s", 100.0)
        text = stream.get_stream()
        assert text.count("event:") == 2


class TestPackageView:
    def test_to_dict(self):
        pv = PackageView(name="pkg1", complexity="MEDIUM", task_count=5)
        d = pv.to_dict()
        assert d["name"] == "pkg1"
        assert d["task_count"] == 5


class TestDashboard:
    def test_generate_html(self):
        html = generate_dashboard_html()
        assert "<!DOCTYPE html>" in html
        assert "Migration Dashboard" in html
        assert "api-table" in html

    def test_with_packages(self):
        pkgs = [PackageView(name="p1", complexity="LOW", task_count=2)]
        html = generate_dashboard_html(pkgs)
        assert "p1" in html

    def test_write_dashboard(self, tmp_path: Path):
        path = write_dashboard(tmp_path)
        assert path.exists()
        assert path.suffix == ".html"


class TestCLI:
    def test_dashboard_registered(self):
        runner = CliRunner()
        result = runner.invoke(main, ["dashboard", "--help"])
        assert result.exit_code == 0
