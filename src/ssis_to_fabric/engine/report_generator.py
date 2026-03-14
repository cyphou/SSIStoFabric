"""
Migration Report Generator
===========================
Reads ``migration_report.json`` produced by the migration engine and
generates a self-contained HTML dashboard showing:

- Overall migration summary (total items, status breakdown, complexity)
- Per-package breakdown with task lists and complexity scores
- Expression-transpilation statistics (TODO counts)
- Structured error / warning list
- Effort estimation per package

Usage (Python API)::

    from ssis_to_fabric.engine.report_generator import ReportGenerator
    gen = ReportGenerator()
    html_path = gen.generate(output_dir=Path("output"))

CLI (added via ``ssis2fabric report``)::

    ssis2fabric report output/
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from ssis_to_fabric.logging_config import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# HTML template (self-contained — no external CDN dependencies)
# ---------------------------------------------------------------------------
_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SSIStoFabric Migration Report — {project_name}</title>
<style>
  :root{{--blue:#2563eb;--green:#16a34a;--yellow:#ca8a04;--red:#dc2626;--grey:#6b7280;}}
  *{{box-sizing:border-box;margin:0;padding:0;}}
  body{{font-family:system-ui,sans-serif;background:#f1f5f9;color:#1e293b;font-size:14px;}}
  header{{background:var(--blue);color:#fff;padding:1.5rem 2rem;}}
  header h1{{font-size:1.5rem;}}
  header p{{opacity:.8;margin-top:.25rem;font-size:.875rem;}}
  main{{max-width:1200px;margin:2rem auto;padding:0 1rem;}}
  .cards{{display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:1rem;margin-bottom:2rem;}}
  .card{{background:#fff;border-radius:.5rem;padding:1rem;box-shadow:0 1px 3px rgba(0,0,0,.1);text-align:center;}}
  .card .num{{font-size:2rem;font-weight:700;line-height:1;}}
  .card .lbl{{font-size:.75rem;color:var(--grey);margin-top:.25rem;text-transform:uppercase;letter-spacing:.05em;}}
  .card.green .num{{color:var(--green);}}
  .card.yellow .num{{color:var(--yellow);}}
  .card.red .num{{color:var(--red);}}
  .card.blue .num{{color:var(--blue);}}
  section{{background:#fff;border-radius:.5rem;padding:1.5rem;
    box-shadow:0 1px 3px rgba(0,0,0,.1);margin-bottom:1.5rem;}}
  section h2{{font-size:1rem;font-weight:600;margin-bottom:1rem;border-bottom:1px solid #e2e8f0;padding-bottom:.5rem;}}
  table{{width:100%;border-collapse:collapse;font-size:.8125rem;}}
  th{{text-align:left;padding:.5rem .75rem;background:#f8fafc;font-weight:600;border-bottom:2px solid #e2e8f0;}}
  td{{padding:.5rem .75rem;border-bottom:1px solid #f1f5f9;vertical-align:top;}}
  tr:hover td{{background:#f8fafc;}}
  .badge{{display:inline-block;padding:.125rem .5rem;border-radius:9999px;font-size:.6875rem;font-weight:600;}}
  .badge-green{{background:#dcfce7;color:#166534;}}
  .badge-yellow{{background:#fef9c3;color:#854d0e;}}
  .badge-red{{background:#fee2e2;color:#991b1b;}}
  .badge-blue{{background:#dbeafe;color:#1e40af;}}
  .badge-grey{{background:#f1f5f9;color:#475569;}}
  .badge-orange{{background:#ffedd5;color:#9a3412;}}
  .error-warning{{color:var(--yellow);}}
  .error-error{{color:var(--red);}}
  .error-info{{color:var(--blue);}}
  footer{{text-align:center;color:var(--grey);font-size:.75rem;padding:2rem;}}
</style>
</head>
<body>
<header>
  <h1>📊 SSIStoFabric Migration Report</h1>
  <p>Project: <strong>{project_name}</strong> &nbsp;·&nbsp; Strategy: {strategy}
     &nbsp;·&nbsp; Generated: {generated_at}</p>
</header>
<main>

<!-- Summary cards -->
<div class="cards">
  <div class="card blue"><div class="num">{total}</div><div class="lbl">Total Items</div></div>
  <div class="card green"><div class="num">{completed}</div><div class="lbl">Completed</div></div>
  <div class="card red"><div class="num">{errors}</div><div class="lbl">Errors</div></div>
  <div class="card yellow"><div class="num">{manual}</div><div class="lbl">Manual Review</div></div>
  <div class="card blue"><div class="num">{df_pipelines}</div><div class="lbl">Pipelines</div></div>
  <div class="card blue"><div class="num">{spark_notebooks}</div><div class="lbl">Notebooks</div></div>
  <div class="card blue"><div class="num">{dataflow_gen2}</div><div class="lbl">Dataflows</div></div>
  <div class="card yellow"><div class="num">{todo_count}</div><div class="lbl">TODO Items</div></div>
</div>

<!-- Complexity breakdown -->
<section>
  <h2>Complexity Breakdown</h2>
  <table>
    <tr><th>Complexity</th><th>Count</th><th>% of Total</th></tr>
    {complexity_rows}
  </table>
</section>

<!-- Per-package breakdown -->
<section>
  <h2>Package Migration Items</h2>
  <table>
    <tr>
      <th>Package</th>
      <th>Task</th>
      <th>Type</th>
      <th>Target</th>
      <th>Complexity</th>
      <th>Status</th>
      <th>Notes</th>
    </tr>
    {item_rows}
  </table>
</section>

<!-- Errors & warnings -->
{errors_section}

</main>
<footer>
  Generated by SSIStoFabric &nbsp;·&nbsp; {generated_at}
</footer>
</body>
</html>
"""

_COMPLEXITY_BADGE = {
    "LOW": "badge-green",
    "MEDIUM": "badge-yellow",
    "HIGH": "badge-orange",
    "MANUAL": "badge-red",
}
_STATUS_BADGE = {
    "completed": "badge-green",
    "error": "badge-red",
    "manual_review_required": "badge-yellow",
    "pending": "badge-grey",
    "pending_pipeline": "badge-grey",
    "skipped": "badge-grey",
}
_SEVERITY_CLASS = {
    "error": "error-error",
    "warning": "error-warning",
    "info": "error-info",
}


class ReportGenerator:
    """Generates a self-contained HTML dashboard from ``migration_report.json``."""

    def generate(self, output_dir: Path | str) -> Path:
        """Read ``migration_report.json`` from *output_dir* and write ``migration_report.html``.

        Returns the path to the generated HTML file.
        """
        output_dir = Path(output_dir)
        report_path = output_dir / "migration_report.json"

        if not report_path.exists():
            raise FileNotFoundError(f"migration_report.json not found in {output_dir}")

        with open(report_path, encoding="utf-8") as f:
            report: dict[str, Any] = json.load(f)

        html = self._render(report)
        html_path = output_dir / "migration_report.html"
        html_path.write_text(html, encoding="utf-8")
        logger.info("html_report_generated", path=str(html_path))
        return html_path

    # ------------------------------------------------------------------
    # Rendering helpers
    # ------------------------------------------------------------------

    def _render(self, report: dict[str, Any]) -> str:
        items: list[dict[str, Any]] = report.get("items", [])
        errors: list[dict[str, Any]] = report.get("errors", [])
        summary: dict[str, Any] = report.get("summary", {})
        complexity_counts: dict[str, int] = summary.get("complexity_breakdown", {})
        total = summary.get("total", len(items))

        status_counts: dict[str, int] = {}
        todo_count = 0
        for item in items:
            status_counts[item.get("status", "unknown")] = status_counts.get(item.get("status", "unknown"), 0) + 1
            for note in item.get("notes", []):
                if "TODO" in note.upper():
                    todo_count += 1

        completed = status_counts.get("completed", 0)
        error_count = status_counts.get("error", 0)
        manual = status_counts.get("manual_review_required", 0)

        # Complexity rows
        complexity_rows = ""
        for cplx, count in sorted(complexity_counts.items()):
            pct = f"{100 * count / total:.1f}" if total else "0.0"
            badge_cls = _COMPLEXITY_BADGE.get(cplx, "badge-grey")
            complexity_rows += (
                f"<tr><td><span class='badge {badge_cls}'>{cplx}</span></td><td>{count}</td><td>{pct}%</td></tr>\n"
            )
        if not complexity_rows:
            complexity_rows = "<tr><td colspan='3'>No items</td></tr>"

        # Item rows
        item_rows = ""
        for item in items:
            status = item.get("status", "")
            badge_cls = _STATUS_BADGE.get(status, "badge-grey")
            cplx = item.get("complexity", "")
            cplx_cls = _COMPLEXITY_BADGE.get(cplx, "badge-grey")
            notes_html = "<br>".join(item.get("notes", []))
            item_rows += (
                f"<tr>"
                f"<td>{_esc(item.get('source_package', ''))}</td>"
                f"<td>{_esc(item.get('source_task', ''))}</td>"
                f"<td>{_esc(item.get('task_type', ''))}</td>"
                f"<td>{_esc(item.get('target_artifact', ''))}</td>"
                f"<td><span class='badge {cplx_cls}'>{_esc(cplx)}</span></td>"
                f"<td><span class='badge {badge_cls}'>{_esc(status)}</span></td>"
                f"<td>{notes_html}</td>"
                f"</tr>\n"
            )
        if not item_rows:
            item_rows = "<tr><td colspan='7'>No items</td></tr>"

        # Errors section
        if errors:
            rows = ""
            for err in errors:
                sev = err.get("severity", "info")
                sev_cls = _SEVERITY_CLASS.get(sev, "")
                rows += (
                    f"<tr>"
                    f"<td class='{sev_cls}'><strong>{_esc(sev.upper())}</strong></td>"
                    f"<td>{_esc(err.get('source', ''))}</td>"
                    f"<td>{_esc(err.get('message', ''))}</td>"
                    f"<td>{_esc(err.get('suggested_fix', ''))}</td>"
                    f"</tr>\n"
                )
            errors_section = (
                "<section><h2>Errors &amp; Warnings</h2>"
                "<table><tr><th>Severity</th><th>Source</th><th>Message</th><th>Suggested Fix</th></tr>"
                f"{rows}</table></section>"
            )
        else:
            errors_section = ""

        return _HTML_TEMPLATE.format(
            project_name=_esc(report.get("project_name", "Unknown")),
            strategy=_esc(report.get("strategy", "")),
            generated_at=datetime.now().strftime("%Y-%m-%d %H:%M UTC"),
            total=total,
            completed=completed,
            errors=error_count,
            manual=manual,
            df_pipelines=summary.get("data_factory_pipeline", 0),
            spark_notebooks=summary.get("spark_notebook", 0),
            dataflow_gen2=summary.get("dataflow_gen2", 0),
            todo_count=todo_count,
            complexity_rows=complexity_rows,
            item_rows=item_rows,
            errors_section=errors_section,
        )


def _esc(s: str) -> str:
    """HTML-escape a string."""
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
