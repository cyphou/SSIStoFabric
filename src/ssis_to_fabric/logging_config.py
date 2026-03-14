"""
Structured logging setup using structlog.
Provides consistent, production-ready JSON or console logging.
Includes correlation ID support for tracing operations across agents.
"""

from __future__ import annotations

import json
import logging
import sys
import uuid
from contextvars import ContextVar
from datetime import datetime, timezone
from pathlib import Path  # noqa: TC003 — used at runtime
from typing import Any

import structlog

# ---------------------------------------------------------------------------
# Correlation ID — thread- and async-safe via contextvars
# ---------------------------------------------------------------------------

_correlation_id_var: ContextVar[str] = ContextVar("correlation_id", default="")


def bind_correlation_id(correlation_id: str | None = None) -> str:
    """Bind a correlation ID to the current context (thread-safe).

    If *correlation_id* is ``None``, a new UUID-based ID is generated.
    Returns the bound ID.
    """
    cid = correlation_id or f"ssis2fabric-{uuid.uuid4().hex[:12]}"
    _correlation_id_var.set(cid)
    structlog.contextvars.bind_contextvars(correlation_id=cid)
    return cid


def get_correlation_id() -> str:
    """Return the current correlation ID (empty string if none bound)."""
    return _correlation_id_var.get()


def clear_correlation_id() -> None:
    """Remove the correlation ID from the current context."""
    _correlation_id_var.set("")
    structlog.contextvars.unbind_contextvars("correlation_id")


# ---------------------------------------------------------------------------
# Audit logging — append-only JSON-lines file
# ---------------------------------------------------------------------------

_audit_log_path: Path | None = None


def configure_audit_log(path: Path | None) -> None:
    """Set the audit log file path. ``None`` disables audit logging."""
    global _audit_log_path  # noqa: PLW0603
    _audit_log_path = path


def write_audit_entry(
    action: str,
    *,
    detail: dict[str, Any] | None = None,
) -> None:
    """Append a JSON-lines entry to the audit log (if configured)."""
    if _audit_log_path is None:
        return
    entry: dict[str, Any] = {
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "correlation_id": get_correlation_id(),
        "action": action,
    }
    if detail:
        entry["detail"] = detail
    _audit_log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(_audit_log_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, default=str) + "\n")


def setup_logging(level: str = "INFO", log_format: str = "json", log_file: str | None = None) -> None:
    """
    Configure structured logging for the migration tool.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR)
        log_format: 'json' for production, 'console' for development
        log_file: Optional file path for log output
    """
    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]

    renderer: Any
    if log_format == "json":
        renderer = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=sys.stdout.isatty())

    structlog.configure(
        processors=[
            *shared_processors,  # type: ignore[list-item]
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    handlers: list[logging.Handler] = [handler]

    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    for h in handlers:
        root_logger.addHandler(h)
    root_logger.setLevel(getattr(logging, level.upper()))


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Get a structured logger instance."""
    return structlog.get_logger(name)  # type: ignore[no-any-return]
