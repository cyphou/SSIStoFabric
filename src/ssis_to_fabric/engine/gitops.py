"""
GitOps & Artifact Versioning
==============================
Phase 18: Auto-commit generated artifacts, branch-per-migration,
artifact diff, change tracking, and .fabricignore support.
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from ssis_to_fabric.logging_config import get_logger

if TYPE_CHECKING:
    from pathlib import Path

logger = get_logger(__name__)


# =====================================================================
# Change Tracking
# =====================================================================


@dataclass
class ChangeRecord:
    """Metadata for a tracked migration change."""

    artifact_path: str
    artifact_type: str  # pipeline, notebook, connection
    action: str  # created, modified, deleted
    migrated_by: str = "ssis2fabric"
    source_package: str = ""
    ssis_version: str = ""
    timestamp: str = ""

    def __post_init__(self) -> None:
        if not self.timestamp:
            self.timestamp = datetime.now(tz=timezone.utc).isoformat()

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_path": self.artifact_path,
            "artifact_type": self.artifact_type,
            "action": self.action,
            "migrated_by": self.migrated_by,
            "source_package": self.source_package,
            "ssis_version": self.ssis_version,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ChangeRecord:
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class ChangeLog:
    """A collection of change records for a migration run."""

    records: list[ChangeRecord] = field(default_factory=list)
    run_id: str = ""

    def add(self, record: ChangeRecord) -> None:
        self.records.append(record)

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "total_changes": len(self.records),
            "records": [r.to_dict() for r in self.records],
        }

    def save(self, path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.to_dict(), indent=2), encoding="utf-8")
        return path

    @classmethod
    def load(cls, path: Path) -> ChangeLog:
        data = json.loads(path.read_text(encoding="utf-8"))
        cl = cls(run_id=data.get("run_id", ""))
        cl.records = [ChangeRecord.from_dict(r) for r in data.get("records", [])]
        return cl


# =====================================================================
# .fabricignore
# =====================================================================


def parse_fabricignore(base_dir: Path) -> list[str]:
    """Parse .fabricignore file and return list of ignore patterns."""
    ignore_file = base_dir / ".fabricignore"
    if not ignore_file.exists():
        return []
    patterns = []
    for line in ignore_file.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            patterns.append(stripped)
    return patterns


def should_ignore(path: str, patterns: list[str]) -> bool:
    """Check if a path matches any fabricignore pattern."""
    from fnmatch import fnmatch

    normalized = path.replace("\\", "/")
    return any(fnmatch(normalized, pattern) or fnmatch(normalized.split("/")[-1], pattern) for pattern in patterns)


def generate_default_fabricignore() -> str:
    """Return a default .fabricignore template."""
    return """\
# .fabricignore — Selective artifact versioning for SSIS to Fabric
# Patterns follow .gitignore syntax

# Ignore temporary files
*.tmp
*.bak
*.log

# Ignore local config
migration_config.yaml
.env

# Ignore build artifacts
__pycache__/
*.pyc

# Ignore large binary files
*.parquet
*.avro
"""


# =====================================================================
# Artifact Diff
# =====================================================================


@dataclass
class ArtifactDiff:
    """Comparison result between two versions of an artifact."""

    artifact_path: str
    diff_type: str  # added, removed, modified, unchanged
    old_hash: str = ""
    new_hash: str = ""
    changes_summary: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_path": self.artifact_path,
            "diff_type": self.diff_type,
            "old_hash": self.old_hash,
            "new_hash": self.new_hash,
            "changes_summary": self.changes_summary,
        }


def diff_artifacts(old_dir: Path, new_dir: Path) -> list[ArtifactDiff]:
    """Compare two artifact directories and return diffs."""
    import hashlib

    def hash_file(p: Path) -> str:
        return hashlib.sha256(p.read_bytes()).hexdigest()[:16]

    diffs: list[ArtifactDiff] = []
    old_files = {f.relative_to(old_dir).as_posix(): f for f in old_dir.rglob("*") if f.is_file()}
    new_files = {f.relative_to(new_dir).as_posix(): f for f in new_dir.rglob("*") if f.is_file()}

    for rel, f in new_files.items():
        if rel not in old_files:
            diffs.append(ArtifactDiff(artifact_path=rel, diff_type="added", new_hash=hash_file(f)))
        else:
            oh = hash_file(old_files[rel])
            nh = hash_file(f)
            if oh != nh:
                diffs.append(ArtifactDiff(
                    artifact_path=rel, diff_type="modified",
                    old_hash=oh, new_hash=nh,
                    changes_summary="Content changed",
                ))

    for rel in old_files:
        if rel not in new_files:
            diffs.append(ArtifactDiff(
                artifact_path=rel, diff_type="removed",
                old_hash=hash_file(old_files[rel]),
            ))

    return diffs


# =====================================================================
# Git Operations (safe wrappers)
# =====================================================================


@dataclass
class GitResult:
    """Result of a Git operation."""

    success: bool
    command: str
    output: str = ""
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "command": self.command,
            "output": self.output,
            "error": self.error,
        }


def _run_git(args: list[str], cwd: Path) -> GitResult:
    """Run a git command safely."""
    cmd = ["git", *args]
    cmd_str = " ".join(cmd)
    try:
        result = subprocess.run(  # noqa: S603
            cmd, cwd=str(cwd), capture_output=True, text=True, timeout=60,
        )
        return GitResult(
            success=result.returncode == 0,
            command=cmd_str,
            output=result.stdout.strip(),
            error=result.stderr.strip(),
        )
    except (subprocess.TimeoutExpired, FileNotFoundError) as e:
        return GitResult(success=False, command=cmd_str, error=str(e))


def git_init(repo_dir: Path) -> GitResult:
    """Initialize a git repository."""
    return _run_git(["init"], repo_dir)


def git_create_branch(repo_dir: Path, branch_name: str) -> GitResult:
    """Create and checkout a new branch."""
    return _run_git(["checkout", "-b", branch_name], repo_dir)


def git_add_and_commit(repo_dir: Path, message: str, paths: list[str] | None = None) -> GitResult:
    """Stage files and commit."""
    add_paths = paths or ["."]
    add_result = _run_git(["add", *add_paths], repo_dir)
    if not add_result.success:
        return add_result
    return _run_git(["commit", "-m", message], repo_dir)


def git_diff_stat(repo_dir: Path) -> GitResult:
    """Get git diff stat."""
    return _run_git(["diff", "--stat"], repo_dir)


def sync_artifacts_to_git(
    artifact_dir: Path,
    repo_dir: Path,
    branch_name: str = "",
    commit_message: str = "",
) -> dict[str, Any]:
    """Sync migration artifacts to a git repository.

    Returns a summary dict of the sync operation.
    """
    import shutil

    summary: dict[str, Any] = {"branch": branch_name, "artifacts_copied": 0, "committed": False}

    if not branch_name:
        branch_name = f"migration-{datetime.now(tz=timezone.utc).strftime('%Y%m%d-%H%M%S')}"
    summary["branch"] = branch_name

    # Ensure repo exists
    if not (repo_dir / ".git").exists():
        init_result = git_init(repo_dir)
        if not init_result.success:
            summary["error"] = init_result.error
            return summary

    # Create branch
    branch_result = git_create_branch(repo_dir, branch_name)
    summary["branch_created"] = branch_result.success

    # Copy artifacts (respecting .fabricignore)
    ignore_patterns = parse_fabricignore(artifact_dir)
    copied = 0
    if artifact_dir.exists():
        for f in artifact_dir.rglob("*"):
            if f.is_file():
                rel = f.relative_to(artifact_dir).as_posix()
                if not should_ignore(rel, ignore_patterns):
                    dest = repo_dir / rel
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(str(f), str(dest))
                    copied += 1
    summary["artifacts_copied"] = copied

    # Commit
    if not commit_message:
        commit_message = f"ssis2fabric: migration artifacts ({copied} files)"
    commit_result = git_add_and_commit(repo_dir, commit_message)
    summary["committed"] = commit_result.success
    if not commit_result.success:
        summary["commit_error"] = commit_result.error

    logger.info("git_sync_complete", **summary)
    return summary
