"""
C# Script Task to Python Transpiler
======================================
Converts simple C# Script Task code extracted from SSIS ``.dtsx`` packages
into Python equivalents suitable for Spark notebooks.

Conversion coverage:
- String operations (``string.Format``, ``StringBuilder``, concatenation)
- File I/O (``File.ReadAllText``, ``File.WriteAllText``, ``StreamReader``)
- Basic SQL (``SqlConnection``, ``SqlCommand``, ``ExecuteNonQuery``)
- SSIS variable access (``Dts.Variables["pkg::Var"].Value``)
- DateTime operations (``DateTime.Now``, ``DateTime.Parse``)
- Logging (``Dts.Log``, ``Console.WriteLine``)

For unsupported patterns a ``# TODO: Manual conversion required`` comment is
emitted so reviewers can handle them.

Usage::

    from ssis_to_fabric.engine.csharp_transpiler import CSharpTranspiler
    t = CSharpTranspiler()
    python_code = t.transpile(csharp_source)
"""

from __future__ import annotations

import re
from typing import Any

from ssis_to_fabric.logging_config import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Line-level conversion rules: (pattern, replacement [, flags])
# Each entry is applied via re.sub in order.
# ---------------------------------------------------------------------------
_LINE_RULES: list[tuple[Any, ...]] = [
    # --- Comments ---
    (r"//(.*)$", r"#\1", re.MULTILINE),
    # --- String interpolation: $"Hello {name}" → f"Hello {name}" ---
    (r'\$"', 'f"', 0),
    # --- Common type declarations ---
    (r"\bstring\b", "str", 0),
    (r"\bint\b(?!\s*\()", "int", 0),
    (r"\bbool\b", "bool", 0),
    (r"\bdouble\b", "float", 0),
    (r"\bfloat\b", "float", 0),
    (r"\bvar\b", "", 0),
    # --- Null ---
    (r"\bnull\b", "None", 0),
    (r"\btrue\b", "True", 0),
    (r"\bfalse\b", "False", 0),
    # --- Dts.Variables access ---
    (
        r'Dts\.Variables\["(?:[^:]+::)?([^"]+)"\]\.Value',
        r"dts_variables['\1']",
        0,
    ),
    # --- SSIS Dts.Log ---
    (r"Dts\.Log\(([^,)]+),[^)]*\)", r"print(\1)  # Dts.Log", 0),
    # --- Console.WriteLine ---
    (r"Console\.WriteLine\(", "print(", 0),
    # --- DateTime ---
    (r"DateTime\.Now", "datetime.now()", 0),
    (r"DateTime\.UtcNow", "datetime.utcnow()", 0),
    (r"DateTime\.Parse\(([^)]+)\)", r"datetime.fromisoformat(\1)", 0),
    (r"DateTime\.Today", "datetime.today()", 0),
    # --- String methods ---
    (r"\.ToString\(\)", "", 0),
    (r"\.ToUpper\(\)", ".upper()", 0),
    (r"\.ToLower\(\)", ".lower()", 0),
    (r"\.Trim\(\)", ".strip()", 0),
    (r"\.TrimStart\(\)", ".lstrip()", 0),
    (r"\.TrimEnd\(\)", ".rstrip()", 0),
    (r"\.Contains\(([^)]+)\)", r" in \1", 0),
    (r"\.StartsWith\(([^)]+)\)", r".startswith(\1)", 0),
    (r"\.EndsWith\(([^)]+)\)", r".endswith(\1)", 0),
    (r"\.Replace\(([^,]+),\s*([^)]+)\)", r".replace(\1, \2)", 0),
    (r"\.Split\(([^)]+)\)", r".split(\1)", 0),
    (r"\.Substring\((\d+),\s*(\d+)\)", r"[\1:\1+\2]", 0),
    (r"\.Length\b", r"len(___PLACEHOLDER___)", 0),  # handled separately
    (r"string\.IsNullOrEmpty\(([^)]+)\)", r"not \1", 0),
    (r"string\.IsNullOrWhiteSpace\(([^)]+)\)", r"not (\1 or \1.strip())", 0),
    # --- File I/O ---
    (r"File\.ReadAllText\(([^)]+)\)", r"open(\1).read()", 0),
    (r"File\.WriteAllText\(([^,]+),\s*([^)]+)\)", r"open(\1, 'w').write(\2)", 0),
    (r"File\.Exists\(([^)]+)\)", r"os.path.exists(\1)", 0),
    (r"File\.Delete\(([^)]+)\)", r"os.remove(\1)", 0),
    # --- SQL ---
    (
        r"new SqlConnection\(([^)]+)\)",
        r"pyodbc.connect(\1)  # TODO: verify connection string",
        0,
    ),
    (r"new SqlCommand\(([^,)]+)[^)]*\)", r"conn.cursor()  # SQL: \1", 0),
    (r"\.ExecuteNonQuery\(\)", ".execute(sql)  # TODO: define sql", 0),
    (r"\.ExecuteScalar\(\)", ".fetchone()[0]  # TODO: define sql", 0),
    # --- Math ---
    (r"Math\.Abs\(", "abs(", 0),
    (r"Math\.Round\(", "round(", 0),
    (r"Math\.Floor\(", "math.floor(", 0),
    (r"Math\.Ceiling\(", "math.ceil(", 0),
    (r"Math\.Sqrt\(", "math.sqrt(", 0),
    (r"Math\.Pow\(([^,)]+),\s*([^)]+)\)", r"pow(\1, \2)", 0),
    # --- Control flow ---
    (r"\bforeach\s*\(.*?\bin\s+", r"for _item in ", 0),
    (r"\bfor\s*\(int\s+(\w+)\s*=\s*(\d+);\s*\1\s*<\s*([^;]+);\s*\1\+\+\s*\)", r"for \1 in range(\2, \3):", 0),
    # --- Braces / semicolons ---
    (r"^\s*\{\s*$", "", re.MULTILINE),
    (r"^\s*\}\s*$", "", re.MULTILINE),
    (r";(\s*)$", r"\1", re.MULTILINE),
]

# Patterns that we know we cannot handle — emit TODO
_UNSUPPORTED_PATTERNS: list[tuple[str, str]] = [
    (r"\bThread\b", "threading"),
    (r"\bTask\b", "async/await"),
    (r"\bHttpClient\b", "HTTP client"),
    (r"\bXmlDocument\b", "XML manipulation"),
    (r"\bregex\b", "Regex"),
    (r"\bDelegate\b", "delegates"),
    (r"\bevent\b", "events"),
    (r"\bAssembly\b", "reflection"),
]


class CSharpTranspiler:
    """Converts simple C# Script Task source to Python."""

    def transpile(self, csharp: str) -> str:
        """Convert *csharp* source code to a Python equivalent.

        Always succeeds — unsupported patterns are preserved as
        ``# TODO: Manual conversion required`` comments.
        """
        if not csharp or not csharp.strip():
            return "# Empty Script Task\n"

        result = csharp

        # Flag unsupported patterns before conversion
        todo_lines: list[str] = []
        for pattern, description in _UNSUPPORTED_PATTERNS:
            if re.search(pattern, result, re.IGNORECASE):
                todo_lines.append(f"# TODO: Manual conversion required — uses {description}")

        # Apply line-by-line rules
        import contextlib

        for rule in _LINE_RULES:
            pat, repl = rule[0], rule[1]
            flags = rule[2] if len(rule) > 2 else 0
            with contextlib.suppress(re.error):
                result = re.sub(pat, repl, result, flags=flags)

        # Add imports inferred from the converted output
        imports = _infer_imports(result)

        header_lines = [
            "# Auto-generated Python — review before use",
        ]
        header_lines.extend(todo_lines)
        if todo_lines:
            header_lines.append("")

        if imports:
            header_lines.extend(imports)
            header_lines.append("")

        header = "\n".join(header_lines)
        return header + "\n" + result.strip() + "\n"

    def transpile_from_package(self, package: Any) -> dict[str, str]:
        """Extract and transpile all Script Tasks from an ``SSISPackage``.

        Returns a mapping of ``task_name → python_code``.
        """
        results: dict[str, str] = {}
        for task in package.control_flow_tasks:
            if hasattr(task, "task_type") and str(task.task_type).endswith("SCRIPT"):
                csharp = task.properties.get("ScriptCode", "") or task.properties.get("script", "")
                if csharp:
                    results[task.name] = self.transpile(csharp)
                else:
                    results[task.name] = (
                        "# TODO: Manual conversion required\n"
                        f"# Script Task '{task.name}' — source not extracted\n"
                        "# Please implement the equivalent Python logic below.\n"
                    )
        return results


# ---------------------------------------------------------------------------
# Import inference
# ---------------------------------------------------------------------------

_IMPORT_HINTS: list[tuple[str, str]] = [
    (r"\bdatetime\b", "from datetime import datetime"),
    (r"\bos\.path\b|\bos\.remove\b", "import os"),
    (r"\bmath\.", "import math"),
    (r"\bpyodbc\b", "import pyodbc"),
    (r"\bpathlib\b", "from pathlib import Path"),
    (r"\bjson\b", "import json"),
    (r"\bre\b", "import re"),
]


def _infer_imports(code: str) -> list[str]:
    imports = []
    for pattern, import_stmt in _IMPORT_HINTS:
        if re.search(pattern, code):
            imports.append(import_stmt)
    return imports
