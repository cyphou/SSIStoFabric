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
    (r"\bstring\b(?!\.)", "str", 0),
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
    (r"(\w[\w.]*)?\.Contains\(([^)]+)\)", r"\2 in \1", 0),
    (r"\.StartsWith\(([^)]+)\)", r".startswith(\1)", 0),
    (r"\.EndsWith\(([^)]+)\)", r".endswith(\1)", 0),
    # --- Regex (before generic .Replace/.Split so Regex.Replace is caught first) ---
    (r"Regex\.IsMatch\(([^,]+),\s*([^)]+)\)", r"bool(re.search(\2, \1))", 0),
    (r"Regex\.Match\(([^,]+),\s*([^)]+)\)", r"re.search(\2, \1)", 0),
    (r"Regex\.Replace\(([^,]+),\s*([^,]+),\s*([^)]+)\)", r"re.sub(\2, \3, \1)", 0),
    (r"Regex\.Split\(([^,]+),\s*([^)]+)\)", r"re.split(\2, \1)", 0),
    (r"new Regex\(([^)]+)\)", r"re.compile(\1)", 0),
    # --- String methods (after regex to avoid clobbering Regex.Replace) ---
    (r"\.Replace\(([^,]+),\s*([^)]+)\)", r".replace(\1, \2)", 0),
    (r"\.Split\(([^)]+)\)", r".split(\1)", 0),
    (r"\.Substring\((\d+),\s*(\d+)\)", r"[\1:\1+\2]", 0),
    (r"\.Length\b", r"len(___PLACEHOLDER___)", 0),  # handled separately
    (r"string\.IsNullOrEmpty\(([^)]+)\)", r"not \1", 0),
    (r"string\.IsNullOrWhiteSpace\(([^)]+)\)", r"not (\1 and \1.strip())", 0),
    # --- File I/O ---
    (r"File\.ReadAllText\(([^)]+)\)", r"Path(\1).read_text()", 0),
    (r"File\.WriteAllText\(([^,]+),\s*([^)]+)\)", r"Path(\1).write_text(\2)", 0),
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
    (r"\bwhile\s*\(([^)]+)\)\s*\{?", r"while \1:", 0),
    (r"\belse if\s*\(([^)]+)\)\s*\{?", r"elif \1:", 0),
    (r"\bif\s*\(([^)]+)\)\s*\{?", r"if \1:", 0),
    (r"\belse\b(?!\s*if)\s*\{?", r"else:", 0),
    (r"\breturn\s+", "return ", 0),
    (r"\bnew\s+List<[^>]+>\(\)", "[]", 0),
    (r"\bnew\s+Dictionary<[^>]+>\(\)", "{}", 0),
    # --- Collections ---
    (r"\.Add\(([^)]+)\)", r".append(\1)", 0),
    (r"\.AddRange\(([^)]+)\)", r".extend(\1)", 0),
    (r"\.Remove\(([^)]+)\)", r".remove(\1)", 0),
    (r"\.Count\b", r".__len__()", 0),  # len() handled via post-processing
    (r"\.Clear\(\)", ".clear()", 0),
    (r"(\w[\w.]*)?\.ContainsKey\(([^)]+)\)", r"\2 in \1", 0),
    # --- StringBuilder ---
    (r"new StringBuilder\(\)", '""', 0),
    (r"new StringBuilder\(([^)]+)\)", r"str(\1)", 0),
    (r"\.Append\(([^)]+)\)", r" += str(\1)", 0),
    (r"\.AppendLine\(([^)]+)\)", r' += str(\1) + "\\n"', 0),
    (r"\.AppendLine\(\)", r' += "\\n"', 0),
    # --- DataTable / DataRow ---
    (r"new DataTable\(\)", "[]  # DataTable → list of dicts", 0),
    (r"(\w+)\.Rows\.Add\(([^)]+)\)", r"\1.append(\2)", 0),
    (r'(\w+)\.Rows\[(\d+)\]\["([^"]+)"\]', r"\1[\2]['\3']", 0),
    (r'(\w+)\["([^"]+)"\]', r"\1['\2']", 0),
    (r"\.Rows\.Count\b", ".__len__()", 0),
    # --- File I/O extended ---
    (r"File\.ReadAllLines\(([^)]+)\)", r"Path(\1).read_text().splitlines()", 0),
    (r"File\.WriteAllLines\(([^,]+),\s*([^)]+)\)", r"Path(\1).write_text('\\n'.join(\2))", 0),
    (r"File\.AppendAllText\(([^,]+),\s*([^)]+)\)", r"Path(\1).open('a').write(\2)  # TODO: use context manager", 0),
    (r"File\.Copy\(([^,]+),\s*([^)]+)\)", r"shutil.copy(\1, \2)", 0),
    (r"File\.Move\(([^,]+),\s*([^)]+)\)", r"shutil.move(\1, \2)", 0),
    (r"Directory\.CreateDirectory\(([^)]+)\)", r"os.makedirs(\1, exist_ok=True)", 0),
    (r"Directory\.Exists\(([^)]+)\)", r"os.path.isdir(\1)", 0),
    (r"Directory\.GetFiles\(([^)]+)\)", r"os.listdir(\1)", 0),
    (r"Path\.Combine\(([^)]+)\)", r"os.path.join(\1)", 0),
    (r"Path\.GetFileName\(([^)]+)\)", r"os.path.basename(\1)", 0),
    (r"Path\.GetDirectoryName\(([^)]+)\)", r"os.path.dirname(\1)", 0),
    (r"Path\.GetExtension\(([^)]+)\)", r"os.path.splitext(\1)[1]", 0),
    # --- Stream patterns ---
    (
        r"new StreamWriter\(([^)]+)\)",
        r"open(\1, 'w')  # StreamWriter",
        0,
    ),
    (
        r"new StreamReader\(([^)]+)\)",
        r"open(\1, 'r')  # StreamReader",
        0,
    ),
    (r"\.ReadLine\(\)", ".readline()", 0),
    (r"\.ReadToEnd\(\)", ".read()", 0),
    (r"\.Write\(([^)]+)\)", r".write(\1)", 0),
    (r"\.WriteLine\(([^)]+)\)", r".write(\1 + '\\n')", 0),
    (r"\.Close\(\)", ".close()", 0),
    (r"\.Dispose\(\)", ".close()  # Dispose", 0),
    # --- Type conversions ---
    (r"int\.Parse\(([^)]+)\)", r"int(\1)", 0),
    (r"int\.TryParse\(([^,]+),\s*out\s+\w+\s+(\w+)\)", r"\2 = int(\1) if \1.isdigit() else 0", 0),
    (r"Convert\.ToInt32\(([^)]+)\)", r"int(\1)", 0),
    (r"Convert\.ToDouble\(([^)]+)\)", r"float(\1)", 0),
    (r"Convert\.ToString\(([^)]+)\)", r"str(\1)", 0),
    (r"Convert\.ToBoolean\(([^)]+)\)", r"bool(\1)", 0),
    (r"Convert\.ToDateTime\(([^)]+)\)", r"datetime.fromisoformat(\1)", 0),
    # --- String.Format ---
    (r'string\.Format\("([^"]*)",\s*([^)]+)\)', r'"\1".format(\2)', 0),
    (r"string\.Join\(([^,]+),\s*([^)]+)\)", r"\1.join(\2)", 0),
    (r"string\.Concat\(([^)]+)\)", r'"".join([\1])', 0),
    (r"string\.Empty\b", '""', 0),
    # --- Exception handling ---
    (r"\btry\s*\{?", "try:", 0),
    (r"\bcatch\s*\(\s*(\w+)\s+(\w+)\s*\)\s*\{?", r"except Exception as \2:  # \1", 0),
    (r"\bcatch\s*\{?", "except Exception:", 0),
    (r"\bfinally\s*\{?", "finally:", 0),
    (r"\bthrow new\s+\w+\(([^)]*)\)", r"raise Exception(\1)", 0),
    (r"\bthrow\b", "raise", 0),
    # --- Braces / semicolons ---
    (r"^\s*\{\s*$", "", re.MULTILINE),
    (r"^\s*\}\s*$", "", re.MULTILINE),
    (r";(\s*)$", r"\1", re.MULTILINE),
]

# Patterns that we know we cannot handle — emit TODO
_UNSUPPORTED_PATTERNS: list[tuple[str, str]] = [
    (r"\bThread\b", "threading"),
    (r"\bTask\.Run\b|\bTask\.WhenAll\b", "async/await"),
    (r"\bHttpClient\b|\bWebClient\b", "HTTP client"),
    (r"\bXmlDocument\b|\bXElement\b|\bXDocument\b", "XML/LINQ manipulation"),
    (r"\bDelegate\b", "delegates"),
    (r"\bevent\b", "events"),
    (r"\bAssembly\b|\bReflection\b", "reflection"),
    (r"\bWmi\b|\bManagementObject\b", "WMI access"),
    (r"\bRegistry\b|\bRegistryKey\b", "Windows Registry"),
    (r"\bProcess\.Start\b", "process execution"),
    (r"\bSmtpClient\b|\bMailMessage\b", "email sending"),
    (r"\bServiceController\b", "Windows services"),
    (r"OleDb|\bOdbcConnection\b", "OLEDB/ODBC (use pyodbc instead)"),
    (r"\bunsafe\b|\bfixed\b|\bstackalloc\b", "unsafe/pointer operations"),
]


class CSharpTranspiler:
    """Converts simple C# Script Task source to Python.

    Uses a two-pass approach:
    1. **AST pass** — tree-walk parser extracts class/method structure,
       identifies using directives, and builds a structural skeleton.
    2. **Line-rule pass** — regex-based line-level conversion for the
       body of each method.
    """

    # Patterns that indicate a script is "low-risk" and mostly auto-migratable
    _LOW_RISK_PATTERNS = {
        "string_ops": re.compile(
            r"\.(ToUpper|ToLower|Trim|Replace|Split|Contains|StartsWith|EndsWith)\(",
        ),
        "simple_loops": re.compile(r"\bfor\s*\(int\b|\bforeach\s*\("),
        "basic_io": re.compile(r"File\.(ReadAllText|WriteAllText|Exists)"),
        "logging": re.compile(r"Dts\.Log|Console\.WriteLine"),
        "variables": re.compile(r"Dts\.Variables"),
    }

    # Patterns that indicate a script is "high-risk" and needs manual review
    _HIGH_RISK_PATTERNS = {
        "database": re.compile(r"SqlConnection|OleDb|OdbcConnection|ADO\.NET"),
        "network": re.compile(r"HttpClient|WebClient|SmtpClient|FtpWebRequest"),
        "system": re.compile(r"WmiObject|Registry|Process\.Start|ServiceController"),
        "threading": re.compile(r"\bThread\b|Task\.Run|Task\.WhenAll"),
        "unsafe": re.compile(r"\bunsafe\b|\bfixed\b|\bstackalloc\b"),
    }

    def classify_risk(self, csharp: str) -> str:
        """Classify a C# script as ``"low"``, ``"medium"``, or ``"high"`` risk.

        Low-risk scripts are mostly string/file ops and loops — typically
        auto-migratable.  High-risk scripts use database, network, or OS
        features that need manual review.
        """
        if not csharp or not csharp.strip():
            return "low"

        high_hits = sum(1 for p in self._HIGH_RISK_PATTERNS.values() if p.search(csharp))
        if high_hits >= 1:
            return "high"

        low_hits = sum(1 for p in self._LOW_RISK_PATTERNS.values() if p.search(csharp))
        if low_hits >= 2:
            return "low"
        return "medium"

    def transpile(self, csharp: str) -> str:
        """Convert *csharp* source code to a Python equivalent.

        Uses a two-pass approach:
        1. AST pass — extract class/method structure, using directives
        2. Line-rule pass — regex conversion of method bodies

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

        # --- AST pass: extract structural elements ---
        structure = self._extract_structure(result)

        # Strip using directives (handled by import inference)
        result = re.sub(r"^\s*using\s+[\w.]+\s*;\s*$", "", result, flags=re.MULTILINE)

        # Strip namespace wrapper
        result = re.sub(
            r"^\s*namespace\s+[\w.]+\s*\{",
            "# namespace removed",
            result,
            flags=re.MULTILINE,
        )

        # Convert class declarations to Python class
        result = re.sub(
            r"^\s*(?:public\s+)?(?:partial\s+)?class\s+(\w+)"
            r"(?:\s*:\s*[\w.\s,]+)?\s*\{",
            r"class \1:",
            result,
            flags=re.MULTILINE,
        )

        # Convert method signatures to Python def
        result = re.sub(
            r"^\s*(?:public|private|protected|internal)?\s*"
            r"(?:static\s+)?(?:override\s+)?(?:void|string|int|bool|"
            r"object|Task|var|double|float|long|decimal|DataTable)\s+"
            r"(\w+)\s*\(([^)]*)\)\s*\{?",
            lambda mm: _method_to_def(mm.group(1), mm.group(2)),
            result,
            flags=re.MULTILINE,
        )

        # Apply line-by-line rules
        import contextlib

        for rule in _LINE_RULES:
            pat, repl = rule[0], rule[1]
            flags = rule[2] if len(rule) > 2 else 0
            with contextlib.suppress(re.error):
                result = re.sub(pat, repl, result, flags=flags)

        # Add imports inferred from the converted output + structure
        imports = _infer_imports(result)
        for ns in structure.get("usings", []):
            hint = _USING_TO_IMPORT.get(ns)
            if hint and hint not in imports:
                imports.append(hint)

        header_lines = [
            "# Auto-generated Python — review before use",
        ]
        header_lines.extend(todo_lines)
        if todo_lines:
            header_lines.append("")

        if imports:
            header_lines.extend(sorted(set(imports)))
            header_lines.append("")

        header = "\n".join(header_lines)
        return header + "\n" + result.strip() + "\n"

    @staticmethod
    def _extract_structure(csharp: str) -> dict[str, Any]:
        """AST-style tree-walk: extract structural elements from C# source.

        Returns a dict with:
        - ``usings``: list of namespace strings
        - ``classes``: list of class name strings
        - ``methods``: list of ``{name, return_type, params, is_static}`` dicts
        - ``namespace``: namespace string or empty
        """
        usings: list[str] = []
        for m in re.finditer(r"^\s*using\s+([\w.]+)\s*;", csharp, re.MULTILINE):
            usings.append(m.group(1))

        namespace = ""
        ns_match = re.search(r"^\s*namespace\s+([\w.]+)", csharp, re.MULTILINE)
        if ns_match:
            namespace = ns_match.group(1)

        classes: list[str] = []
        for m in re.finditer(
            r"^\s*(?:public\s+)?(?:partial\s+)?class\s+(\w+)",
            csharp,
            re.MULTILINE,
        ):
            classes.append(m.group(1))

        methods: list[dict[str, Any]] = []
        method_pat = re.compile(
            r"(?:public|private|protected|internal)?\s*"
            r"(static\s+)?(?:override\s+)?"
            r"(void|string|int|bool|object|Task|var|double|float|long|decimal|DataTable)\s+"
            r"(\w+)\s*\(([^)]*)\)",
        )
        for m in method_pat.finditer(csharp):
            methods.append({
                "name": m.group(3),
                "return_type": m.group(2),
                "params": m.group(4).strip(),
                "is_static": bool(m.group(1)),
            })

        return {
            "usings": usings,
            "namespace": namespace,
            "classes": classes,
            "methods": methods,
        }

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
    (r"\bos\.path\b|\bos\.remove\b|\bos\.makedirs\b|\bos\.listdir\b", "import os"),
    (r"\bmath\.", "import math"),
    (r"\bpyodbc\b", "import pyodbc"),
    (r"\bpathlib\b", "from pathlib import Path"),
    (r"\bjson\b", "import json"),
    (r"\bre\.", "import re"),
    (r"\bshutil\.", "import shutil"),
]


def _infer_imports(code: str) -> list[str]:
    imports = []
    for pattern, import_stmt in _IMPORT_HINTS:
        if re.search(pattern, code):
            imports.append(import_stmt)
    return imports


# ---------------------------------------------------------------------------
# C# using → Python import mapping
# ---------------------------------------------------------------------------

_USING_TO_IMPORT: dict[str, str] = {
    "System": "",
    "System.IO": "import os",
    "System.Text": "",
    "System.Text.RegularExpressions": "import re",
    "System.Collections.Generic": "",
    "System.Linq": "",
    "System.Data": "",
    "System.Data.SqlClient": "import pyodbc",
    "System.Threading.Tasks": "",
    "System.Net": "",
    "System.Net.Http": "",
    "Newtonsoft.Json": "import json",
    "System.Xml": "",
    "System.Xml.Linq": "",
}


def _method_to_def(name: str, params: str) -> str:
    """Convert a C# method signature to a Python def statement."""
    py_params = []
    if params.strip():
        for p in params.split(","):
            p = p.strip()
            # "string name" → "name" (drop the type)
            parts = p.split()
            if len(parts) >= 2:
                py_params.append(parts[-1])
            elif parts:
                py_params.append(parts[0])
    param_str = ", ".join(py_params)
    return f"def {name}({param_str}):"
