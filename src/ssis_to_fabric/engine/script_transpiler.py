"""
Advanced Script Transpilation
==============================
Phase 21: AST-based C# parsing, .NET BCL method mapping,
LINQ → PySpark transpilation, confidence scoring, and diff views.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from ssis_to_fabric.logging_config import get_logger

logger = get_logger(__name__)


# =====================================================================
# .NET BCL Method Mapping
# =====================================================================


class BCLNamespace(str, Enum):
    SYSTEM = "System"
    SYSTEM_IO = "System.IO"
    SYSTEM_DATA = "System.Data"
    SYSTEM_NET = "System.Net"
    SYSTEM_TEXT = "System.Text"
    SYSTEM_LINQ = "System.Linq"
    SYSTEM_COLLECTIONS = "System.Collections.Generic"


# C# method → Python equivalent
_BCL_METHOD_MAP: dict[str, str] = {
    # System
    "Console.WriteLine": "print",
    "Convert.ToInt32": "int",
    "Convert.ToString": "str",
    "Convert.ToDouble": "float",
    "Convert.ToBoolean": "bool",
    "Convert.ToDateTime": "datetime.fromisoformat",
    "Math.Abs": "abs",
    "Math.Max": "max",
    "Math.Min": "min",
    "Math.Round": "round",
    "Math.Floor": "math.floor",
    "Math.Ceiling": "math.ceil",
    "Math.Sqrt": "math.sqrt",
    "Math.Pow": "math.pow",
    "String.IsNullOrEmpty": "not bool",
    "String.IsNullOrWhiteSpace": "not str.strip",
    "string.Format": "str.format or f-string",
    "DateTime.Now": "datetime.now()",
    "DateTime.UtcNow": "datetime.now(tz=timezone.utc)",
    "DateTime.Parse": "datetime.fromisoformat",
    "DateTime.TryParse": "datetime.fromisoformat (with try/except)",
    "Guid.NewGuid": "uuid.uuid4",
    # System.IO
    "File.Exists": "Path.exists",
    "File.ReadAllText": "Path.read_text",
    "File.WriteAllText": "Path.write_text",
    "File.ReadAllLines": "Path.read_text().splitlines()",
    "File.Delete": "Path.unlink",
    "File.Copy": "shutil.copy2",
    "File.Move": "shutil.move",
    "Directory.Exists": "Path.is_dir",
    "Directory.CreateDirectory": "Path.mkdir(parents=True, exist_ok=True)",
    "Directory.GetFiles": "list(Path.glob('*'))",
    "Path.Combine": "Path / 'subpath'",
    "Path.GetFileName": "Path.name",
    "Path.GetExtension": "Path.suffix",
    "Path.GetDirectoryName": "str(Path.parent)",
    # System.Data
    "SqlConnection": "pyodbc.connect",
    "SqlCommand": "cursor.execute",
    "SqlDataReader": "cursor.fetchall",
    "DataTable": "pandas.DataFrame",
    "DataRow": "dict or pandas Series",
    # System.Net
    "WebClient.DownloadString": "requests.get(url).text",
    "WebClient.DownloadFile": "requests.get(url, stream=True)",
    "HttpWebRequest": "requests.Request",
    "WebRequest.Create": "requests.get/post",
    # System.Text
    "StringBuilder": "list or io.StringIO",
    "Encoding.UTF8.GetBytes": "str.encode('utf-8')",
    "Encoding.UTF8.GetString": "bytes.decode('utf-8')",
    "Regex.Match": "re.search",
    "Regex.IsMatch": "re.search(...) is not None",
    "Regex.Replace": "re.sub",
    # System.Linq
    "Enumerable.Where": ".filter(lambda x: ...)",
    "Enumerable.Select": ".map(lambda x: ...)",
    "Enumerable.OrderBy": "sorted(key=lambda x: ...)",
    "Enumerable.OrderByDescending": "sorted(key=lambda x: ..., reverse=True)",
    "Enumerable.First": "[0] or next(iter(...))",
    "Enumerable.FirstOrDefault": "next(iter(...), None)",
    "Enumerable.Count": "len(...)",
    "Enumerable.Any": "any(...)",
    "Enumerable.All": "all(...)",
    "Enumerable.Sum": "sum(...)",
    "Enumerable.Average": "statistics.mean(...)",
    "Enumerable.ToList": "list(...)",
    "Enumerable.ToDictionary": "dict(...)",
    "Enumerable.GroupBy": "itertools.groupby",
    "Enumerable.Distinct": "set(...) or list(dict.fromkeys(...))",
}


def get_bcl_mapping(csharp_method: str) -> str | None:
    """Look up a Python equivalent for a C# BCL method call."""
    return _BCL_METHOD_MAP.get(csharp_method)


def get_all_bcl_mappings() -> dict[str, str]:
    """Return all BCL method mappings."""
    return dict(_BCL_METHOD_MAP)


# =====================================================================
# C# AST Nodes (simplified)
# =====================================================================


class CSharpNodeType(str, Enum):
    CLASS = "class"
    METHOD = "method"
    VARIABLE = "variable"
    ASSIGNMENT = "assignment"
    IF_STATEMENT = "if"
    FOR_LOOP = "for"
    FOREACH_LOOP = "foreach"
    WHILE_LOOP = "while"
    TRY_CATCH = "try_catch"
    RETURN = "return"
    METHOD_CALL = "method_call"
    LINQ_QUERY = "linq_query"
    USING = "using"
    PROPERTY = "property"
    UNKNOWN = "unknown"


@dataclass
class CSharpNode:
    """A node in the simplified C# AST."""

    node_type: CSharpNodeType
    name: str = ""
    body: str = ""
    children: list[CSharpNode] = field(default_factory=list)
    line_number: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": self.node_type.value,
            "name": self.name,
            "body": self.body[:100],
            "children_count": len(self.children),
            "line_number": self.line_number,
        }


def parse_csharp_ast(code: str) -> list[CSharpNode]:
    """Parse C# code into a simplified AST.

    Uses pattern matching for common constructs rather than a full parser.
    """
    nodes: list[CSharpNode] = []
    lines = code.split("\n")

    for i, line in enumerate(lines, 1):
        stripped = line.strip()
        if not stripped or stripped.startswith("//"):
            continue

        # Using statements
        if stripped.startswith("using "):
            nodes.append(CSharpNode(CSharpNodeType.USING, name=stripped, line_number=i))
        # Class declarations
        elif re.match(r"(public|private|internal|static)?\s*class\s+(\w+)", stripped):
            m = re.match(r"(?:public|private|internal|static)?\s*class\s+(\w+)", stripped)
            name = m.group(1) if m else "Unknown"
            nodes.append(CSharpNode(CSharpNodeType.CLASS, name=name, body=stripped, line_number=i))
        # Method declarations
        elif re.match(r"(public|private|protected|internal|static|void|override)\s+\w+\s+\w+\s*\(", stripped):
            m = re.match(r"(?:\w+\s+)+(\w+)\s*\(", stripped)
            name = m.group(1) if m else "Unknown"
            nodes.append(CSharpNode(CSharpNodeType.METHOD, name=name, body=stripped, line_number=i))
        # Variable declarations
        elif re.match(r"(var|int|string|bool|double|float|DateTime|List|Dictionary)\s+\w+", stripped):
            nodes.append(CSharpNode(CSharpNodeType.VARIABLE, body=stripped, line_number=i))
        # If statements
        elif stripped.startswith("if ") or stripped.startswith("if("):
            nodes.append(CSharpNode(CSharpNodeType.IF_STATEMENT, body=stripped, line_number=i))
        # Foreach
        elif stripped.startswith("foreach"):
            nodes.append(CSharpNode(CSharpNodeType.FOREACH_LOOP, body=stripped, line_number=i))
        # For
        elif stripped.startswith("for ") or stripped.startswith("for("):
            nodes.append(CSharpNode(CSharpNodeType.FOR_LOOP, body=stripped, line_number=i))
        # While
        elif stripped.startswith("while"):
            nodes.append(CSharpNode(CSharpNodeType.WHILE_LOOP, body=stripped, line_number=i))
        # Try/catch
        elif stripped.startswith("try"):
            nodes.append(CSharpNode(CSharpNodeType.TRY_CATCH, body=stripped, line_number=i))
        # Return
        elif stripped.startswith("return "):
            nodes.append(CSharpNode(CSharpNodeType.RETURN, body=stripped, line_number=i))
        # LINQ queries
        elif ".Where(" in stripped or ".Select(" in stripped or ".OrderBy(" in stripped:
            nodes.append(CSharpNode(CSharpNodeType.LINQ_QUERY, body=stripped, line_number=i))
        # Method calls
        elif re.match(r"\w+\.\w+\(", stripped):
            nodes.append(CSharpNode(CSharpNodeType.METHOD_CALL, body=stripped, line_number=i))

    return nodes


# =====================================================================
# Confidence Scoring
# =====================================================================


@dataclass
class TranspilationResult:
    """Result of transpiling a C# function/block to Python."""

    original_code: str
    transpiled_code: str
    confidence: float = 0.0  # 0.0 - 1.0
    warnings: list[str] = field(default_factory=list)
    unmapped_calls: list[str] = field(default_factory=list)
    node_count: int = 0

    @property
    def needs_manual_review(self) -> bool:
        return self.confidence < 0.7

    def to_dict(self) -> dict[str, Any]:
        return {
            "confidence": round(self.confidence, 2),
            "needs_manual_review": self.needs_manual_review,
            "warnings": self.warnings,
            "unmapped_calls": self.unmapped_calls,
            "node_count": self.node_count,
            "original_lines": self.original_code.count("\n") + 1,
            "transpiled_lines": self.transpiled_code.count("\n") + 1,
        }


def transpile_csharp_to_python(code: str) -> TranspilationResult:
    """Transpile C# code to Python with confidence scoring.

    Uses AST parsing + BCL mapping for better accuracy than regex-only.
    """
    nodes = parse_csharp_ast(code)
    python_lines: list[str] = []
    warnings: list[str] = []
    unmapped: list[str] = []
    mapped_count = 0
    total_significant = 0

    for node in nodes:
        if node.node_type == CSharpNodeType.USING:
            # Map using statements to imports
            ns = node.name.replace("using ", "").rstrip(";").strip()
            import_map = {
                "System": "import sys",
                "System.IO": "from pathlib import Path",
                "System.Data": "import pyodbc",
                "System.Net": "import requests",
                "System.Text.RegularExpressions": "import re",
                "System.Linq": "from functools import reduce",
                "System.Collections.Generic": "# collections available natively",
            }
            python_lines.append(import_map.get(ns, f"# TODO: Map '{ns}' import"))
            mapped_count += 1 if ns in import_map else 0
            total_significant += 1
        elif node.node_type == CSharpNodeType.METHOD:
            # Convert method signature
            name = node.name
            python_lines.append(f"\ndef {_to_snake_case(name)}():")
            mapped_count += 1
            total_significant += 1
        elif node.node_type == CSharpNodeType.VARIABLE:
            # Convert variable declaration
            converted = _convert_variable(node.body)
            python_lines.append(f"    {converted}")
            mapped_count += 1
            total_significant += 1
        elif node.node_type == CSharpNodeType.IF_STATEMENT:
            condition = node.body.replace("if ", "if ").replace("(", "").replace(")", "").replace("{", ":").rstrip()
            python_lines.append(f"    {condition}")
            mapped_count += 1
            total_significant += 1
        elif node.node_type == CSharpNodeType.FOREACH_LOOP:
            m = re.match(r"foreach\s*\(\s*\w+\s+(\w+)\s+in\s+(.+?)\s*\)", node.body)
            if m:
                python_lines.append(f"    for {m.group(1)} in {m.group(2)}:")
                mapped_count += 1
            else:
                python_lines.append(f"    # TODO: Convert foreach: {node.body}")
                warnings.append(f"Line {node.line_number}: Could not parse foreach")
            total_significant += 1
        elif node.node_type == CSharpNodeType.FOR_LOOP:
            python_lines.append(f"    # TODO: Convert for loop: {node.body}")
            total_significant += 1
        elif node.node_type == CSharpNodeType.TRY_CATCH:
            python_lines.append("    try:")
            mapped_count += 1
            total_significant += 1
        elif node.node_type == CSharpNodeType.RETURN:
            val = node.body.replace("return ", "").rstrip(";").strip()
            python_lines.append(f"    return {val}")
            mapped_count += 1
            total_significant += 1
        elif node.node_type == CSharpNodeType.LINQ_QUERY:
            converted = _convert_linq(node.body)
            python_lines.append(f"    {converted}")
            if "TODO" in converted:
                unmapped.append(node.body[:80])
            else:
                mapped_count += 1
            total_significant += 1
        elif node.node_type == CSharpNodeType.METHOD_CALL:
            converted = _convert_method_call(node.body)
            python_lines.append(f"    {converted}")
            if "TODO" in converted:
                unmapped.append(node.body[:80])
            else:
                mapped_count += 1
            total_significant += 1

    confidence = (mapped_count / total_significant) if total_significant > 0 else 1.0

    return TranspilationResult(
        original_code=code,
        transpiled_code="\n".join(python_lines),
        confidence=confidence,
        warnings=warnings,
        unmapped_calls=unmapped,
        node_count=len(nodes),
    )


# =====================================================================
# Diff View
# =====================================================================


def generate_diff_view(original: str, transpiled: str) -> str:
    """Generate a side-by-side diff view as text."""
    orig_lines = original.split("\n")
    trans_lines = transpiled.split("\n")
    max_lines = max(len(orig_lines), len(trans_lines))

    header = f"{'C# (Original)':<50} | {'Python (Transpiled)':<50}"
    separator = "-" * 50 + " | " + "-" * 50
    lines = [header, separator]

    for i in range(max_lines):
        left = orig_lines[i][:48] if i < len(orig_lines) else ""
        right = trans_lines[i][:48] if i < len(trans_lines) else ""
        lines.append(f"{left:<50} | {right:<50}")

    return "\n".join(lines)


# =====================================================================
# Internal Helpers
# =====================================================================


def _to_snake_case(name: str) -> str:
    """Convert PascalCase/camelCase to snake_case."""
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def _convert_variable(line: str) -> str:
    """Convert a C# variable declaration to Python."""
    # var x = value; → x = value
    type_pat = r"(?:var|int|string|bool|double|float|DateTime|List\w*|Dictionary\w*)"
    m = re.match(type_pat + r"\s+(\w+)\s*=\s*(.+);", line.strip())
    if m:
        return f"{m.group(1)} = {m.group(2)}"
    m = re.match(r"(?:var|int|string|bool|double|float)\s+(\w+)\s*;", line.strip())
    if m:
        return f"{m.group(1)} = None"
    return f"# TODO: Convert variable: {line.strip()}"


def _convert_method_call(line: str) -> str:
    """Convert a C# method call using BCL mapping."""
    stripped = line.strip().rstrip(";")
    for cs_method, py_equiv in _BCL_METHOD_MAP.items():
        if cs_method in stripped:
            return f"{py_equiv}  # was: {cs_method}"
    return f"# TODO: Convert call: {stripped}"


def _convert_linq(line: str) -> str:
    """Convert LINQ expressions to Python equivalents."""
    stripped = line.strip().rstrip(";")
    result = stripped
    linq_map = {
        ".Where(": ".filter(",
        ".Select(": ".map(",
        ".OrderBy(": "sorted(",
        ".ToList()": "list(...)",
        ".Count()": "len(...)",
        ".Any()": "any(...)",
        ".First()": "[0]",
    }
    converted = False
    for cs, py in linq_map.items():
        if cs in result:
            result = result.replace(cs, py)
            converted = True
    if converted:
        return f"{result}  # LINQ converted"
    return f"# TODO: Convert LINQ: {stripped}"
