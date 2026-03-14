"""Tests for Phase 21 — Advanced Script Transpilation."""

from __future__ import annotations

from ssis_to_fabric.engine.script_transpiler import (
    CSharpNodeType,
    TranspilationResult,
    generate_diff_view,
    get_all_bcl_mappings,
    get_bcl_mapping,
    parse_csharp_ast,
    transpile_csharp_to_python,
)


class TestBCLMapping:
    def test_console_writeline(self):
        assert get_bcl_mapping("Console.WriteLine") == "print"

    def test_file_exists(self):
        assert get_bcl_mapping("File.Exists") == "Path.exists"

    def test_unknown_method(self):
        assert get_bcl_mapping("SomeCustom.Method") is None

    def test_all_mappings(self):
        mappings = get_all_bcl_mappings()
        assert len(mappings) > 50
        assert "Math.Abs" in mappings
        assert "Regex.Match" in mappings

    def test_linq_mappings(self):
        assert get_bcl_mapping("Enumerable.Where") is not None
        assert get_bcl_mapping("Enumerable.Select") is not None


class TestCSharpAST:
    def test_parse_using(self):
        nodes = parse_csharp_ast("using System;\nusing System.IO;")
        assert len(nodes) == 2
        assert all(n.node_type == CSharpNodeType.USING for n in nodes)

    def test_parse_class(self):
        nodes = parse_csharp_ast("public class MyClass {")
        assert len(nodes) == 1
        assert nodes[0].node_type == CSharpNodeType.CLASS
        assert nodes[0].name == "MyClass"

    def test_parse_method(self):
        nodes = parse_csharp_ast("public void DoWork() {")
        assert len(nodes) == 1
        assert nodes[0].node_type == CSharpNodeType.METHOD

    def test_parse_variable(self):
        nodes = parse_csharp_ast("var x = 42;")
        assert len(nodes) == 1
        assert nodes[0].node_type == CSharpNodeType.VARIABLE

    def test_parse_if(self):
        nodes = parse_csharp_ast("if (x > 0) {")
        assert nodes[0].node_type == CSharpNodeType.IF_STATEMENT

    def test_parse_foreach(self):
        nodes = parse_csharp_ast("foreach (var item in items) {")
        assert nodes[0].node_type == CSharpNodeType.FOREACH_LOOP

    def test_parse_try(self):
        nodes = parse_csharp_ast("try {")
        assert nodes[0].node_type == CSharpNodeType.TRY_CATCH

    def test_parse_return(self):
        nodes = parse_csharp_ast("return result;")
        assert nodes[0].node_type == CSharpNodeType.RETURN

    def test_parse_linq(self):
        nodes = parse_csharp_ast("items.Where(x => x > 0);")
        assert nodes[0].node_type == CSharpNodeType.LINQ_QUERY

    def test_skip_comments(self):
        nodes = parse_csharp_ast("// this is a comment\nvar x = 1;")
        assert len(nodes) == 1


class TestTranspilation:
    def test_simple_code(self):
        code = "using System;\nvar x = 42;\nreturn x;"
        result = transpile_csharp_to_python(code)
        assert isinstance(result, TranspilationResult)
        assert result.confidence > 0
        assert "x = 42" in result.transpiled_code

    def test_foreach_conversion(self):
        code = "foreach (var item in items) {"
        result = transpile_csharp_to_python(code)
        assert "for item in items:" in result.transpiled_code

    def test_confidence_scoring(self):
        code = "var x = 1;\nreturn x;"
        result = transpile_csharp_to_python(code)
        assert 0.0 <= result.confidence <= 1.0

    def test_unmapped_calls(self):
        code = "CustomFramework.DoSomethingWeird();"
        result = transpile_csharp_to_python(code)
        assert "TODO" in result.transpiled_code

    def test_needs_manual_review(self):
        result = TranspilationResult(original_code="", transpiled_code="", confidence=0.5)
        assert result.needs_manual_review

    def test_high_confidence_no_review(self):
        result = TranspilationResult(original_code="", transpiled_code="", confidence=0.9)
        assert not result.needs_manual_review


class TestDiffView:
    def test_diff_output(self):
        diff = generate_diff_view("using System;\nvar x = 1;", "import sys\nx = 1")
        assert "C# (Original)" in diff
        assert "Python (Transpiled)" in diff
