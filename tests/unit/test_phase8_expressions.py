"""Phase 8 — Expression & Transpiler Completeness tests.

Tests for:
- Bitwise operators (BITAND, BITOR, BITXOR) in PySpark and M
- Recursive nested expression handling in PySpark
- New functions: GETUTCDATE, trig, LOG2, RAND, PI
- Expression validation & error reporting
- C# transpiler AST-style structure extraction
- Power Query M parity for new functions
"""

from __future__ import annotations

from ssis_to_fabric.engine.csharp_transpiler import CSharpTranspiler
from ssis_to_fabric.engine.expression_transpiler import (
    ExpressionTranspiler,
    TargetLanguage,
    ValidationResult,
)

# Helper aliases
_to_pyspark = ExpressionTranspiler.to_pyspark
_to_m = ExpressionTranspiler.to_m
_validate = ExpressionTranspiler.validate


# ===================================================================
# PySpark: Bitwise operators
# ===================================================================


class TestPySparkBitwiseOps:
    def test_bitand(self):
        result = _to_pyspark("BITAND(flags, 3)")
        assert "bitwiseAND" in result

    def test_bitor(self):
        result = _to_pyspark("BITOR(flags, 8)")
        assert "bitwiseOR" in result

    def test_bitxor(self):
        result = _to_pyspark("BITXOR(a, b)")
        assert "bitwiseXOR" in result

    def test_bitand_nested(self):
        result = _to_pyspark("BITAND(BITOR(a, b), 255)")
        assert "bitwiseAND" in result
        assert "bitwiseOR" in result


# ===================================================================
# Power Query M: Bitwise operators
# ===================================================================


class TestMBitwiseOps:
    def test_bitand(self):
        result = _to_m("BITAND(flags, 3)")
        assert "Number.BitwiseAnd" in result

    def test_bitor(self):
        result = _to_m("BITOR(flags, 8)")
        assert "Number.BitwiseOr" in result

    def test_bitxor(self):
        result = _to_m("BITXOR(a, b)")
        assert "Number.BitwiseXor" in result


# ===================================================================
# PySpark: Recursive nested expressions
# ===================================================================


class TestPySparkNestedExpressions:
    def test_upper_of_substring(self):
        result = _to_pyspark("UPPER(SUBSTRING(Name, 1, 5))")
        assert "F.upper" in result
        assert "F.substring" in result

    def test_replace_nested_in_lower(self):
        result = _to_pyspark('LOWER(REPLACE(Name, "old", "new"))')
        assert "F.lower" in result
        assert "F.regexp_replace" in result

    def test_three_levels_deep(self):
        result = _to_pyspark('UPPER(SUBSTRING(REPLACE(Name, "a", "b"), 1, 5))')
        assert "F.upper" in result
        assert "F.substring" in result
        assert "F.regexp_replace" in result

    def test_datepart_nested(self):
        result = _to_pyspark('DATEPART("yyyy", GETDATE())')
        assert "F.year" in result
        assert "current_timestamp" in result

    def test_replacenull_with_function_arg(self):
        result = _to_pyspark("REPLACENULL(Age, ABS(DefaultAge))")
        assert "F.coalesce" in result
        assert "F.abs" in result

    def test_len_of_trim(self):
        result = _to_pyspark("LEN(TRIM(Name))")
        assert "F.length" in result
        assert "F.trim" in result

    def test_abs_of_round(self):
        result = _to_pyspark("ABS(ROUND(Price, 2))")
        assert "F.abs" in result
        assert "F.round" in result


# ===================================================================
# PySpark: New functions
# ===================================================================


class TestPySparkNewFunctions:
    def test_getutcdate(self):
        result = _to_pyspark("GETUTCDATE()")
        assert "utc_timestamp" in result.lower() or "UTC" in result

    def test_sin(self):
        result = _to_pyspark("SIN(angle)")
        assert "F.sin" in result

    def test_cos(self):
        result = _to_pyspark("COS(angle)")
        assert "F.cos" in result

    def test_tan(self):
        result = _to_pyspark("TAN(angle)")
        assert "F.tan" in result

    def test_asin(self):
        result = _to_pyspark("ASIN(val)")
        assert "F.asin" in result

    def test_acos(self):
        result = _to_pyspark("ACOS(val)")
        assert "F.acos" in result

    def test_atan(self):
        result = _to_pyspark("ATAN(val)")
        assert "F.atan" in result

    def test_atan2(self):
        result = _to_pyspark("ATAN2(y, x)")
        assert "F.atan2" in result

    def test_log2(self):
        result = _to_pyspark("LOG2(val)")
        assert "F.log2" in result

    def test_pi(self):
        result = _to_pyspark("PI()")
        assert "3.14159" in result

    def test_rand(self):
        result = _to_pyspark("RAND()")
        assert "F.rand()" in result

    def test_square(self):
        result = _to_pyspark("SQUARE(val)")
        assert "F.pow" in result
        assert "2" in result


# ===================================================================
# Power Query M: New functions
# ===================================================================


class TestMNewFunctions:
    def test_getutcdate(self):
        result = _to_m("GETUTCDATE()")
        assert "DateTimeZone.UtcNow()" in result

    def test_sin(self):
        result = _to_m("SIN(angle)")
        assert "Number.Sin" in result

    def test_cos(self):
        result = _to_m("COS(angle)")
        assert "Number.Cos" in result

    def test_tan(self):
        result = _to_m("TAN(angle)")
        assert "Number.Tan" in result

    def test_atan2(self):
        result = _to_m("ATAN2(y, x)")
        assert "Number.Atan2" in result

    def test_pi(self):
        result = _to_m("PI()")
        assert "Number.PI" in result

    def test_rand(self):
        result = _to_m("RAND()")
        assert "Number.Random()" in result

    def test_log2(self):
        result = _to_m("LOG2(val)")
        assert "Number.Log" in result


# ===================================================================
# Expression Validation
# ===================================================================


class TestExpressionValidation:
    def test_valid_simple(self):
        result = _validate("UPPER(Name)")
        assert result.is_valid

    def test_valid_nested(self):
        result = _validate("DATEADD(dd, 1, GETDATE())")
        assert result.is_valid

    def test_empty_expression(self):
        result = _validate("")
        assert len(result.issues) == 1
        assert result.issues[0].severity == "warning"
        assert "Empty" in result.issues[0].message

    def test_unbalanced_parens_extra_close(self):
        result = _validate("UPPER(Name))")
        assert not result.is_valid
        assert any("Unbalanced" in i.message for i in result.issues)

    def test_unbalanced_parens_unclosed(self):
        result = _validate("UPPER(LOWER(Name)")
        assert not result.is_valid
        assert any("unclosed" in i.message for i in result.issues)

    def test_unknown_function(self):
        result = _validate("MYSTERY_FUNC(x, y)")
        assert any("Unknown function" in i.message and "MYSTERY_FUNC" in i.message for i in result.issues)

    def test_known_functions_no_warning(self):
        result = _validate("UPPER(LOWER(TRIM(Name)))")
        assert result.is_valid
        assert not any(i.severity == "warning" for i in result.issues)

    def test_bitwise_functions_known(self):
        result = _validate("BITAND(BITOR(a, b), BITXOR(c, d))")
        assert result.is_valid

    def test_unknown_type_cast(self):
        result = _validate("(DT_FOOBAR) col")
        assert any("Unknown SSIS type cast" in i.message for i in result.issues)

    def test_known_type_cast_no_warning(self):
        result = _validate("(DT_I4) col")
        assert not any("Unknown SSIS type cast" in i.message for i in result.issues)


# ===================================================================
# ExpressionTranspiler facade
# ===================================================================


class TestTranspilerFacade:
    def test_transpile_pyspark(self):
        t = ExpressionTranspiler()
        result = t.transpile("UPPER(Name)", TargetLanguage.PYSPARK)
        assert "F.upper" in result

    def test_transpile_m(self):
        t = ExpressionTranspiler()
        result = t.transpile("UPPER(Name)", TargetLanguage.POWER_QUERY_M)
        assert "Text.Upper" in result


# ===================================================================
# PySpark: Existing function regressions (recursive rewrite)
# ===================================================================


class TestPySparkRegressions:
    def test_getdate(self):
        assert "current_timestamp" in _to_pyspark("GETDATE()")

    def test_null_literal(self):
        assert _to_pyspark("null") == "F.lit(None)"
        assert _to_pyspark("") == "F.lit(None)"

    def test_dateadd_days(self):
        result = _to_pyspark('DATEADD("dd", 5, OrderDate)')
        assert "date_add" in result

    def test_dateadd_months(self):
        result = _to_pyspark('DATEADD("mm", 3, StartDate)')
        assert "add_months" in result

    def test_datediff_days(self):
        result = _to_pyspark('DATEDIFF("dd", StartDate, EndDate)')
        assert "datediff" in result.lower()

    def test_datepart_year(self):
        result = _to_pyspark('DATEPART("yyyy", OrderDate)')
        assert "F.year" in result

    def test_year(self):
        result = _to_pyspark("YEAR(OrderDate)")
        assert "F.year" in result

    def test_month(self):
        result = _to_pyspark("MONTH(OrderDate)")
        assert "F.month" in result

    def test_day(self):
        result = _to_pyspark("DAY(OrderDate)")
        assert "dayofmonth" in result

    def test_isnull(self):
        result = _to_pyspark("ISNULL(Name)")
        assert "isNull" in result

    def test_replacenull(self):
        result = _to_pyspark('REPLACENULL(Name, "N/A")')
        assert "coalesce" in result

    def test_null_dt_wstr(self):
        result = _to_pyspark("NULL(DT_WSTR, 50)")
        assert 'cast("string")' in result

    def test_null_dt_i4(self):
        result = _to_pyspark("NULL(DT_I4)")
        assert 'cast("int")' in result

    def test_upper(self):
        result = _to_pyspark("UPPER(Name)")
        assert "F.upper" in result

    def test_lower(self):
        result = _to_pyspark("LOWER(Name)")
        assert "F.lower" in result

    def test_trim(self):
        result = _to_pyspark("TRIM(Name)")
        assert "F.trim" in result

    def test_len(self):
        result = _to_pyspark("LEN(Name)")
        assert "F.length" in result

    def test_left(self):
        result = _to_pyspark("LEFT(Name, 5)")
        assert "F.substring" in result

    def test_findstring(self):
        result = _to_pyspark('FINDSTRING(Name, "test")')
        assert "F.locate" in result

    def test_replace(self):
        result = _to_pyspark('REPLACE(Name, "old", "new")')
        assert "regexp_replace" in result

    def test_substring(self):
        result = _to_pyspark("SUBSTRING(Name, 1, 5)")
        assert "F.substring" in result

    def test_reverse(self):
        result = _to_pyspark("REVERSE(Name)")
        assert "F.reverse" in result

    def test_codepoint(self):
        result = _to_pyspark("CODEPOINT(Name)")
        assert "F.ascii" in result

    def test_hex(self):
        result = _to_pyspark("HEX(val)")
        assert "F.hex" in result

    def test_abs(self):
        result = _to_pyspark("ABS(val)")
        assert "F.abs" in result

    def test_ceiling(self):
        result = _to_pyspark("CEILING(val)")
        assert "F.ceil" in result

    def test_floor(self):
        result = _to_pyspark("FLOOR(val)")
        assert "F.floor" in result

    def test_round(self):
        result = _to_pyspark("ROUND(val, 2)")
        assert "F.round" in result

    def test_power(self):
        result = _to_pyspark("POWER(val, 3)")
        assert "F.pow" in result

    def test_sign(self):
        result = _to_pyspark("SIGN(val)")
        assert "F.signum" in result

    def test_sqrt(self):
        result = _to_pyspark("SQRT(val)")
        assert "F.sqrt" in result

    def test_exp(self):
        result = _to_pyspark("EXP(val)")
        assert "F.exp" in result

    def test_log(self):
        result = _to_pyspark("LOG(val)")
        assert "F.log" in result

    def test_log10(self):
        result = _to_pyspark("LOG10(val)")
        assert "F.log10" in result

    def test_variable_package(self):
        result = _to_pyspark("@[$Package::LoadId]")
        assert "LOADID" in result

    def test_variable_user(self):
        result = _to_pyspark("@[User::StartDate]")
        assert "STARTDATE" in result

    def test_cast_dt_i4(self):
        result = _to_pyspark("(DT_I4) Amount")
        assert 'cast("int")' in result

    def test_cast_dt_r8(self):
        result = _to_pyspark("(DT_R8) Price")
        assert 'cast("double")' in result

    def test_cast_dt_bool(self):
        result = _to_pyspark("(DT_BOOL) IsActive")
        assert 'cast("boolean")' in result

    def test_cast_dt_date(self):
        result = _to_pyspark("(DT_DATE) OrderDate")
        assert 'cast("date")' in result

    def test_cast_dt_timestamp(self):
        result = _to_pyspark("(DT_DBTIMESTAMP) Created")
        assert 'cast("timestamp")' in result

    def test_cast_dt_decimal(self):
        result = _to_pyspark("(DT_DECIMAL, 2) Amount")
        assert "decimal" in result

    def test_cast_dt_numeric(self):
        result = _to_pyspark("(DT_NUMERIC, 10, 2) Amount")
        assert "decimal(10,2)" in result

    def test_string_concat(self):
        result = _to_pyspark('"Hello " + Name + " World"')
        assert "F.concat" in result

    def test_ternary(self):
        result = _to_pyspark("Age > 18 ? Adult : Child")
        assert "F.when" in result
        assert "otherwise" in result

    def test_tokencount(self):
        result = _to_pyspark('TOKENCOUNT(Name, ",")')
        assert "F.size" in result
        assert "F.split" in result


# ===================================================================
# PySpark: Cast with nested expressions
# ===================================================================


class TestPySparkCastNested:
    def test_cast_wraps_nested(self):
        result = _to_pyspark("(DT_I4) ROUND(Price, 0)")
        assert 'cast("int")' in result
        assert "F.round" in result

    def test_cast_str_with_expr(self):
        result = _to_pyspark("(DT_WSTR, 50) UPPER(Name)")
        assert 'cast("string")' in result
        assert "F.upper" in result


# ===================================================================
# C# Transpiler: AST structure extraction
# ===================================================================


class TestCSharpASTExtraction:
    def test_extract_usings(self):
        code = """
using System;
using System.IO;
using System.Text.RegularExpressions;

class MyScript {
    void Main() {
        Console.WriteLine("Hello");
    }
}
"""
        t = CSharpTranspiler()
        structure = t._extract_structure(code)
        assert "System" in structure["usings"]
        assert "System.IO" in structure["usings"]
        assert "System.Text.RegularExpressions" in structure["usings"]

    def test_extract_classes(self):
        code = """
public class ScriptMain : UserComponent {
    public override void Main() { }
}
"""
        t = CSharpTranspiler()
        structure = t._extract_structure(code)
        assert "ScriptMain" in structure["classes"]

    def test_extract_methods(self):
        code = """
public class MyClass {
    public void ProcessData(string input, int count) {
        // body
    }
    private string FormatOutput(double value) {
        return value.ToString();
    }
}
"""
        t = CSharpTranspiler()
        structure = t._extract_structure(code)
        assert len(structure["methods"]) == 2
        names = [m["name"] for m in structure["methods"]]
        assert "ProcessData" in names
        assert "FormatOutput" in names

    def test_extract_namespace(self):
        code = """
namespace MyCompany.ETL {
    class MyScript { }
}
"""
        t = CSharpTranspiler()
        structure = t._extract_structure(code)
        assert structure["namespace"] == "MyCompany.ETL"

    def test_extract_static_method(self):
        code = """
public class Util {
    public static string Helper(string s) { return s; }
}
"""
        t = CSharpTranspiler()
        structure = t._extract_structure(code)
        assert structure["methods"][0]["is_static"] is True


# ===================================================================
# C# Transpiler: AST-enhanced transpile
# ===================================================================


class TestCSharpASTTranspile:
    def test_using_to_import(self):
        code = """
using System.Text.RegularExpressions;
using System.Data.SqlClient;

class ScriptMain {
    void Main() {
        string s = "hello";
    }
}
"""
        t = CSharpTranspiler()
        result = t.transpile(code)
        assert "import re" in result

    def test_class_to_python_class(self):
        code = """
public class ScriptMain : UserComponent {
    public void Main() {
        Console.WriteLine("Hi");
    }
}
"""
        t = CSharpTranspiler()
        result = t.transpile(code)
        assert "class ScriptMain:" in result

    def test_method_to_def(self):
        code = """
public class MyScript {
    public void ProcessData(string input, int count) {
        Console.WriteLine(input);
    }
}
"""
        t = CSharpTranspiler()
        result = t.transpile(code)
        assert "def ProcessData(input, count):" in result

    def test_namespace_stripped(self):
        code = """
namespace ETL.Scripts {
    class MyScript {
        void Main() { }
    }
}
"""
        t = CSharpTranspiler()
        result = t.transpile(code)
        assert "namespace" not in result or "# namespace" in result

    def test_using_stripped(self):
        code = """
using System;
using System.IO;

class Test {
    void Main() { }
}
"""
        t = CSharpTranspiler()
        result = t.transpile(code)
        # Using directives should be stripped from body (converted to imports)
        assert "using System;" not in result


# ===================================================================
# M transpiler: Regression tests for existing functions
# ===================================================================


class TestMRegressions:
    def test_upper(self):
        assert "Text.Upper" in _to_m("UPPER(Name)")

    def test_lower(self):
        assert "Text.Lower" in _to_m("LOWER(Name)")

    def test_trim(self):
        assert "Text.Trim" in _to_m("TRIM(Name)")

    def test_len(self):
        assert "Text.Length" in _to_m("LEN(Name)")

    def test_replace(self):
        result = _to_m('REPLACE(Name, "a", "b")')
        assert "Text.Replace" in result

    def test_getdate(self):
        assert "DateTime.LocalNow()" in _to_m("GETDATE()")

    def test_dateadd_day(self):
        result = _to_m('DATEADD("DD", 1, OrderDate)')
        assert "Date.AddDays" in result

    def test_datepart_year(self):
        result = _to_m('DATEPART("YYYY", OrderDate)')
        assert "Date.Year" in result

    def test_abs(self):
        assert "Number.Abs" in _to_m("ABS(val)")

    def test_ceiling(self):
        assert "Number.RoundUp" in _to_m("CEILING(val)")

    def test_floor(self):
        assert "Number.RoundDown" in _to_m("FLOOR(val)")

    def test_round(self):
        assert "Number.Round" in _to_m("ROUND(val, 2)")

    def test_power(self):
        assert "Number.Power" in _to_m("POWER(val, 3)")

    def test_sqrt(self):
        assert "Number.Sqrt" in _to_m("SQRT(val)")

    def test_sign(self):
        assert "Number.Sign" in _to_m("SIGN(val)")

    def test_log(self):
        assert "Number.Log" in _to_m("LOG(val)")

    def test_tokencount(self):
        result = _to_m('TOKENCOUNT(Name, ",")')
        assert "List.Count" in result
        assert "Text.Split" in result

    def test_codepoint(self):
        result = _to_m("CODEPOINT(Name)")
        assert "Character.ToNumber" in result

    def test_hex(self):
        result = _to_m("HEX(val)")
        assert "Number.ToText" in result

    def test_replacenull(self):
        result = _to_m('REPLACENULL(Name, "N/A")')
        assert "if" in result and "null" in result

    def test_isnull(self):
        result = _to_m("ISNULL(Name)")
        assert "null" in result


# ===================================================================
# ValidationResult dataclass
# ===================================================================


class TestValidationResult:
    def test_empty_issues_is_valid(self):
        r = ValidationResult(expression="X")
        assert r.is_valid

    def test_warning_only_is_valid(self):
        from ssis_to_fabric.engine.expression_transpiler import ExpressionIssue

        r = ValidationResult(
            expression="X",
            issues=[ExpressionIssue("warning", "test", "X")],
        )
        assert r.is_valid

    def test_error_is_not_valid(self):
        from ssis_to_fabric.engine.expression_transpiler import ExpressionIssue

        r = ValidationResult(
            expression="X",
            issues=[ExpressionIssue("error", "test", "X")],
        )
        assert not r.is_valid
