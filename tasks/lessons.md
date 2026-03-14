# Lessons Learned — SSIS to Fabric Migration Tool

> Copilot updates this file after any correction from the user.
> Review at the start of each session to avoid repeating mistakes.

---

## 2026-03-14 — Merge Conflict Resolution

### Lesson: Always verify test suite after `git checkout --theirs`

**What happened**: After resolving merge conflicts by keeping the GitHub version
of `migration_engine.py` and `spark_generator.py`, 84 tests failed because the
GitHub version lacked local features (timing fields, progress callbacks,
recursive expression transpiler, plugin integration, etc.).

**Rule**: After any merge conflict resolution, ALWAYS run `pytest tests/unit/ -q`
immediately and fix all failures before moving on. Don't assume the GitHub
version is complete — it may be behind on features added locally.

### Lesson: Recursive descent for expression transpiling

**What happened**: The flat `re.match()` approach in `_ssis_expr_to_pyspark()`
couldn't handle nested expressions like `UPPER(SUBSTRING(Name, 1, 5))`. Each
function only matched `\w+` as arguments, so nested calls fell through to
`F.expr()` fallback.

**Rule**: Expression transpilers MUST use recursive argument parsing. Extract
function name + raw args, split args respecting parentheses depth, then
recursively convert each argument. This handles arbitrary nesting depth.

### Lesson: Cast of numeric literals needs F.lit() wrapping

**What happened**: `(DT_WSTR,50) 69` produced `69.cast("string")` which is
invalid Python (`69.cast` triggers "invalid decimal literal"). Bare numeric
atoms need `F.lit()` wrapping before `.cast()`.

**Rule**: When building cast expressions, check if inner result is a bare
number (`re.match(r'^-?\d+(\.\d+)?$', inner)`) and wrap with `F.lit()`.

### Lesson: `_save_migration_state` path contract

**What happened**: The test expected `state_dir / "state.json"` but the function
wrote to `state_dir / ".ssis2fabric" / "state.json"`. The calling code already
includes the subdirectory context.

**Rule**: Check test expectations before implementing. The function's contract
is to write directly to `base_dir / "state.json"`, not add its own subdirectory.

---

## Patterns to Watch For

- **Missing fields after merge**: When GitHub version lacks local fields, tests
  will fail with `AttributeError`. Always grep test imports for the exact
  fields/methods expected.
- **Static method signatures**: When adding parameters to `execute()`, also
  update all callers: `api.py`, `agents.py`, CLI commands.
- **Plugin registry integration**: Both `spark_generator.py` and
  `dataflow_generator.py` should check `get_component_registry()` before
  the if/elif chain, and apply `get_expression_rules()` in transpilers.
