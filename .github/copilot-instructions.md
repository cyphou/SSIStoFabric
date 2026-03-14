# Copilot Instructions — SSIS to Fabric Migration Tool

> These instructions define how GitHub Copilot (and any AI coding agent) should
> operate inside this repository. They are loaded automatically by VS Code
> GitHub Copilot when `chat.promptFiles` is enabled.

---

## 1. Plan Mode Default

- Enter plan mode for ANY non-trivial task (3+ steps or architectural decisions).
- If something goes sideways, **STOP and re-plan immediately** — don't keep pushing.
- Use plan mode for verification steps, not just building.
- Write detailed specs upfront to reduce ambiguity.

## 2. Subagent Strategy

- Use subagents liberally to keep main context window clean.
- Offload research, exploration, and parallel analysis to subagents.
- For complex problems, throw more compute at it via subagents.
- One task per subagent for focused execution.

## 3. Self-Improvement Loop

- After ANY correction from the user: update `tasks/lessons.md` with the pattern.
- Write rules for yourself that prevent the same mistake.
- Ruthlessly iterate on these lessons until mistake rate drops.
- Review lessons at session start for relevant project.

## 4. Verification Before Done

- Never mark a task complete without proving it works.
- Diff behavior between main and your changes when relevant.
- Ask yourself: "Would a staff engineer approve this?"
- Run tests, check logs, demonstrate correctness.

## 5. Demand Elegance (Balanced)

- For non-trivial changes: pause and ask "is there a more elegant way?"
- If a fix feels hacky: "Knowing everything I know now, implement the elegant solution."
- Skip this for simple, obvious fixes — don't over-engineer.
- Challenge your own work before presenting it.

## 6. Autonomous Bug Fixing

- When given a bug report: just fix it. Don't ask for hand-holding.
- Point at logs, errors, failing tests — then resolve them.
- Zero context switching required from the user.
- Go fix failing CI tests without being told how.

---

## Task Management

1. **Plan First**: Write plan to `tasks/todo.md` with checkable items.
2. **Verify Plan**: Check in before starting implementation.
3. **Track Progress**: Mark items complete as you go.
4. **Explain Changes**: High-level summary at each step.
5. **Document Results**: Add review section to `tasks/todo.md`.
6. **Capture Lessons**: Update `tasks/lessons.md` after corrections.

---

## Core Principles

- **Simplicity First**: Make every change as simple as possible. Impact minimal code.
- **No Laziness**: Find root causes. No temporary fixes. Senior developer standards.
- **Test Everything**: Every code change must include or update tests.
- **Preserve Patterns**: Follow existing project conventions (see below).

---

## Project-Specific Conventions

### Architecture

```
src/ssis_to_fabric/
├── analyzer/         → dtsx_parser.py, models.py (Pydantic)
├── engine/           → 32 migration modules
├── cli.py            → 33 Click CLI commands
├── api.py            → SSISMigrator facade
├── config.py         → MigrationConfig (Pydantic)
└── logging_config.py → structlog
```

### Code Patterns

- **Dataclasses** with `to_dict()` methods for engine entities (`MigrationItem`, `MigrationPlan`, etc.)
- **`(str, Enum)`** for JSON-serializable enums
- **`TYPE_CHECKING`** guards for heavy import deferral
- **Module-level functions** as public API (e.g., `_save_migration_state()`)
- **Lazy imports** inside CLI commands to keep startup fast
- Line length: **120** (enforced by `ruff`)
- Python target: **3.10+**

### Testing

- Framework: **pytest** with markers (`@pytest.mark.unit`)
- Each phase gets its own test file: `test_phase{N}_{name}.py`
- Tests use real temp directories — no mocking of file I/O
- Run: `pytest tests/unit/ -v`

### Git

- Conventional commits: `feat:`, `fix:`, `test:`, `docs:`
- Branch per feature: `feature/{name}`

### Release Checklist

1. Update `CHANGELOG.md`
2. Update version in `pyproject.toml`
3. Update badges in `README.md`
4. Update dev plan in `CONTRIBUTING.md`
5. Run full test suite
6. Commit and tag

---

## File References

| File | Purpose |
|------|---------|
| `tasks/todo.md` | Current task plan and progress |
| `tasks/lessons.md` | Accumulated lessons and anti-patterns |
| `CONTRIBUTING.md` | Dev plan and roadmap |
| `CHANGELOG.md` | Version history |
| `README.md` | User-facing documentation |

---

## 🤖 Multi-Agent Prompts

Specialized agents live in `.github/prompts/`. Invoke them via `@workspace /prompt` or by name in Copilot Chat.

| Agent | File | Purpose |
|-------|------|---------|
| **Planner** | `.github/prompts/planner.prompt.md` | Phase design, roadmap updates, task breakdown, scope estimation |
| **Migrate** | `.github/prompts/migrate.prompt.md` | Engine modules, code generation, CLI commands, API methods |
| **Test** | `.github/prompts/test.prompt.md` | Write tests, run suite, fix failures, coverage gaps |
| **Deploy** | `.github/prompts/deploy.prompt.md` | Push artifacts to Fabric, validate deployments, rollback |
| **Review** | `.github/prompts/review.prompt.md` | Code review, quality checks, standards enforcement |
| **Debug** | `.github/prompts/debug.prompt.md` | Root cause analysis, bug fixes, regression prevention |
| **Release** | `.github/prompts/release.prompt.md` | Version bumps, changelog, badges, git tags |

### Typical Workflow

```
Planner  →  defines Phase N scope and writes tasks/todo.md
Migrate  →  implements engine module, CLI, API
Test     →  writes tests, runs suite, fixes failures
Review   →  reviews changes against quality checklist
Debug    →  fixes any bugs found during review/testing
Release  →  bumps version, updates docs, prepares tag
Deploy   →  pushes artifacts to Fabric workspace
```
