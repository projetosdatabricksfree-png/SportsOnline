# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Databricks Sports** is a data transformation project built on **dbt core** for analyzing sports data. The codebase uses declarative SQL-based transformations with tests and documentation, deployed on Databricks.

### Directory Structure

```
DATABRICKS_SPORTS/
├── projects/
│   └── databricks-sports/          # Main dbt project (work here)
│       ├── models/                 # dbt models (SQL transformations)
│       ├── tests/                  # dbt data quality tests
│       ├── macros/                 # Reusable SQL macros
│       ├── seeds/                  # Static CSV data
│       ├── snapshots/              # Slowly Changing Dimension configs
│       ├── analyses/               # Exploratory SQL analysis
│       ├── dbt_project.yml         # Main dbt configuration
│       ├── profiles.yml            # Databricks connection config (local)
│       └── logs/                   # dbt execution logs (gitignored)
│
├── .agents/                        # Official Databricks CLI skills and config
│   └── skills/                     # Skills for use with Claude Code
│
├── .agent-instances/               # Legacy AI agent instances (can delete)
│   ├── .claude/                    # Claude agent config
│   ├── .goose/, .bob/, etc.        # Other agent configs (mostly unused)
│   └── README.md                   # Agent documentation
│
├── skills/                         # Shared skill configuration (skills-lock.json)
├── venv/                           # Python virtual environment (gitignored)
└── README.md, STRUCTURE.md         # Project documentation
```

## Common Commands

**All dbt commands are run from `projects/databricks-sports/`**

```bash
cd projects/databricks-sports/

# Install dbt dependencies (packages defined in packages.yml)
dbt deps

# Run all models (compile and execute SQL on Databricks)
dbt run

# Run only models with certain tags or selectors
dbt run --select tag:daily        # By tag
dbt run --select models/staging   # By path
dbt run --select state:new+       # New + downstream

# Run tests (data quality checks)
dbt test

# Run tests for specific models
dbt test --select my_model

# Generate documentation and lineage graph
dbt docs generate
dbt docs serve  # View at http://localhost:8000

# Dry run (compile without executing)
dbt compile --select my_model

# Clean generated files (target/, dbt_packages/)
dbt clean

# Validate project structure and references
dbt parse

# Test a single model's path
dbt test --select tests/data/my_test.sql
```

## Development Workflow

### Writing Models

Models are in `models/` subdirectories (e.g., `models/staging/`, `models/marts/`). Each `.sql` file in these folders is a model:

- **Structure**: `{{ config(...) }} SELECT ...` at the top, then the SQL
- **Materialization**: Controlled by `config()` or `dbt_project.yml`. Common types:
  - `view` (default) — computed on-demand
  - `table` — materialized and stored
  - `incremental` — appends new rows, idempotent
- **Ref function**: Use `{{ ref('model_name') }}` to reference other models (creates dependency graph)
- **Source function**: Use `{{ source('source_name', 'table') }}` for raw Databricks tables

### Adding Tests

Tests go in `tests/`. Two types:

1. **Generic tests** (in YAML): Reusable, typically in a `.yml` file alongside models
   ```yaml
   models:
     - name: my_table
       columns:
         - name: id
           tests:
             - unique
             - not_null
   ```

2. **Singular tests** (in SQL): Custom `.sql` files in `tests/`. One test per file.
   ```sql
   SELECT *
   FROM {{ ref('my_model') }}
   WHERE id IS NULL
   ```

### Using Macros

Macros are reusable Jinja2 blocks in `macros/`. Call them with `{{ macro_name(...) }}`. Use for DRY logic across multiple models.

## Architecture Insights

### dbt Profile Configuration

The `profile` in `dbt_project.yml` references `databrickssports`. Your local `~/.dbt/profiles.yml` (or project-level `profiles.yml`) must define:

```yaml
databrickssports:
  target: dev
  outputs:
    dev:
      type: databricks
      host: [your-workspace-url]
      http_path: [your-cluster-http-path]
      token: [your-PAT-token]
      threads: 4
```

**Never commit `profiles.yml`** to the repo — use environment variables or local git-ignored config.

### Model Organization

- **staging/** (`models/staging/`): Raw data cleaned/renamed, 1:1 with source tables
- **marts/** (`models/marts/`): Business logic, aggregations, denormalizations for end users
- **intermediate/** (optional): Reusable building blocks between staging and marts

Follow this layering to keep transformations modular and testable.

### Project Configuration

`dbt_project.yml` defines:
- `name: 'databrickssports'` — project name
- `profile: 'databrickssports'` — which credentials to use
- `model-paths`, `test-paths`, `macro-paths`, etc. — where dbt looks for files
- `models:` config — default materialization, tags, and meta for all models
- `clean-targets` — directories removed by `dbt clean`

## AI Agent Instances

The `.agent-instances/` folder contains configurations for various AI agents that were installed over time. These are mostly **legacy** and not actively used.

### What Are These Agents?

Each agent (Kiro, Goose, Claude, Bob, OpenHands, etc.) is a separate AI tool from different platforms:

- **Kiro** — Automation agent (legacy)
- **Goose** — Autonomous development agent (legacy)
- **Claude** — Claude agent from Anthropic (potential use)
- **OpenHands** — Open-source autonomous agent (potential use)
- **Continue** — IDE extension for code assistance
- **Windsurf** — Codeium's IDE agent
- **Others** (.bob, .qwen, .cortex, .crush, etc.) — Legacy agents from various platforms

### What Do They Do?

Each agent:
- Has its own **configuration folder** (cached settings, cache files, etc.)
- Contains **symlinks** to the Databricks skills in `.agents/skills/`
- Can be used independently with different tools/IDEs

### Should You Keep Them?

**Recommendation**: You can safely **delete `.agent-instances/`** if you:
- Use **Claude Code** (cli or VS Code extension) — it's the native way to work with Claude
- Don't use legacy agents like Kiro, Goose, Bob, etc.
- Don't use Continue or other IDE extensions requiring those configs

**Keep them only if** you actively use:
- Continue IDE extension
- Windsurf IDE
- Claude agent in another workflow

For details, see [`.agent-instances/README.md`](.agent-instances/README.md).

## Databricks Skills Integration

The project includes official Databricks CLI skills accessible via Claude Code slash commands:

- **/databricks-core** — Authentication, workspace navigation, data exploration, CLI operations
- **/databricks-pipelines** — Lakeflow Spark Declarative Pipelines (Delta Live Tables)
- **/databricks-jobs** — Job scheduling and monitoring on Databricks
- **/databricks-dabs** — Declarative Automation Bundles (infrastructure-as-code for Databricks)
- **/databricks-apps** — Building dashboards and analytical applications
- **/databricks-lakebase** — Postgres Autoscaling project and compute management
- **/databricks-model-serving** — Model serving endpoints for inference

Skills are in `.agents/skills/` for reference; invoke them via commands, not by editing the files directly.

## Key Files

| File | Purpose |
|------|---------|
| `dbt_project.yml` | Main dbt config: project name, profile, model paths, default configs |
| `packages.yml` | (if exists) Third-party dbt packages to install via `dbt deps` |
| `profiles.yml` (local) | Databricks credentials; **never commit** |
| `sources.yml` | (if exists) Defines raw Databricks tables as dbt sources |
| `schema.yml` | Column-level docs, generic tests, and model metadata |
| `.gitignore` | Excludes `target/`, `dbt_packages/`, `logs/`, `venv/` |

## Testing and Validation

Run the full test suite before committing:

```bash
dbt run && dbt test
```

For CI/CD, lineage validation, or specific checks:

```bash
dbt parse                    # Validate YAML and Jinja2
dbt test --fail-fast         # Stop on first failure
dbt run --select state:modified+  # Run modified models + downstream (requires dbt Cloud artifacts)
```

## Troubleshooting

- **Profile not found**: Check `~/.dbt/profiles.yml` or `profiles.yml` in the project root. Ensure the target name matches.
- **Model not found**: Verify the ref is correct: `{{ ref('exact_model_name') }}`. dbt is case-sensitive on some databases.
- **Test failures**: Run `dbt test --select failing_test --debug` for detailed SQL.
- **Circular dependencies**: Use `dbt list --select +model_name` to visualize the lineage and find cycles.

## Environment

- **Python**: Project uses a virtual environment (`venv/`) — activate with `source venv/bin/activate`
- **dbt version**: Specified in `dbt_project.yml` version, check `dbt --version` locally
- **Databricks adapter**: Uses `dbt-databricks` adapter for SQL compilation and execution

---

**Last updated**: 2026-04-08 | **Agent instances organized**: 29 agent configs moved to `.agent-instances/`
