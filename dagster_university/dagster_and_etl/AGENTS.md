# Repository Guidelines

## Project Structure & Module Organization
Source lives under `src/dagster_and_etl`, with runnable assets, jobs, resources, schedules, and sensors in `src/dagster_and_etl/defs`. The lightweight entry point for the orchestrator is `src/dagster_and_etl/definitions.py`. Lesson walk-throughs are preserved in `src/dagster_and_etl/completed/lesson_*` for reference. Integration and dataset fixtures sit in `tests/`, alongside docker assets under `tests/postgres/`. Local data artifacts are in `data/`, and the embedded DuckDB file is `csv_pipeline.duckdb`.

## Build, Test, and Development Commands
Install dependencies with `uv sync` (or `pip install -e .[dev]` if you prefer pip). Run Dagster locally using `dagster dev -f src/dagster_and_etl/definitions.py` to inspect assets and launch runs. Execute the asset graph via `dagster job execute -m dagster_and_etl.definitions`. Lint, format, and import-sort in one go with `make ruff`. Run the full test suite with `pytest`; scope to a lesson via `pytest tests/test_lesson_5.py`.

## Coding Style & Naming Conventions
Stick to Python 3.9+ with 4-space indentation. Prefer descriptive asset and resource names (e.g., `nasa_asteroid_assets`). Module paths mirror Dagster conventions: assets in `assets.py`, jobs in `jobs.py`, etc. Ruff enforces formatting, import order, and style; run it before raising a PR. Keep configuration and secrets out of version control—rely on Dagster resources and environment variables instead.

## Testing Guidelines
Tests use Pytest with fixtures in `tests/fixtures.py` and `tests/postgres/`. Name tests after the lesson or feature, e.g., `test_lesson_6_pipeline.py`. Aim to cover both asset materialization logic and external integrations with mocked resources. Before submitting changes, run `pytest` plus any targeted lesson tests you touched. When altering Dagster schedules or sensors, add regression coverage that exercises the new tick behavior.

## Commit & Pull Request Guidelines
Follow the existing history: imperative, capitalized subjects ("Add NASA asteroid data pipeline") and include issue references in parentheses when relevant ("(#141)"). Keep commits focused; split infrastructure, tests, and feature work where possible. For PRs, supply a concise summary, list affected assets/jobs, call out new configs or resources, and attach screenshots of Dagster runs when UI changes are involved. Link to any related documentation updates or follow-up tasks.
