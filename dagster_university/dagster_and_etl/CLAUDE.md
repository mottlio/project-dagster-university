# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Dagster ETL training project designed for Dagster University. It demonstrates ETL concepts using Dagster, DuckDB, and various data integration tools.

## Development Commands

### Linting and Formatting
```bash
make ruff                  # Run all ruff checks and formatting
ruff check . --fix        # Fix auto-fixable issues
ruff format .             # Format code
```

### Testing
```bash
pytest                    # Run all tests
pytest tests/test_lesson_3.py  # Run specific lesson tests
```

### Dagster Development
```bash
dagster dev              # Start Dagster development server (runs on localhost:3000)
dagster asset materialize --select <asset_name>  # Materialize specific asset
```

## Architecture

### Core Structure
The project uses Dagster's `load_from_defs_folder` pattern where definitions are automatically loaded from `src/dagster_and_etl/defs/`:

- **definitions.py**: Main entry point using `@definitions` decorator with `load_from_defs_folder`
- **defs/**: Contains all Dagster definitions:
  - **assets.py**: Asset definitions including partitioned assets and asset checks
  - **resources.py**: DuckDB resource configuration
  - **schedules.py**: Schedule definitions for partitioned jobs
  - **sensors.py**: Sensor for dynamic partition detection
  - **jobs.py**: Job definitions (currently empty, jobs are built from assets)

### Key Patterns

1. **Resource Management**: DuckDB is configured as a shared resource in `defs/resources.py`, accessible to all assets via dependency injection.

2. **Partitioned Assets**: The project demonstrates three partition patterns:
   - Static daily partitions (`partitions_def`)
   - Dynamic partitions (`dynamic_partitions_def`) with sensor-based detection

3. **Asset Checks**: Implements blocking asset checks (e.g., `not_empty`) to validate data quality before downstream processing.

4. **Data Flow**: 
   - Source CSV files in `data/source/`
   - Staging DuckDB database at `data/staging/data.duckdb`
   - Assets handle file import → validation → database ingestion

### Completed Lessons
The `src/dagster_and_etl/completed/` directory contains reference implementations for lessons 2-7, useful for understanding advanced patterns and troubleshooting.

## Important Notes

- The project uses `uv` for dependency management (see `pyproject.toml`)
- Tests are in `tests/` with lesson-specific test files
- DuckDB is the primary data storage, configured to persist at `data/staging/data.duckdb`
- File paths are resolved relative to the asset file location using `Path(__file__).parent`