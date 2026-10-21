# Tests (`tests/`)

This directory will house automated checks for:

- **Schema & Data Integrity:** Verifying that `data/processed/*.parquet` tables conform to expected column names, types, and non-null constraints.
- **Provenance & Invariants:** Ensuring immutable AI prediction columns match raw source counts and are never overwritten by review metadata.
- **Pipeline Reproducibility:** Unit tests for helper functions and transformation logic used in `scripts/`.

