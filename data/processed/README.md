# Processed Data (`data/processed/`)

This directory stores generated, analysis-ready datasets and provenance manifests produced by the preprocessing scripts in `scripts/`.

## Planned Outputs from `01_preprocess.py`

- `hansard_labels.parquet` — Label-focused dataset extracted and typed from the canonical export.
- `hansard_analysis.parquet` — Primary analysis-ready Parquet dataset.
- `preprocessing_manifest.json` — Checksums, row counts, schema metadata, and timestamp of the preprocessing run.

## Conventions

- All files in this directory should be deterministically regenerable from `data/raw/canonical_full_corpus_ai.csv`.
- Data files in this directory are excluded from Git version control.

