# AuPSA-presentation

Research-analysis and presentation repository for the Australian Hansard AI annotation dataset.

## Purpose & Relationship to the Annotation Repository

This repository is dedicated to the statistical analysis, validation, and presentation of the completed Australian Hansard AI annotation dataset.

It is strictly separate from the **Australian Parliamentary Annotation** software repository:
- **Annotation software repository:** Handles corpus ingestion, prompt engineering, LLM inference pipelines, and human review interfaces.
- **`AuPSA-presentation` (this repository):** Treats the completed, exported annotations as immutable research data for reproducible empirical analysis and conference presentation.

## Reproducibility Principles

1. **Read-Only Raw Data:** Raw exports placed in `data/raw/` are never modified in place or manually edited.
2. **Scripted Pipeline:** All data transformations, summary tables, and figures must be generated deterministically by numbered scripts in `scripts/`.
3. **Exploratory Separation:** Jupyter notebooks in `notebooks/` are for interactive exploration only; final results and presentation artifacts must be produced by standalone scripts.
4. **No Large Data in Git:** The source dataset (~1.39 GB) and derived analytical datasets are excluded from version control via `.gitignore`. Only code, documentation, and lightweight directory placeholders are tracked.

## Directory Structure

```text
AuPSA-presentation/
├── README.md
├── .gitignore
├── requirements.txt
├── data/
│   ├── raw/            # Untouched canonical export from the annotation system
│   ├── processed/      # Generated analysis-ready datasets (primarily Parquet) & manifests
│   └── external/       # Supplementary external datasets
├── scripts/            # Reproducible, numbered preprocessing and analysis scripts
├── notebooks/          # Exploratory notebooks only (non-authoritative for final outputs)
├── outputs/
│   ├── tables/         # Generated analytical and summary tables
│   ├── figures/        # Generated plots and visualisations
│   └── presentation/   # Final conference and presentation-ready artifacts
├── docs/
│   ├── analysis_plan.md
│   ├── data_dictionary.md
│   └── methodology.md
└── tests/              # Data integrity and pipeline unit tests
```

## Canonical Data Placement

Place the untouched canonical export from the annotation system at:

```text
data/raw/canonical_full_corpus_ai.csv
```

*(Note: This file is approximately 1.39 GB and is ignored by Git.)*

## Planned Processing Flow

```text
data/raw/canonical_full_corpus_ai.csv
    │
    ▼
scripts/01_preprocess.py
    │
    ├──► data/processed/hansard_labels.parquet
    ├──► data/processed/hansard_analysis.parquet
    └──► data/processed/preprocessing_manifest.json
            │
            ▼
    descriptive / statistical analysis scripts
            │
            ├──► outputs/tables/
            ├──► outputs/figures/
            └──► outputs/presentation/
```

## Environment Setup

Create a virtual environment and install the core dependencies:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

