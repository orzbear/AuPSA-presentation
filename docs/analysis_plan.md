# Analysis Plan

This document outlines the three-stage research workflow for analysing and presenting the Australian Hansard AI annotation dataset.

---

## Stage 1: Data Integrity and Preprocessing

**Objective:** Verify the structural integrity of the raw canonical export (`data/raw/canonical_full_corpus_ai.csv`) and transform it into fast, strongly typed analytical tables.

- Inspect exact column names, data types, encoding, and row counts without loading the entire 1.39 GB CSV into memory unnecessarily (leveraging DuckDB / PyArrow).
- Verify uniqueness and completeness of record identifiers.
- Audit missingness, malformed rows, and expected value ranges across metadata and annotation fields.
- Ensure clear separation between immutable AI prediction fields and any human review metadata.
- Document the verified schema in `docs/data_dictionary.md`.
- Implement `scripts/01_preprocess.py` to output:
  - `data/processed/hansard_labels.parquet`
  - `data/processed/hansard_analysis.parquet`
  - `data/processed/preprocessing_manifest.json` (recording input file hash, row counts, and run metadata).

---

## Stage 2: Descriptive Analysis

**Objective:** Characterise the distribution of annotations, speakers, parties, chambers, and policy domains across the corpus.

- Compute baseline corpus statistics (total speeches/utterances, temporal coverage, chamber breakdown, speaker and party distributions).
- Tabulate frequencies and proportions of annotation outcomes and policy/domain categories.
- Examine cross-tabulations of annotation outcomes by time period, government/opposition status, party, and chamber.
- Evaluate model provenance coverage and review metadata distributions where applicable.
- Export reproducible summary tables to `outputs/tables/` and diagnostic/descriptive plots to `outputs/figures/`.

---

## Stage 3: Substantive / Presentation Analysis

**Objective:** Address core research questions for the AuPSA presentation and produce publication- and slide-ready visualisations and tables.

- Conduct substantive empirical analyses linking parliamentary speech attributes and policy domains to annotation outcomes over time.
- Perform robustness and sensitivity checks (e.g., filtering by confidence thresholds, speech length, or procedural vs. substantive debate contexts).
- Generate final, formatted tables and high-resolution figures tailored for conference slides and presentation artifacts in `outputs/presentation/`.

