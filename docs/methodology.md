# Methodology

## 1. Separation of Annotation Production and Empirical Analysis

The annotations analysed in this project were generated in a separate software repository (**Australian Parliamentary Annotation**), which manages corpus ingestion, LLM prompting, batch inference, and human review workflows.

This repository (`AuPSA-presentation`) treats the exported canonical dataset (`data/raw/canonical_full_corpus_ai.csv`) strictly as **research data**. No annotation generation or inference takes place within this repository.

## 2. Immutability of Raw Data

The canonical export placed in `data/raw/canonical_full_corpus_ai.csv` (1,498,049,041 bytes; 160,577 rows; 64 columns) is treated as an immutable, read-only archival artifact:
- Raw data files must **never** be manually edited, re-saved in spreadsheet software, or modified in place.
- All schema casting, validation, column selection, and analytical feature derivations occur deterministically in `scripts/01_preprocess.py` using DuckDB directly against the CSV without loading the full corpus into Python memory.
- Every preprocessing run emits `data/processed/preprocessing_manifest.json` recording source and output file byte sizes, SHA-256 digests, row counts, outcome distributions, and column inclusion/exclusion lists.

## 3. Stage 1 Preprocessing Architecture (`scripts/01_preprocess.py`)

`scripts/01_preprocess.py` scans `data/raw/canonical_full_corpus_ai.csv` via DuckDB, verifies corpus integrity invariants, and produces two ZSTD-compressed Parquet files (`ROW_GROUP_SIZE = 10,000`, ordered by `corpus_order`):

1. **`data/processed/hansard_labels.parquet` (52 columns):**
   - Contains corpus and turn identifiers, sitting dates (`speech_date` and derived `year`), chamber, source metadata, speaker metadata, headings and `business_type`, `calculated_word_count`, `warning_codes`, outcome and domain variables (`outcome_status`, `is_non_policy`, derived `is_policy`, `primary_australian_domain`, derived `primary_domain`, `secondary_australian_domains`), lightweight run/campaign/refusal metadata, and human review metadata.
   - Excludes `text_clean`, `text_raw`, `reasoning`, JSON provenance/configuration blobs (`refusal_provenance_json`, `generation_config_json`), and invariant/batch SHA-256 hashes and filesystem paths (`prompt_sha256`, `canonical_schema_sha256`, `prediction_contract_sha256`, `provider_schema_sha256`, `artifact_reference`, `run_artifact_sha256`, `run_record_set_sha256`, `provider_job_id`, `campaign_record_set_sha256`, `campaign_artifact_sha256`).
2. **`data/processed/hansard_analysis.parquet` (53 columns):**
   - Contains the exact 52 columns of `hansard_labels.parquet` plus `text_clean` for text-based and mixed analytical workflows.

### Integrity Validation Rules

Preprocessing fails loudly if any of the following conditions are violated:
- Any required identity, metadata, or outcome column is missing from the source CSV.
- `turn_key` values are duplicated (`unique_turn_count != row_count`) or identity/date fields contain `NULL`.
- `outcome_status` contains `NULL` or any value outside `{'PREDICTION', 'PROVIDER_REFUSAL', 'NO_USABLE_TEXT'}`.
- Policy and domain invariants fail (e.g., `PREDICTION` rows with `NULL` `is_non_policy`, policy predictions missing `primary_australian_domain`, non-policy predictions with a non-null `primary_australian_domain`, or non-prediction rows populated with policy/domain classifications).
- Output Parquet row counts diverge from the source CSV row count.

### Observed Corpus Reconciliation

Across the completed canonical export (`160,577` rows and `160,577` unique `turn_key` values):
- **`PREDICTION`:** `160,567` rows (`117,893` policy turns across `AU01`–`AU14`; `42,674` non-policy turns).
- **`PROVIDER_REFUSAL`:** `8` rows (all with `provider_block_reason = 'PROHIBITED_CONTENT'` and populated `abstention_id` / `abstention_created_at`).
- **`NO_USABLE_TEXT`:** `2` rows (`calculated_word_count = 0`, `warning_codes` containing `"EMPTY_SPEECH_TEXT"`).

## 4. Distinction Between AI Predictions and Human Review Metadata

To preserve analytical transparency:
- **Immutable AI Predictions:** Original model output fields (`outcome_status`, `is_non_policy`, `primary_australian_domain`, `secondary_australian_domains`) are preserved unmutated in both Parquet datasets.
- **Derived Analytical Variables:** Convenience columns (`year`, `is_policy`, `primary_domain`) are added alongside—never replacing—the raw fields. For `PROVIDER_REFUSAL` and `NO_USABLE_TEXT` rows, both `is_policy` and `primary_domain` remain `NULL` so non-predictions are never conflated with non-policy predictions.
- **Human Review Metadata:** Review and codebook fields (`latest_review_status`, `review_edited`, `latest_reviewed_at`, `latest_review_id`, `latest_review_annotation_version_id`, `latest_review_note`, `has_codebook_flag`, `latest_codebook_flag_note`, `latest_codebook_flagged_at`) are stored in dedicated columns and never overwrite the AI predictions.
