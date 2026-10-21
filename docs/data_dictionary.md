# Data Dictionary

This data dictionary documents all 64 columns observed in `data/raw/canonical_full_corpus_ai.csv` and the 3 derived analytical variables added by `scripts/01_preprocess.py`.

Dataset placement indicators:
- **Labels Parquet:** `data/processed/hansard_labels.parquet` (52 columns)
- **Analysis Parquet:** `data/processed/hansard_analysis.parquet` (53 columns; adds `text_clean`)
- **Archival CSV only:** Retained in `data/raw/canonical_full_corpus_ai.csv` and excluded from processed Parquet tables.

---

## 1. Corpus Metadata

| Field Name | DuckDB / Parquet Type | Included In | Description & Observed Scope |
| :--- | :--- | :--- | :--- |
| `corpus_order` | `BIGINT` | Labels, Analysis | Sequential ordering index across the canonical corpus (`1` to `160577`). |
| `corpus_id` | `VARCHAR` | Labels, Analysis | Corpus UUID (`385a0089-7e50-59c7-bfc6-02ada0e4a139`). |
| `corpus_slug` | `VARCHAR` | Labels, Analysis | Corpus slug identifier (`australian-house-representatives-hansard`). |
| `corpus_name` | `VARCHAR` | Labels, Analysis | Human-readable corpus name (`Australian House of Representatives Hansard`). |
| `turn_id` | `BIGINT` | Labels, Analysis | Unique numeric turn identifier (`1` to `160577`). |
| `turn_key` | `VARCHAR` | Labels, Analysis | Unique deterministic SHA-256 turn key (`160577` unique values). |
| `record_id` | `VARCHAR` | Labels, Analysis | Prefixed record identifier (`turn:<turn_key>`). |
| `source_file` | `VARCHAR` | Labels, Analysis | Relative source XML filename (`YYYY/YYYY-MM-DD.xml`). |
| `source_url` | `VARCHAR` | Labels, Analysis | Source ParlInfo URL for the sitting day transcript. |
| `turn_sequence` | `BIGINT` | Labels, Analysis | Sequence index of the turn within its sitting file. |
| `calculated_word_count` | `BIGINT` | Labels, Analysis | Word count of the cleaned speech turn (`0` to `43471`). |
| `warning_codes` | `VARCHAR` | Labels, Analysis | JSON-encoded list of ingestion/parsing warning codes (e.g., `[]`, `["UNKNOWN_BUSINESS_HEADING"]`, `["EMPTY_SPEECH_TEXT"]`). |
| `text_clean` | `VARCHAR` | Analysis only | Cleaned speech turn text used for annotation and text analysis. Excluded from `hansard_labels.parquet`. |
| `text_raw` | `VARCHAR` | Archival CSV only | Uncleaned raw transcript text from source XML. Excluded from both Parquet files. |

---

## 2. Parliamentary Metadata

| Field Name | DuckDB / Parquet Type | Included In | Description & Observed Scope |
| :--- | :--- | :--- | :--- |
| `speech_date` | `DATE` | Labels, Analysis | Date of parliamentary sitting (`2010-02-02` to `2025-10-29`). |
| `chamber` | `VARCHAR` | Labels, Analysis | Parliamentary chamber (`Australian Federal House of Representatives`). |
| `speaker_id` | `VARCHAR` | Labels, Analysis | Speaker identifier from source XML (e.g., `uk.org.publicwhip/member/450`, `unknown`, or `NULL`). |
| `speaker_name` | `VARCHAR` | Labels, Analysis | Speaker display name (or `NULL` when absent in source). |
| `major_heading` | `VARCHAR` | Labels, Analysis | Major debate or proceeding heading from Hansard transcript. |
| `minor_heading` | `VARCHAR` | Labels, Analysis | Minor heading or sub-topic from Hansard transcript (`NULL` when absent). |
| `business_type` | `VARCHAR` | Labels, Analysis | Normalised parliamentary business category (e.g., `adjournment`, `members_sworn`, `statements_on_significant_matters`, or `NULL`). |

---

## 3. Annotation Outcomes

| Field Name | DuckDB / Parquet Type | Included In | Description & Observed Scope |
| :--- | :--- | :--- | :--- |
| `outcome_status` | `VARCHAR` | Labels, Analysis | Pipeline outcome status for the turn: `PREDICTION` (`160,567`), `PROVIDER_REFUSAL` (`8`), or `NO_USABLE_TEXT` (`2`). |
| `is_non_policy` | `BOOLEAN` | Labels, Analysis | Raw boolean model output indicating non-policy/procedural speech (`true` = `42,674`; `false` = `117,893`; `NULL` for `PROVIDER_REFUSAL` and `NO_USABLE_TEXT`). |
| `prediction_id` | `VARCHAR` | Labels, Analysis | Unique UUID for the model prediction record (`NULL` when `outcome_status != 'PREDICTION'`). |
| `abstention_id` | `VARCHAR` | Labels, Analysis | Unique UUID for provider refusal/abstention records (`Non-null` for `PROVIDER_REFUSAL` rows, `NULL` otherwise). |
| `provider_block_reason` | `VARCHAR` | Labels, Analysis | Provider refusal reason code (`PROHIBITED_CONTENT` for the 8 `PROVIDER_REFUSAL` rows, `NULL` otherwise). |
| `reasoning` | `VARCHAR` | Archival CSV only | Model reasoning text for the classification. Excluded from both Parquet files. |
| `refusal_provenance_json` | `VARCHAR` | Archival CSV only | Raw JSON provenance payload for provider refusals. Excluded from both Parquet files. |

---

## 4. Policy / Domain Variables

| Field Name | DuckDB / Parquet Type | Included In | Description & Observed Scope |
| :--- | :--- | :--- | :--- |
| `primary_australian_domain` | `VARCHAR` | Labels, Analysis | Raw primary Australian domain code (`AU01` through `AU14` for the `117,893` policy predictions; `NULL` for non-policy predictions, refusals, and no-usable-text turns). |
| `secondary_australian_domains` | `VARCHAR` | Labels, Analysis | JSON-encoded list of secondary Australian domain codes (e.g., `[]`, `["AU04"]`, or `NULL` for non-prediction rows). |

---

## 5. Model Provenance

| Field Name | DuckDB / Parquet Type | Included In | Description & Observed Scope |
| :--- | :--- | :--- | :--- |
| `provider` | `VARCHAR` | Labels, Analysis | Inference provider identifier (`gemini`). |
| `configured_model` | `VARCHAR` | Labels, Analysis | Configured model identifier (`gemini-3.1-flash-lite`). |
| `returned_model` | `VARCHAR` | Labels, Analysis | Returned model identifier (`gemini-3.1-flash-lite`). |
| `prompt_version` | `VARCHAR` | Labels, Analysis | Prompt version identifier (`phase4a-policy-v2`). |
| `thinking_level` | `VARCHAR` | Labels, Analysis | Configured model thinking level (`minimal`). |
| `execution_mode` | `VARCHAR` | Labels, Analysis | Inference execution mode (`batch`). |
| `prediction_run_id` | `VARCHAR` | Labels, Analysis | UUID of the prediction run (`4` distinct run IDs; `NULL` for `NO_USABLE_TEXT`). |
| `run_imported_at` | `TIMESTAMPTZ` | Labels, Analysis | Timestamp when the prediction run was imported (`NULL` for `NO_USABLE_TEXT`). |
| `prediction_created_at` | `TIMESTAMPTZ` | Labels, Analysis | Timestamp when the prediction record was created (`NULL` for `PROVIDER_REFUSAL` and `NO_USABLE_TEXT`). |
| `abstention_created_at` | `TIMESTAMPTZ` | Labels, Analysis | Timestamp when the abstention record was created (`Non-null` for `PROVIDER_REFUSAL`, `NULL` otherwise). |
| `campaign_id` | `VARCHAR` | Labels, Analysis | Annotation campaign UUID (`4` distinct campaign IDs; `NULL` for `NO_USABLE_TEXT`). |
| `campaign_name` | `VARCHAR` | Labels, Analysis | Human-readable campaign name (`NULL` for `NO_USABLE_TEXT`). |
| `campaign_status` | `VARCHAR` | Labels, Analysis | Campaign lifecycle status (`IMPORTED`, or `NULL` for `NO_USABLE_TEXT`). |
| `campaign_imported_at` | `TIMESTAMPTZ` | Labels, Analysis | Timestamp when the campaign was imported (`NULL` for `NO_USABLE_TEXT`). |
| `prompt_sha256` | `VARCHAR` | Archival CSV only | Invariant SHA-256 digest of the prompt template. Excluded from Parquet outputs. |
| `canonical_schema_sha256` | `VARCHAR` | Archival CSV only | Invariant SHA-256 digest of the canonical output schema. Excluded from Parquet outputs. |
| `prediction_contract_sha256` | `VARCHAR` | Archival CSV only | Invariant SHA-256 digest of the prediction contract. Excluded from Parquet outputs. |
| `provider_schema_sha256` | `VARCHAR` | Archival CSV only | Invariant SHA-256 digest of the provider schema. Excluded from Parquet outputs. |
| `generation_config_json` | `VARCHAR` | Archival CSV only | JSON blob of model generation configuration. Excluded from Parquet outputs. |
| `artifact_reference` | `VARCHAR` | Archival CSV only | Server filesystem path to the campaign batch artifact. Excluded from Parquet outputs. |
| `run_artifact_sha256` | `VARCHAR` | Archival CSV only | SHA-256 digest of the run artifact file. Excluded from Parquet outputs. |
| `run_record_set_sha256` | `VARCHAR` | Archival CSV only | SHA-256 digest of the run record set. Excluded from Parquet outputs. |
| `provider_job_id` | `VARCHAR` | Archival CSV only | Provider batch job identifier. Excluded from Parquet outputs. |
| `campaign_record_set_sha256` | `VARCHAR` | Archival CSV only | SHA-256 digest of the campaign record set. Excluded from Parquet outputs. |
| `campaign_artifact_sha256` | `VARCHAR` | Archival CSV only | SHA-256 digest of the campaign artifact. Excluded from Parquet outputs. |

---

## 6. Review Metadata

| Field Name | DuckDB / Parquet Type | Included In | Description & Observed Scope |
| :--- | :--- | :--- | :--- |
| `latest_review_status` | `VARCHAR` | Labels, Analysis | Status of latest human review (`accepted` for `3` rows; `NULL` for `160,574` rows). |
| `review_edited` | `BOOLEAN` | Labels, Analysis | Boolean indicating whether human review modified the annotation (`false` across all `160,577` rows). |
| `latest_reviewed_at` | `TIMESTAMPTZ` | Labels, Analysis | Timestamp of the latest human review (`Non-null` for `3` rows; `NULL` otherwise). |
| `latest_review_id` | `VARCHAR` | Labels, Analysis | UUID of the latest human review record (`Non-null` for `3` rows; `NULL` otherwise). |
| `latest_review_annotation_version_id` | `VARCHAR` | Labels, Analysis | Annotation version identifier from latest human review (`251`, `252`, `253` for the `3` reviewed rows; `NULL` otherwise). |
| `latest_review_note` | `VARCHAR` | Labels, Analysis | Free-text note attached to human review (`NULL` across all `160,577` rows). |
| `has_codebook_flag` | `BOOLEAN` | Labels, Analysis | Boolean flag indicating whether a turn was flagged for codebook review (`false` across all `160,577` rows). |
| `latest_codebook_flag_note` | `VARCHAR` | Labels, Analysis | Free-text note for codebook flag (`NULL` across all `160,577` rows). |
| `latest_codebook_flagged_at` | `TIMESTAMPTZ` | Labels, Analysis | Timestamp when codebook flag was created (`NULL` across all `160,577` rows). |

---

## 7. Derived Analytical Variables

| Field Name | DuckDB / Parquet Type | Source Script | Derivation Logic & Scope |
| :--- | :--- | :--- | :--- |
| `year` | `INTEGER` | `scripts/01_preprocess.py` | Calendar year extracted from `speech_date` via `CAST(EXTRACT(YEAR FROM speech_date) AS INTEGER)` (`2010` to `2025`). |
| `is_policy` | `BOOLEAN` | `scripts/01_preprocess.py` | `NOT is_non_policy` when `outcome_status = 'PREDICTION'` (`true` for `117,893` policy predictions; `false` for `42,674` non-policy predictions); `NULL` (`10` rows) when `outcome_status IN ('PROVIDER_REFUSAL', 'NO_USABLE_TEXT')`. |
| `primary_domain` | `VARCHAR` | `scripts/01_preprocess.py` | `primary_australian_domain` (`AU01`–`AU14`) when `outcome_status = 'PREDICTION' AND is_non_policy = FALSE` (`117,893` rows); `'NON_POLICY'` when `outcome_status = 'PREDICTION' AND is_non_policy = TRUE` (`42,674` rows); `NULL` (`10` rows) when `outcome_status IN ('PROVIDER_REFUSAL', 'NO_USABLE_TEXT')`. |
