"""Stage 1 preprocessing for the canonical Australian Hansard AI annotation dataset.

Scans `data/raw/canonical_full_corpus_ai.csv` via DuckDB without loading the full
corpus into pandas memory, validates corpus and outcome integrity invariants, and writes:
  - `data/processed/hansard_labels.parquet` (compact analytical & metadata table)
  - `data/processed/hansard_analysis.parquet` (analytical table + `text_clean`)
  - `data/processed/preprocessing_manifest.json` (provenance, checksums, and counts)
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import time
from typing import Any

import duckdb

PREPROCESSING_VERSION = "1.0.0"
DEFAULT_ROW_GROUP_SIZE = 10_000

ALLOWED_OUTCOME_STATUSES = ("PREDICTION", "PROVIDER_REFUSAL", "NO_USABLE_TEXT")

# Required columns that must exist in any valid canonical export CSV
REQUIRED_SOURCE_COLUMNS: tuple[str, ...] = (
    "corpus_order",
    "corpus_id",
    "corpus_slug",
    "corpus_name",
    "turn_id",
    "turn_key",
    "record_id",
    "speech_date",
    "chamber",
    "source_file",
    "source_url",
    "turn_sequence",
    "speaker_id",
    "speaker_name",
    "major_heading",
    "minor_heading",
    "business_type",
    "calculated_word_count",
    "warning_codes",
    "text_clean",
    "outcome_status",
    "is_non_policy",
    "primary_australian_domain",
    "secondary_australian_domains",
    "prediction_id",
    "abstention_id",
    "provider_block_reason",
    "provider",
    "configured_model",
    "returned_model",
    "prompt_version",
    "thinking_level",
    "execution_mode",
    "prediction_run_id",
    "run_imported_at",
    "prediction_created_at",
    "abstention_created_at",
    "campaign_id",
    "campaign_name",
    "campaign_status",
    "campaign_imported_at",
    "latest_review_status",
    "review_edited",
    "latest_reviewed_at",
    "latest_review_id",
    "latest_review_annotation_version_id",
    "latest_review_note",
    "has_codebook_flag",
    "latest_codebook_flag_note",
    "latest_codebook_flagged_at",
)

# Heavy / archival / redundant-hash fields intentionally excluded from both Parquet files
EXCLUDED_FROM_BOTH: tuple[str, ...] = (
    "text_raw",
    "reasoning",
    "refusal_provenance_json",
    "generation_config_json",
    "prompt_sha256",
    "canonical_schema_sha256",
    "prediction_contract_sha256",
    "provider_schema_sha256",
    "artifact_reference",
    "run_artifact_sha256",
    "run_record_set_sha256",
    "provider_job_id",
    "campaign_record_set_sha256",
    "campaign_artifact_sha256",
)

# Excluded from hansard_labels.parquet, but retained in hansard_analysis.parquet
EXCLUDED_FROM_LABELS_ONLY: tuple[str, ...] = ("text_clean",)

# Output columns in hansard_labels.parquet (in exact schema order)
LABELS_OUTPUT_COLUMNS: tuple[str, ...] = (
    "corpus_order",
    "corpus_id",
    "corpus_slug",
    "corpus_name",
    "turn_id",
    "turn_key",
    "record_id",
    "speech_date",
    "year",
    "chamber",
    "source_file",
    "source_url",
    "turn_sequence",
    "speaker_id",
    "speaker_name",
    "major_heading",
    "minor_heading",
    "business_type",
    "calculated_word_count",
    "warning_codes",
    "outcome_status",
    "is_non_policy",
    "is_policy",
    "primary_australian_domain",
    "primary_domain",
    "secondary_australian_domains",
    "prediction_id",
    "abstention_id",
    "provider_block_reason",
    "provider",
    "configured_model",
    "returned_model",
    "prompt_version",
    "thinking_level",
    "execution_mode",
    "prediction_run_id",
    "run_imported_at",
    "prediction_created_at",
    "abstention_created_at",
    "campaign_id",
    "campaign_name",
    "campaign_status",
    "campaign_imported_at",
    "latest_review_status",
    "review_edited",
    "latest_reviewed_at",
    "latest_review_id",
    "latest_review_annotation_version_id",
    "latest_review_note",
    "has_codebook_flag",
    "latest_codebook_flag_note",
    "latest_codebook_flagged_at",
)

# Output columns in hansard_analysis.parquet (same as labels + text_clean)
ANALYSIS_OUTPUT_COLUMNS: tuple[str, ...] = (*LABELS_OUTPUT_COLUMNS, "text_clean")


def compute_sha256(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    """Compute the SHA-256 hex digest of a file using streaming reads."""
    hasher = hashlib.sha256()
    with path.open("rb") as fh:
        while chunk := fh.read(chunk_size):
            hasher.update(chunk)
    return hasher.hexdigest()


def _sql_quote_path(path: Path) -> str:
    """Return a single-quoted SQL string literal for a filesystem path."""
    escaped = str(path.resolve()).replace("'", "''")
    return f"'{escaped}'"


def _build_projection_sql(include_text_clean: bool) -> str:
    """Build the SQL projection list for labels or analysis datasets."""
    base_select = """
        CAST(corpus_order AS BIGINT) AS corpus_order,
        CAST(corpus_id AS VARCHAR) AS corpus_id,
        CAST(corpus_slug AS VARCHAR) AS corpus_slug,
        CAST(corpus_name AS VARCHAR) AS corpus_name,
        CAST(turn_id AS BIGINT) AS turn_id,
        CAST(turn_key AS VARCHAR) AS turn_key,
        CAST(record_id AS VARCHAR) AS record_id,
        CAST(speech_date AS DATE) AS speech_date,
        CAST(EXTRACT(YEAR FROM CAST(speech_date AS DATE)) AS INTEGER) AS year,
        CAST(chamber AS VARCHAR) AS chamber,
        CAST(source_file AS VARCHAR) AS source_file,
        CAST(source_url AS VARCHAR) AS source_url,
        CAST(turn_sequence AS BIGINT) AS turn_sequence,
        CAST(speaker_id AS VARCHAR) AS speaker_id,
        CAST(speaker_name AS VARCHAR) AS speaker_name,
        CAST(major_heading AS VARCHAR) AS major_heading,
        CAST(minor_heading AS VARCHAR) AS minor_heading,
        CAST(business_type AS VARCHAR) AS business_type,
        CAST(calculated_word_count AS BIGINT) AS calculated_word_count,
        CAST(warning_codes AS VARCHAR) AS warning_codes,
        CAST(outcome_status AS VARCHAR) AS outcome_status,
        CAST(is_non_policy AS BOOLEAN) AS is_non_policy,
        CASE
            WHEN outcome_status = 'PREDICTION' AND CAST(is_non_policy AS BOOLEAN) IS NOT NULL
            THEN NOT CAST(is_non_policy AS BOOLEAN)
            ELSE NULL
        END AS is_policy,
        CAST(primary_australian_domain AS VARCHAR) AS primary_australian_domain,
        CASE
            WHEN outcome_status = 'PREDICTION' AND CAST(is_non_policy AS BOOLEAN) = FALSE
            THEN CAST(primary_australian_domain AS VARCHAR)
            WHEN outcome_status = 'PREDICTION' AND CAST(is_non_policy AS BOOLEAN) = TRUE
            THEN 'NON_POLICY'
            ELSE NULL
        END AS primary_domain,
        CAST(secondary_australian_domains AS VARCHAR) AS secondary_australian_domains,
        CAST(prediction_id AS VARCHAR) AS prediction_id,
        CAST(abstention_id AS VARCHAR) AS abstention_id,
        CAST(provider_block_reason AS VARCHAR) AS provider_block_reason,
        CAST(provider AS VARCHAR) AS provider,
        CAST(configured_model AS VARCHAR) AS configured_model,
        CAST(returned_model AS VARCHAR) AS returned_model,
        CAST(prompt_version AS VARCHAR) AS prompt_version,
        CAST(thinking_level AS VARCHAR) AS thinking_level,
        CAST(execution_mode AS VARCHAR) AS execution_mode,
        CAST(prediction_run_id AS VARCHAR) AS prediction_run_id,
        CAST(run_imported_at AS TIMESTAMPTZ) AS run_imported_at,
        CAST(prediction_created_at AS TIMESTAMPTZ) AS prediction_created_at,
        CAST(abstention_created_at AS TIMESTAMPTZ) AS abstention_created_at,
        CAST(campaign_id AS VARCHAR) AS campaign_id,
        CAST(campaign_name AS VARCHAR) AS campaign_name,
        CAST(campaign_status AS VARCHAR) AS campaign_status,
        CAST(campaign_imported_at AS TIMESTAMPTZ) AS campaign_imported_at,
        CAST(latest_review_status AS VARCHAR) AS latest_review_status,
        CAST(review_edited AS BOOLEAN) AS review_edited,
        CAST(latest_reviewed_at AS TIMESTAMPTZ) AS latest_reviewed_at,
        CAST(latest_review_id AS VARCHAR) AS latest_review_id,
        CAST(latest_review_annotation_version_id AS VARCHAR) AS latest_review_annotation_version_id,
        CAST(latest_review_note AS VARCHAR) AS latest_review_note,
        CAST(has_codebook_flag AS BOOLEAN) AS has_codebook_flag,
        CAST(latest_codebook_flag_note AS VARCHAR) AS latest_codebook_flag_note,
        CAST(latest_codebook_flagged_at AS TIMESTAMPTZ) AS latest_codebook_flagged_at
    """
    if include_text_clean:
        return f"{base_select},\n        CAST(text_clean AS VARCHAR) AS text_clean"
    return base_select


def inspect_csv_schema(
    con: duckdb.DuckDBPyConnection, csv_path: Path
) -> list[tuple[str, str]]:
    """Inspect and return `(column_name, column_type)` pairs from the CSV."""
    quoted_csv = _sql_quote_path(csv_path)
    rel = con.sql(f"SELECT * FROM read_csv({quoted_csv}, auto_detect=true, header=true) LIMIT 0")
    return [(col, str(dtype)) for col, dtype in zip(rel.columns, rel.dtypes)]


def validate_required_columns(found_columns: list[str]) -> None:
    """Fail loudly if any required identity, outcome, or metadata column is missing."""
    found_set = set(found_columns)
    missing = [col for col in REQUIRED_SOURCE_COLUMNS if col not in found_set]
    if missing:
        raise ValueError(
            f"Missing required column(s) in source CSV: {', '.join(missing)}"
        )


def _display_path(path: Path, repo_root: Path | None = None) -> str:
    """Format a path relative to the repository root using POSIX slashes when possible."""
    if repo_root is not None:
        try:
            return path.resolve().relative_to(repo_root.resolve()).as_posix()
        except ValueError:
            pass
    return path.as_posix()


def run_preprocessing(
    source_csv: Path,
    labels_parquet: Path,
    analysis_parquet: Path,
    manifest_json: Path,
    *,
    row_group_size: int = DEFAULT_ROW_GROUP_SIZE,
    repo_root: Path | None = None,
    verbose: bool = True,
) -> dict[str, Any]:
    """Execute Stage 1 preprocessing using DuckDB and return the manifest dictionary."""
    t_start = time.perf_counter()

    if not source_csv.exists():
        raise FileNotFoundError(f"Canonical source CSV not found: {source_csv}")

    labels_parquet.parent.mkdir(parents=True, exist_ok=True)
    analysis_parquet.parent.mkdir(parents=True, exist_ok=True)
    manifest_json.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(database=":memory:")
    try:
        if verbose:
            print(f"[1/6] Inspecting CSV schema: {source_csv}")
        schema_pairs = inspect_csv_schema(con, source_csv)
        found_columns = [col for col, _ in schema_pairs]
        if verbose:
            print(f"      Found {len(found_columns)} columns in source CSV.")
        validate_required_columns(found_columns)

        quoted_csv = _sql_quote_path(source_csv)

        if verbose:
            print("[2/6] Validating source data integrity and outcome invariants via DuckDB...")
        integrity_row = con.sql(f"""
            SELECT
                COUNT(*) AS total_rows,
                COUNT(DISTINCT turn_key) AS unique_turn_keys,
                COUNT(*) FILTER (WHERE turn_key IS NULL OR TRIM(CAST(turn_key AS VARCHAR)) = '') AS null_turn_keys,
                COUNT(*) FILTER (WHERE turn_id IS NULL) AS null_turn_ids,
                COUNT(*) FILTER (WHERE corpus_order IS NULL) AS null_corpus_orders,
                COUNT(*) FILTER (WHERE speech_date IS NULL) AS null_speech_dates,
                COUNT(*) FILTER (WHERE outcome_status = 'PREDICTION') AS n_prediction,
                COUNT(*) FILTER (WHERE outcome_status = 'PROVIDER_REFUSAL') AS n_provider_refusal,
                COUNT(*) FILTER (WHERE outcome_status = 'NO_USABLE_TEXT') AS n_no_usable_text,
                COUNT(*) FILTER (
                    WHERE outcome_status IS NULL
                       OR outcome_status NOT IN ('PREDICTION', 'PROVIDER_REFUSAL', 'NO_USABLE_TEXT')
                ) AS n_unexpected_outcome,
                COUNT(*) FILTER (
                    WHERE outcome_status = 'PREDICTION' AND CAST(is_non_policy AS BOOLEAN) IS NULL
                ) AS n_prediction_null_policy_flag,
                COUNT(*) FILTER (
                    WHERE outcome_status = 'PREDICTION'
                      AND CAST(is_non_policy AS BOOLEAN) = FALSE
                      AND (primary_australian_domain IS NULL OR TRIM(CAST(primary_australian_domain AS VARCHAR)) = '')
                ) AS n_policy_missing_domain,
                COUNT(*) FILTER (
                    WHERE outcome_status = 'PREDICTION'
                      AND CAST(is_non_policy AS BOOLEAN) = TRUE
                      AND primary_australian_domain IS NOT NULL
                      AND TRIM(CAST(primary_australian_domain AS VARCHAR)) != ''
                ) AS n_non_policy_with_domain,
                COUNT(*) FILTER (
                    WHERE outcome_status != 'PREDICTION'
                      AND (
                          CAST(is_non_policy AS BOOLEAN) IS NOT NULL
                          OR (primary_australian_domain IS NOT NULL AND TRIM(CAST(primary_australian_domain AS VARCHAR)) != '')
                      )
                ) AS n_non_prediction_with_policy_or_domain
            FROM read_csv({quoted_csv}, auto_detect=true, header=true)
        """).fetchone()

        assert integrity_row is not None
        (
            total_rows,
            unique_turn_keys,
            null_turn_keys,
            null_turn_ids,
            null_corpus_orders,
            null_speech_dates,
            n_prediction,
            n_provider_refusal,
            n_no_usable_text,
            n_unexpected_outcome,
            n_prediction_null_policy_flag,
            n_policy_missing_domain,
            n_non_policy_with_domain,
            n_non_prediction_with_policy_or_domain,
        ) = integrity_row

        if total_rows == 0:
            raise ValueError("Source CSV contains 0 data rows.")

        if null_turn_keys > 0 or null_turn_ids > 0 or null_corpus_orders > 0 or null_speech_dates > 0:
            raise ValueError(
                f"Null identity/date values detected: null_turn_keys={null_turn_keys}, "
                f"null_turn_ids={null_turn_ids}, null_corpus_orders={null_corpus_orders}, "
                f"null_speech_dates={null_speech_dates}"
            )

        duplicate_count = int(total_rows - unique_turn_keys)
        if duplicate_count != 0:
            raise ValueError(
                f"Duplicate turn_key values detected: total_rows={total_rows}, "
                f"unique_turn_keys={unique_turn_keys}, duplicates={duplicate_count}"
            )

        if n_unexpected_outcome > 0:
            bad_statuses = con.sql(f"""
                SELECT DISTINCT outcome_status
                FROM read_csv({quoted_csv}, auto_detect=true, header=true)
                WHERE outcome_status IS NULL
                   OR outcome_status NOT IN ('PREDICTION', 'PROVIDER_REFUSAL', 'NO_USABLE_TEXT')
            """).fetchall()
            raise ValueError(
                f"Unexpected outcome_status value(s) detected ({n_unexpected_outcome} rows): "
                f"{[r[0] for r in bad_statuses]}"
            )

        if n_prediction_null_policy_flag > 0:
            raise ValueError(
                f"Found {n_prediction_null_policy_flag} PREDICTION row(s) with NULL is_non_policy."
            )

        if n_policy_missing_domain > 0:
            raise ValueError(
                f"Found {n_policy_missing_domain} policy PREDICTION row(s) missing primary_australian_domain."
            )

        if n_non_policy_with_domain > 0:
            raise ValueError(
                f"Found {n_non_policy_with_domain} non-policy PREDICTION row(s) with non-null primary_australian_domain."
            )

        if n_non_prediction_with_policy_or_domain > 0:
            raise ValueError(
                f"Found {n_non_prediction_with_policy_or_domain} non-PREDICTION row(s) with policy/domain values."
            )

        # Write hansard_labels.parquet
        if verbose:
            print(
                f"[3/6] Writing labels dataset (ZSTD, row_group_size={row_group_size}): "
                f"{labels_parquet}"
            )
        labels_select_sql = _build_projection_sql(include_text_clean=False)
        quoted_labels = _sql_quote_path(labels_parquet)
        con.execute(f"""
            COPY (
                SELECT {labels_select_sql}
                FROM read_csv({quoted_csv}, auto_detect=true, header=true)
                ORDER BY CAST(corpus_order AS BIGINT)
            ) TO {quoted_labels} (
                FORMAT PARQUET,
                COMPRESSION ZSTD,
                ROW_GROUP_SIZE {int(row_group_size)}
            )
        """)

        # Write hansard_analysis.parquet
        if verbose:
            print(
                f"[4/6] Writing analysis dataset (ZSTD, row_group_size={row_group_size}): "
                f"{analysis_parquet}"
            )
        analysis_select_sql = _build_projection_sql(include_text_clean=True)
        quoted_analysis = _sql_quote_path(analysis_parquet)
        con.execute(f"""
            COPY (
                SELECT {analysis_select_sql}
                FROM read_csv({quoted_csv}, auto_detect=true, header=true)
                ORDER BY CAST(corpus_order AS BIGINT)
            ) TO {quoted_analysis} (
                FORMAT PARQUET,
                COMPRESSION ZSTD,
                ROW_GROUP_SIZE {int(row_group_size)}
            )
        """)

        # Verify written Parquet files
        if verbose:
            print("[5/6] Verifying output Parquet schemas and row count reconciliation...")
        labels_verify = con.sql(f"""
            SELECT
                COUNT(*) AS n_rows,
                COUNT(DISTINCT turn_key) AS n_unique_keys,
                COUNT(*) FILTER (WHERE is_policy = TRUE) AS n_policy,
                COUNT(*) FILTER (WHERE is_policy = FALSE) AS n_non_policy,
                COUNT(*) FILTER (WHERE is_policy IS NULL) AS n_policy_null,
                COUNT(*) FILTER (WHERE primary_domain = 'NON_POLICY') AS n_domain_non_policy,
                COUNT(*) FILTER (WHERE primary_domain IS NOT NULL AND primary_domain != 'NON_POLICY') AS n_domain_policy,
                COUNT(*) FILTER (WHERE primary_domain IS NULL) AS n_domain_null
            FROM read_parquet({quoted_labels})
        """).fetchone()
        analysis_verify = con.sql(f"""
            SELECT
                COUNT(*) AS n_rows,
                COUNT(DISTINCT turn_key) AS n_unique_keys
            FROM read_parquet({quoted_analysis})
        """).fetchone()

        assert labels_verify is not None and analysis_verify is not None
        labels_rows = int(labels_verify[0])
        analysis_rows = int(analysis_verify[0])

        if labels_rows != total_rows or analysis_rows != total_rows:
            raise RuntimeError(
                f"Row count mismatch after preprocessing: source={total_rows}, "
                f"labels={labels_rows}, analysis={analysis_rows}"
            )

        labels_cols = con.sql(f"SELECT * FROM read_parquet({quoted_labels}) LIMIT 0").columns
        analysis_cols = con.sql(f"SELECT * FROM read_parquet({quoted_analysis}) LIMIT 0").columns

        if tuple(labels_cols) != LABELS_OUTPUT_COLUMNS:
            raise RuntimeError(
                f"Unexpected columns in hansard_labels.parquet: {labels_cols}"
            )
        if tuple(analysis_cols) != ANALYSIS_OUTPUT_COLUMNS:
            raise RuntimeError(
                f"Unexpected columns in hansard_analysis.parquet: {analysis_cols}"
            )
    finally:
        con.close()

    if verbose:
        print("[6/6] Computing SHA-256 digests and writing preprocessing_manifest.json...")

    source_size = source_csv.stat().st_size
    labels_size = labels_parquet.stat().st_size
    analysis_size = analysis_parquet.stat().st_size

    source_sha256 = compute_sha256(source_csv)
    labels_sha256 = compute_sha256(labels_parquet)
    analysis_sha256 = compute_sha256(analysis_parquet)

    outcome_counts = {
        "PREDICTION": int(n_prediction),
        "PROVIDER_REFUSAL": int(n_provider_refusal),
        "NO_USABLE_TEXT": int(n_no_usable_text),
    }

    derived_counts = {
        "is_policy_true": int(labels_verify[2]),
        "is_policy_false": int(labels_verify[3]),
        "is_policy_null": int(labels_verify[4]),
        "primary_domain_policy": int(labels_verify[6]),
        "primary_domain_non_policy": int(labels_verify[5]),
        "primary_domain_null": int(labels_verify[7]),
    }

    excluded_present_in_source = [
        col for col in (*EXCLUDED_FROM_BOTH, *EXCLUDED_FROM_LABELS_ONLY) if col in found_columns
    ]

    manifest: dict[str, Any] = {
        "preprocessing_version": PREPROCESSING_VERSION,
        "generated_timestamp": datetime.now(timezone.utc).isoformat(),
        "source_filename": _display_path(source_csv, repo_root),
        "source_size": source_size,
        "source_size_bytes": source_size,
        "source_sha256": source_sha256,
        "row_count": int(total_rows),
        "unique_turn_count": int(unique_turn_keys),
        "duplicate_count": int(duplicate_count),
        "outcome_counts": outcome_counts,
        "derived_counts": derived_counts,
        "output_filenames": {
            "hansard_labels.parquet": _display_path(labels_parquet, repo_root),
            "hansard_analysis.parquet": _display_path(analysis_parquet, repo_root),
            "preprocessing_manifest.json": _display_path(manifest_json, repo_root),
        },
        "output_byte_sizes": {
            "hansard_labels.parquet": labels_size,
            "hansard_analysis.parquet": analysis_size,
        },
        "output_sha256_hashes": {
            "hansard_labels.parquet": labels_sha256,
            "hansard_analysis.parquet": analysis_sha256,
        },
        "included_columns": {
            "hansard_labels.parquet": list(LABELS_OUTPUT_COLUMNS),
            "hansard_analysis.parquet": list(ANALYSIS_OUTPUT_COLUMNS),
        },
        "excluded_heavy_fields": {
            "excluded_from_both": list(EXCLUDED_FROM_BOTH),
            "excluded_from_labels_only": list(EXCLUDED_FROM_LABELS_ONLY),
            "observed_in_source_csv": excluded_present_in_source,
        },
        "parquet_settings": {
            "compression": "ZSTD",
            "row_group_size": int(row_group_size),
            "ordered_by": "corpus_order",
        },
        "transformation_notes": [
            "Derived `year` as INTEGER from `EXTRACT(YEAR FROM speech_date)`.",
            "Derived `is_policy` as `NOT is_non_policy` where `outcome_status = 'PREDICTION'`, and `NULL` for `PROVIDER_REFUSAL` and `NO_USABLE_TEXT`.",
            "Derived `primary_domain` as `primary_australian_domain` where `outcome_status = 'PREDICTION' AND is_non_policy = FALSE`, `'NON_POLICY'` where `outcome_status = 'PREDICTION' AND is_non_policy = TRUE`, and `NULL` for `PROVIDER_REFUSAL` and `NO_USABLE_TEXT`.",
            "Preserved original `is_non_policy` and `primary_australian_domain` untouched alongside derived variables.",
            "Preserved human review and codebook flag columns strictly separate from immutable AI prediction columns.",
            "Excluded `text_clean` from `hansard_labels.parquet` and retained it in `hansard_analysis.parquet`.",
            "Excluded `text_raw`, `reasoning`, JSON provenance/config blobs, and invariant/batch SHA-256 hashes from both Parquet outputs; `data/raw/canonical_full_corpus_ai.csv` remains the archival source.",
        ],
    }

    manifest_json.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    elapsed = time.perf_counter() - t_start
    if verbose:
        labels_ratio = source_size / labels_size if labels_size else 0.0
        analysis_ratio = source_size / analysis_size if analysis_size else 0.0
        print("\n=== Stage 1 Preprocessing Summary ===")
        print(
            f"Source CSV size:       {source_size:>14,} bytes ({source_size / (1024**2):.2f} MiB)"
        )
        print(
            f"Labels Parquet size:   {labels_size:>14,} bytes ({labels_size / (1024**2):.2f} MiB) "
            f"[{labels_ratio:.2f}x compression vs CSV, {100 * (1 - labels_size / source_size):.1f}% reduction]"
        )
        print(
            f"Analysis Parquet size: {analysis_size:>14,} bytes ({analysis_size / (1024**2):.2f} MiB) "
            f"[{analysis_ratio:.2f}x compression vs CSV, {100 * (1 - analysis_size / source_size):.1f}% reduction]"
        )
        print(f"Runtime:               {elapsed:.2f} seconds")
        print("Integrity counts:")
        print(f"  Total rows:          {total_rows:,}")
        print(f"  Unique turn keys:    {unique_turn_keys:,}")
        print(f"  Duplicate turn keys: {duplicate_count:,}")
        print(f"  PREDICTION:          {n_prediction:,}")
        print(f"    - Policy:          {derived_counts['is_policy_true']:,}")
        print(f"    - Non-policy:      {derived_counts['is_policy_false']:,}")
        print(f"  PROVIDER_REFUSAL:    {n_provider_refusal:,}")
        print(f"  NO_USABLE_TEXT:      {n_no_usable_text:,}")

    return manifest


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments."""
    repo_root = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(
        description="Stage 1 DuckDB preprocessing for the canonical Hansard AI annotation dataset."
    )
    parser.add_argument(
        "--source",
        type=Path,
        default=repo_root / "data" / "raw" / "canonical_full_corpus_ai.csv",
        help="Path to raw canonical CSV export.",
    )
    parser.add_argument(
        "--labels-out",
        type=Path,
        default=repo_root / "data" / "processed" / "hansard_labels.parquet",
        help="Output path for compact labels Parquet dataset.",
    )
    parser.add_argument(
        "--analysis-out",
        type=Path,
        default=repo_root / "data" / "processed" / "hansard_analysis.parquet",
        help="Output path for analysis Parquet dataset (includes text_clean).",
    )
    parser.add_argument(
        "--manifest-out",
        type=Path,
        default=repo_root / "data" / "processed" / "preprocessing_manifest.json",
        help="Output path for preprocessing manifest JSON.",
    )
    parser.add_argument(
        "--row-group-size",
        type=int,
        default=DEFAULT_ROW_GROUP_SIZE,
        help="Parquet row group size (default: 10000).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint."""
    args = parse_args(argv)
    repo_root = Path(__file__).resolve().parent.parent
    run_preprocessing(
        source_csv=args.source,
        labels_parquet=args.labels_out,
        analysis_parquet=args.analysis_out,
        manifest_json=args.manifest_out,
        row_group_size=args.row_group_size,
        repo_root=repo_root,
        verbose=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

