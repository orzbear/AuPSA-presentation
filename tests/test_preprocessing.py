"""Unit and integration tests for Stage 1 DuckDB preprocessing (`scripts/01_preprocess.py`).

Uses lightweight synthetic CSV fixtures in temporary directories rather than the 1.39 GB
canonical CSV export.
"""

from __future__ import annotations

import csv
import importlib.util
import json
from pathlib import Path
import shutil
from typing import Any, Iterator
import uuid

import duckdb
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "01_preprocess.py"

spec = importlib.util.spec_from_file_location("preprocess_01", SCRIPT_PATH)
assert spec is not None and spec.loader is not None
preprocess_01 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(preprocess_01)

ALL_SOURCE_COLUMNS: list[str] = [
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
    "text_raw",
    "outcome_status",
    "is_non_policy",
    "primary_australian_domain",
    "secondary_australian_domains",
    "reasoning",
    "prediction_id",
    "abstention_id",
    "provider_block_reason",
    "refusal_provenance_json",
    "provider",
    "configured_model",
    "returned_model",
    "prompt_version",
    "prompt_sha256",
    "canonical_schema_sha256",
    "prediction_contract_sha256",
    "provider_schema_sha256",
    "thinking_level",
    "generation_config_json",
    "execution_mode",
    "prediction_run_id",
    "artifact_reference",
    "run_artifact_sha256",
    "run_record_set_sha256",
    "provider_job_id",
    "run_imported_at",
    "prediction_created_at",
    "abstention_created_at",
    "campaign_id",
    "campaign_name",
    "campaign_status",
    "campaign_record_set_sha256",
    "campaign_artifact_sha256",
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
]


def _make_base_row(index: int, **overrides: Any) -> dict[str, Any]:
    """Create a realistic synthetic row with all 64 canonical CSV columns."""
    turn_key = f"turn_key_{index:04d}_sha256"
    row: dict[str, Any] = {
        "corpus_order": index,
        "corpus_id": "385a0089-7e50-59c7-bfc6-02ada0e4a139",
        "corpus_slug": "australian-house-representatives-hansard",
        "corpus_name": "Australian House of Representatives Hansard",
        "turn_id": index,
        "turn_key": turn_key,
        "record_id": f"turn:{turn_key}",
        "speech_date": "2018-05-15",
        "chamber": "Australian Federal House of Representatives",
        "source_file": "2018/2018-05-15.xml",
        "source_url": "http://parlinfo.aph.gov.au/example",
        "turn_sequence": index,
        "speaker_id": f"uk.org.publicwhip/member/{100 + index}",
        "speaker_name": f"Speaker {index}",
        "major_heading": "QUESTIONS WITHOUT NOTICE",
        "minor_heading": "Budget",
        "business_type": "questions_without_notice",
        "calculated_word_count": 120 + index,
        "warning_codes": "[]",
        "text_clean": f"Cleaned parliamentary speech text for turn {index}.",
        "text_raw": f"<talk>Raw XML speech text for turn {index}.</talk>",
        "outcome_status": "PREDICTION",
        "is_non_policy": "false",
        "primary_australian_domain": "AU01",
        "secondary_australian_domains": '["AU04"]',
        "reasoning": f"Verbose model reasoning for turn {index}.",
        "prediction_id": f"pred-{index:04d}",
        "abstention_id": "",
        "provider_block_reason": "",
        "refusal_provenance_json": "",
        "provider": "gemini",
        "configured_model": "gemini-3.1-flash-lite",
        "returned_model": "gemini-3.1-flash-lite",
        "prompt_version": "phase4a-policy-v2",
        "prompt_sha256": "a41027b001f10b61074c196996606c86b5dac4c0788ae1",
        "canonical_schema_sha256": "9c94bab9cd2976559c79b1c31991aef4138a2071a5d3b3",
        "prediction_contract_sha256": "99cafa6c473293dc076793ee6baeca44b6961d7cb11de8",
        "provider_schema_sha256": "980b0ed6d6002a8eadd326f84942ba60dcdf8f2abd9e70",
        "thinking_level": "minimal",
        "generation_config_json": '{"temperature": 0.0, "thinking_level": "minimal"}',
        "execution_mode": "batch",
        "prediction_run_id": "b00c2680-0fe2-49fd-95e7-45f014901118",
        "artifact_reference": "/var/lib/hansard-campaign-artifacts/batch.jsonl",
        "run_artifact_sha256": "86ee40e49e2fce5340196ed5c2b23118e6c458d65e6803",
        "run_record_set_sha256": "b440d6af2895aaaf02bbeaea12107295c6631cefb7387a",
        "provider_job_id": "phase4e:bd6c96fa-71e7-4cd1-ba9d-e4d844b3428a",
        "run_imported_at": "2026-10-20T06:17:18.577606+00:00",
        "prediction_created_at": "2026-10-20T06:17:18.577606+00:00",
        "abstention_created_at": "",
        "campaign_id": "bd6c96fa-71e7-4cd1-ba9d-e4d844b3428a",
        "campaign_name": "Full Corpus Production – Gemini 3.1 Flash-Lite – 2026-10",
        "campaign_status": "IMPORTED",
        "campaign_record_set_sha256": "b440d6af2895aaaf02bbeaea12107295c6631cefb7387a",
        "campaign_artifact_sha256": "b4c0cbeb549589e86540ffe25877287301493403c9995d",
        "campaign_imported_at": "2026-10-20T06:17:18.332209+00:00",
        "latest_review_status": "",
        "review_edited": "false",
        "latest_reviewed_at": "",
        "latest_review_id": "",
        "latest_review_annotation_version_id": "",
        "latest_review_note": "",
        "has_codebook_flag": "false",
        "latest_codebook_flag_note": "",
        "latest_codebook_flagged_at": "",
    }
    row.update(overrides)
    return row


def _write_synthetic_csv(
    csv_path: Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None
) -> None:
    """Write synthetic rows to a CSV file."""
    cols = fieldnames if fieldnames is not None else ALL_SOURCE_COLUMNS
    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=cols)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in cols})


@pytest.fixture()
def work_tmp_path() -> Iterator[Path]:
    """Provide an isolated temporary directory inside `.pytest_cache/tmp`."""
    base_dir = REPO_ROOT / ".pytest_cache" / "tmp" / uuid.uuid4().hex
    base_dir.mkdir(parents=True, exist_ok=True)
    try:
        yield base_dir
    finally:
        shutil.rmtree(base_dir, ignore_errors=True)


@pytest.fixture()
def synthetic_pipeline_outputs(work_tmp_path: Path) -> dict[str, Any]:
    """Build a 5-row synthetic dataset covering all outcomes, policy flags, and review metadata."""
    rows = [
        # Row 1: Policy prediction (AU01) in 2010
        _make_base_row(
            1,
            speech_date="2010-02-02",
            outcome_status="PREDICTION",
            is_non_policy="false",
            primary_australian_domain="AU01",
            secondary_australian_domains='["AU12"]',
        ),
        # Row 2: Policy prediction (AU12) with human review metadata in 2015
        _make_base_row(
            2,
            speech_date="2015-08-19",
            outcome_status="PREDICTION",
            is_non_policy="false",
            primary_australian_domain="AU12",
            secondary_australian_domains="[]",
            latest_review_status="accepted",
            review_edited="false",
            latest_reviewed_at="2026-10-16T10:16:32.665428+00:00",
            latest_review_id="8fde6fdd-6bb2-4376-bdff-46d3673c9571",
            latest_review_annotation_version_id="251",
        ),
        # Row 3: Non-policy prediction in 2020
        _make_base_row(
            3,
            speech_date="2020-03-23",
            outcome_status="PREDICTION",
            is_non_policy="true",
            primary_australian_domain="",
            secondary_australian_domains="[]",
        ),
        # Row 4: Provider refusal in 2022
        _make_base_row(
            4,
            speech_date="2022-11-10",
            outcome_status="PROVIDER_REFUSAL",
            is_non_policy="",
            primary_australian_domain="",
            secondary_australian_domains="",
            reasoning="",
            prediction_id="",
            abstention_id="b7756067-3737-4e80-9de4-ac2a6641d20b",
            provider_block_reason="PROHIBITED_CONTENT",
            refusal_provenance_json='{"blockReason": "PROHIBITED_CONTENT"}',
            prediction_created_at="",
            abstention_created_at="2026-10-20T06:17:18.577606+00:00",
        ),
        # Row 5: No usable text in 2025
        _make_base_row(
            5,
            speech_date="2025-10-29",
            calculated_word_count=0,
            warning_codes='["EMPTY_SPEECH_TEXT"]',
            text_clean="",
            outcome_status="NO_USABLE_TEXT",
            is_non_policy="",
            primary_australian_domain="",
            secondary_australian_domains="",
            reasoning="",
            prediction_id="",
            prediction_run_id="",
            run_imported_at="",
            prediction_created_at="",
            campaign_id="",
            campaign_name="",
            campaign_status="",
            campaign_imported_at="",
        ),
    ]

    csv_path = work_tmp_path / "synthetic_canonical.csv"
    labels_path = work_tmp_path / "hansard_labels.parquet"
    analysis_path = work_tmp_path / "hansard_analysis.parquet"
    manifest_path = work_tmp_path / "preprocessing_manifest.json"

    _write_synthetic_csv(csv_path, rows)

    manifest = preprocess_01.run_preprocessing(
        source_csv=csv_path,
        labels_parquet=labels_path,
        analysis_parquet=analysis_path,
        manifest_json=manifest_path,
        row_group_size=10_000,
        repo_root=work_tmp_path,
        verbose=False,
    )

    return {
        "csv_path": csv_path,
        "labels_path": labels_path,
        "analysis_path": analysis_path,
        "manifest_path": manifest_path,
        "manifest": manifest,
        "rows": rows,
    }


def test_outcome_and_policy_non_policy_derivation(
    synthetic_pipeline_outputs: dict[str, Any],
) -> None:
    """Verify year, is_policy, and primary_domain derivations across all outcome types."""
    labels_path = synthetic_pipeline_outputs["labels_path"]
    con = duckdb.connect()
    records = con.sql(f"""
        SELECT
            corpus_order,
            year,
            outcome_status,
            is_non_policy,
            is_policy,
            primary_australian_domain,
            primary_domain,
            latest_review_status
        FROM read_parquet('{labels_path.as_posix()}')
        ORDER BY corpus_order
    """).fetchall()
    con.close()

    assert records == [
        (1, 2010, "PREDICTION", False, True, "AU01", "AU01", None),
        (2, 2015, "PREDICTION", False, True, "AU12", "AU12", "accepted"),
        (3, 2020, "PREDICTION", True, False, None, "NON_POLICY", None),
        (4, 2022, "PROVIDER_REFUSAL", None, None, None, None, None),
        (5, 2025, "NO_USABLE_TEXT", None, None, None, None, None),
    ]


def test_exclusion_of_heavy_fields_and_text_clean_placement(
    synthetic_pipeline_outputs: dict[str, Any],
) -> None:
    """Ensure heavy fields are excluded from both outputs and text_clean is only in analysis."""
    labels_path = synthetic_pipeline_outputs["labels_path"]
    analysis_path = synthetic_pipeline_outputs["analysis_path"]

    con = duckdb.connect()
    labels_cols = set(
        con.sql(f"SELECT * FROM read_parquet('{labels_path.as_posix()}') LIMIT 0").columns
    )
    analysis_cols = set(
        con.sql(f"SELECT * FROM read_parquet('{analysis_path.as_posix()}') LIMIT 0").columns
    )
    con.close()

    # text_clean must ONLY be in hansard_analysis.parquet
    assert "text_clean" not in labels_cols
    assert "text_clean" in analysis_cols
    assert analysis_cols - labels_cols == {"text_clean"}

    # Heavy & invariant hash fields must be absent from both datasets
    for heavy_col in preprocess_01.EXCLUDED_FROM_BOTH:
        assert heavy_col not in labels_cols, f"{heavy_col} should not be in labels Parquet"
        assert heavy_col not in analysis_cols, f"{heavy_col} should not be in analysis Parquet"


def test_refusal_and_no_usable_text_preservation(
    synthetic_pipeline_outputs: dict[str, Any],
) -> None:
    """Confirm PROVIDER_REFUSAL and NO_USABLE_TEXT rows and refusal metadata are preserved."""
    analysis_path = synthetic_pipeline_outputs["analysis_path"]
    con = duckdb.connect()
    refusal_row = con.sql(f"""
        SELECT
            corpus_order,
            outcome_status,
            is_non_policy,
            is_policy,
            primary_australian_domain,
            primary_domain,
            abstention_id,
            provider_block_reason,
            abstention_created_at IS NOT NULL AS has_abstention_ts,
            text_clean
        FROM read_parquet('{analysis_path.as_posix()}')
        WHERE outcome_status = 'PROVIDER_REFUSAL'
    """).fetchone()

    no_text_row = con.sql(f"""
        SELECT
            corpus_order,
            outcome_status,
            calculated_word_count,
            is_non_policy,
            is_policy,
            primary_domain,
            warning_codes
        FROM read_parquet('{analysis_path.as_posix()}')
        WHERE outcome_status = 'NO_USABLE_TEXT'
    """).fetchone()
    con.close()

    assert refusal_row == (
        4,
        "PROVIDER_REFUSAL",
        None,
        None,
        None,
        None,
        "b7756067-3737-4e80-9de4-ac2a6641d20b",
        "PROHIBITED_CONTENT",
        True,
        "Cleaned parliamentary speech text for turn 4.",
    )
    assert no_text_row == (
        5,
        "NO_USABLE_TEXT",
        0,
        None,
        None,
        None,
        '["EMPTY_SPEECH_TEXT"]',
    )


def test_deterministic_row_count_and_ordering(work_tmp_path: Path) -> None:
    """Verify row counts are preserved deterministically and ordered by corpus_order."""
    # Feed rows in reverse corpus_order to verify deterministic ordering
    rows = [
        _make_base_row(3, outcome_status="PREDICTION", is_non_policy="true", primary_australian_domain=""),
        _make_base_row(1, outcome_status="PREDICTION", is_non_policy="false", primary_australian_domain="AU02"),
        _make_base_row(2, outcome_status="PREDICTION", is_non_policy="false", primary_australian_domain="AU05"),
    ]
    csv_path = work_tmp_path / "unordered.csv"
    labels_path = work_tmp_path / "labels.parquet"
    analysis_path = work_tmp_path / "analysis.parquet"
    manifest_path = work_tmp_path / "manifest.json"
    _write_synthetic_csv(csv_path, rows)

    manifest = preprocess_01.run_preprocessing(
        source_csv=csv_path,
        labels_parquet=labels_path,
        analysis_parquet=analysis_path,
        manifest_json=manifest_path,
        verbose=False,
    )

    assert manifest["row_count"] == 3
    assert manifest["unique_turn_count"] == 3
    assert manifest["duplicate_count"] == 0

    con = duckdb.connect()
    orders_labels = [
        r[0]
        for r in con.sql(f"SELECT corpus_order FROM read_parquet('{labels_path.as_posix()}')").fetchall()
    ]
    orders_analysis = [
        r[0]
        for r in con.sql(f"SELECT corpus_order FROM read_parquet('{analysis_path.as_posix()}')").fetchall()
    ]
    con.close()

    assert orders_labels == [1, 2, 3]
    assert orders_analysis == [1, 2, 3]


def test_manifest_generation_completeness(
    synthetic_pipeline_outputs: dict[str, Any],
) -> None:
    """Verify preprocessing_manifest.json contains all required metadata, counts, and hashes."""
    manifest_path = synthetic_pipeline_outputs["manifest_path"]
    assert manifest_path.exists()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    required_keys = {
        "preprocessing_version",
        "generated_timestamp",
        "source_filename",
        "source_size",
        "source_sha256",
        "row_count",
        "unique_turn_count",
        "duplicate_count",
        "outcome_counts",
        "output_filenames",
        "output_byte_sizes",
        "output_sha256_hashes",
        "included_columns",
        "excluded_heavy_fields",
        "transformation_notes",
    }
    assert required_keys.issubset(manifest.keys())

    assert manifest["row_count"] == 5
    assert manifest["unique_turn_count"] == 5
    assert manifest["duplicate_count"] == 0
    assert manifest["outcome_counts"] == {
        "PREDICTION": 3,
        "PROVIDER_REFUSAL": 1,
        "NO_USABLE_TEXT": 1,
    }
    assert len(manifest["source_sha256"]) == 64
    assert len(manifest["output_sha256_hashes"]["hansard_labels.parquet"]) == 64
    assert len(manifest["output_sha256_hashes"]["hansard_analysis.parquet"]) == 64
    assert manifest["output_byte_sizes"]["hansard_labels.parquet"] > 0
    assert manifest["output_byte_sizes"]["hansard_analysis.parquet"] > 0


def test_duplicate_turn_key_detection_fails_loudly(work_tmp_path: Path) -> None:
    """Fail loudly with ValueError if duplicate turn_key values exist."""
    rows = [
        _make_base_row(1, turn_key="duplicate_key_abc"),
        _make_base_row(2, turn_key="duplicate_key_abc"),
    ]
    csv_path = work_tmp_path / "dup.csv"
    _write_synthetic_csv(csv_path, rows)

    with pytest.raises(ValueError, match="Duplicate turn_key"):
        preprocess_01.run_preprocessing(
            source_csv=csv_path,
            labels_parquet=work_tmp_path / "labels.parquet",
            analysis_parquet=work_tmp_path / "analysis.parquet",
            manifest_json=work_tmp_path / "manifest.json",
            verbose=False,
        )


def test_unexpected_outcome_status_fails_loudly(work_tmp_path: Path) -> None:
    """Fail loudly with ValueError if an unexpected outcome_status is encountered."""
    rows = [
        _make_base_row(1, outcome_status="UNKNOWN_STATUS"),
    ]
    csv_path = work_tmp_path / "bad_outcome.csv"
    _write_synthetic_csv(csv_path, rows)

    with pytest.raises(ValueError, match="Unexpected outcome_status"):
        preprocess_01.run_preprocessing(
            source_csv=csv_path,
            labels_parquet=work_tmp_path / "labels.parquet",
            analysis_parquet=work_tmp_path / "analysis.parquet",
            manifest_json=work_tmp_path / "manifest.json",
            verbose=False,
        )


def test_missing_required_columns_fails_loudly(work_tmp_path: Path) -> None:
    """Fail loudly with ValueError when required identity or outcome columns are absent."""
    incomplete_cols = [c for c in ALL_SOURCE_COLUMNS if c not in ("turn_key", "outcome_status")]
    rows = [_make_base_row(1)]
    csv_path = work_tmp_path / "missing_cols.csv"
    _write_synthetic_csv(csv_path, rows, fieldnames=incomplete_cols)

    with pytest.raises(ValueError, match="Missing required column"):
        preprocess_01.run_preprocessing(
            source_csv=csv_path,
            labels_parquet=work_tmp_path / "labels.parquet",
            analysis_parquet=work_tmp_path / "analysis.parquet",
            manifest_json=work_tmp_path / "manifest.json",
            verbose=False,
        )
