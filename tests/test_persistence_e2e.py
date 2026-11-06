from scripts.validate_persistence_e2e import run_validation


def test_synthetic_persistence_pipeline_through_duckdb(tmp_path):
    result = run_validation(tmp_path)
    assert len(result["sqlite_state_rows"]) == 1
    assert result["pending_outbox_rows"] == 0
    assert result["parquet_row_count"] == 2
    assert result["change_types"] == ["INSERT", "UPDATE"]
    assert result["unique_event_identities"] == 2
    assert "review_url" in result["duckdb_columns"]
    assert "image_urls" in result["duckdb_columns"]
