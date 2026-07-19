from cube_web.services.partition_domain_schema import (
    NEW_DOMAIN_TABLES,
    PARTITION_DOMAIN_SCHEMA_VERSION,
    TASK_SCHEDULER_TABLES,
    schema_statements,
)


def test_domain_schema_contains_versioned_tables_and_quality_handoff() -> None:
    sql = "\n".join(schema_statements()).lower()
    assert PARTITION_DOMAIN_SCHEMA_VERSION == "2026-07-19-partition-domain-v2"
    assert NEW_DOMAIN_TABLES == {
        "partition_datasets", "partition_dataset_assets", "partition_dataset_bands",
        "partition_output_versions", "partition_tiles", "partition_indexes", "partition_grid_cells",
        "partition_quality_runs", "partition_quality_results", "partition_quality_errors",
        "partition_quality_warn_approvals", "partition_publications", "partition_domain_outbox",
        "partition_domain_schema_version",
    }
    assert TASK_SCHEDULER_TABLES == {"partition_batches", "partition_assets", "partition_job_attempts"}
    for column in ("current_quality_run_id", "quality_status", "quality_sequence", "quality_error_count", "quality_warning_count"):
        assert column in sql
    assert "trigger_event_id uuid null unique" in sql
    assert "foreign key (trigger_event_id)" not in sql
    assert "unique (dataset_id, output_version, event_type)" in sql
    assert "deferrable initially deferred" in sql
    assert "status in ('publishing','active','withdrawing','failed','withdrawn')" in sql
    assert "desired_action in ('activate','withdraw')" in sql
    assert "uq_partition_publication_live_snapshot" in sql
    assert "source_uri text not null" in sql
    assert "source_format text not null default 'cog'" in sql
    assert "alter table partition_indexes add column if not exists attributes jsonb" in sql


def test_quality_handoff_has_required_columns_and_no_unpublished_row() -> None:
    statements = schema_statements()
    expected = {
        "partition_quality_results": {"quality_run_id", "dataset_id", "output_version", "rule_code", "status", "finding_count", "error_count", "warning_count", "metrics", "execution_error", "started_at", "completed_at"},
        "partition_quality_errors": {"quality_error_id", "quality_run_id", "dataset_id", "output_version", "rule_code", "source_asset_id", "tile_id", "index_id", "output_id", "row_number", "field_name", "error_code", "message", "context", "created_at"},
        "partition_quality_warn_approvals": {"approval_id", "dataset_id", "output_version", "quality_run_id", "rule_set_version", "approved_by", "approved_at", "reason"},
        "partition_publications": {"publication_id", "dataset_id", "output_version", "quality_run_id", "status", "desired_action", "service_version_id", "requested_by", "requested_at", "activated_at", "failure", "withdrawn_by", "withdrawn_at", "withdrawal_reason", "attempt_count", "available_at", "claimed_at", "claimed_by", "last_error", "created_at", "updated_at"},
    }
    for table, columns in expected.items():
        statement = next(item.lower() for item in statements if f"create table if not exists {table}" in item.lower())
        assert columns <= set(statement.replace("(", " ").replace(")", " ").replace(",", " ").split()) | {column for column in columns if column in statement}
    publication = next(item.lower() for item in statements if "create table if not exists partition_publications" in item.lower())
    assert "'published'" not in publication
    assert "'unpublished'" not in publication
