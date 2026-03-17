-- Initial bootstrap schema.
-- Constraint-heavy decisions such as primary keys, foreign keys, and
-- uniqueness rules are intentionally deferred pending explicit approval.

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id TEXT NOT NULL,
    snapshot_date TEXT,
    run_started_at_utc TEXT NOT NULL,
    run_finished_at_utc TEXT,
    status TEXT NOT NULL,
    report_mode TEXT,
    skip_pdf INTEGER NOT NULL DEFAULT 0,
    source_file_name TEXT,
    source_file_sha256 TEXT,
    error_message TEXT,
    app_version TEXT,
    schema_version TEXT
);

CREATE TABLE IF NOT EXISTS source_imports (
    import_id TEXT NOT NULL,
    run_id TEXT,
    snapshot_date TEXT NOT NULL,
    source_file_path TEXT NOT NULL,
    archived_file_path TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    row_count_total INTEGER,
    row_count_motorcycle INTEGER,
    imported_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS reference_refresh_runs (
    refresh_run_id TEXT NOT NULL,
    refresh_scope TEXT NOT NULL,
    source_file_path TEXT NOT NULL,
    source_file_sha256 TEXT NOT NULL,
    started_at_utc TEXT NOT NULL,
    finished_at_utc TEXT,
    status TEXT NOT NULL,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS generated_reports (
    report_id TEXT NOT NULL,
    run_id TEXT,
    snapshot_date TEXT,
    report_type TEXT NOT NULL,
    format TEXT NOT NULL,
    output_path TEXT NOT NULL,
    generated_at_utc TEXT NOT NULL,
    status TEXT NOT NULL
);

CREATE VIEW IF NOT EXISTS vw_latest_successful_run AS
SELECT *
FROM pipeline_runs
WHERE status = 'succeeded'
ORDER BY run_finished_at_utc DESC
LIMIT 1;

CREATE VIEW IF NOT EXISTS vw_latest_source_import AS
SELECT *
FROM source_imports
ORDER BY imported_at_utc DESC
LIMIT 1;

CREATE VIEW IF NOT EXISTS vw_latest_positioning_outputs AS
SELECT *
FROM generated_reports
WHERE report_type = 'positioning'
ORDER BY generated_at_utc DESC;

CREATE VIEW IF NOT EXISTS vw_latest_offeror_outputs AS
SELECT *
FROM generated_reports
WHERE report_type = 'offeror_focus'
ORDER BY generated_at_utc DESC;
