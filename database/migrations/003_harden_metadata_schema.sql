-- Harden metadata tables with approved primary keys, foreign keys, and indexes.
-- Business-row uniqueness for staging, silver, and gold tables will be added
-- when those tables are introduced.

DROP VIEW IF EXISTS vw_latest_successful_run;
DROP VIEW IF EXISTS vw_latest_source_import;
DROP VIEW IF EXISTS vw_latest_positioning_outputs;
DROP VIEW IF EXISTS vw_latest_offeror_outputs;

ALTER TABLE pipeline_runs RENAME TO pipeline_runs_old;

CREATE TABLE pipeline_runs (
    run_id TEXT PRIMARY KEY,
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

INSERT INTO pipeline_runs (
    run_id,
    snapshot_date,
    run_started_at_utc,
    run_finished_at_utc,
    status,
    report_mode,
    skip_pdf,
    source_file_name,
    source_file_sha256,
    error_message,
    app_version,
    schema_version
)
SELECT
    run_id,
    snapshot_date,
    run_started_at_utc,
    run_finished_at_utc,
    status,
    report_mode,
    skip_pdf,
    source_file_name,
    source_file_sha256,
    error_message,
    app_version,
    schema_version
FROM pipeline_runs_old;

DROP TABLE pipeline_runs_old;

ALTER TABLE source_imports RENAME TO source_imports_old;

CREATE TABLE source_imports (
    import_id TEXT PRIMARY KEY,
    run_id TEXT,
    snapshot_date TEXT NOT NULL,
    source_file_path TEXT NOT NULL,
    archived_file_path TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    row_count_total INTEGER,
    row_count_motorcycle INTEGER,
    imported_at_utc TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id)
);

INSERT INTO source_imports (
    import_id,
    run_id,
    snapshot_date,
    source_file_path,
    archived_file_path,
    sha256,
    row_count_total,
    row_count_motorcycle,
    imported_at_utc
)
SELECT
    import_id,
    run_id,
    snapshot_date,
    source_file_path,
    archived_file_path,
    sha256,
    row_count_total,
    row_count_motorcycle,
    imported_at_utc
FROM source_imports_old;

DROP TABLE source_imports_old;

ALTER TABLE reference_refresh_runs RENAME TO reference_refresh_runs_old;

CREATE TABLE reference_refresh_runs (
    refresh_run_id TEXT PRIMARY KEY,
    refresh_scope TEXT NOT NULL,
    source_file_path TEXT NOT NULL,
    source_file_sha256 TEXT NOT NULL,
    started_at_utc TEXT NOT NULL,
    finished_at_utc TEXT,
    status TEXT NOT NULL,
    error_message TEXT
);

INSERT INTO reference_refresh_runs (
    refresh_run_id,
    refresh_scope,
    source_file_path,
    source_file_sha256,
    started_at_utc,
    finished_at_utc,
    status,
    error_message
)
SELECT
    refresh_run_id,
    refresh_scope,
    source_file_path,
    source_file_sha256,
    started_at_utc,
    finished_at_utc,
    status,
    error_message
FROM reference_refresh_runs_old;

DROP TABLE reference_refresh_runs_old;

ALTER TABLE generated_reports RENAME TO generated_reports_old;

CREATE TABLE generated_reports (
    report_id TEXT PRIMARY KEY,
    run_id TEXT,
    snapshot_date TEXT,
    report_type TEXT NOT NULL,
    format TEXT NOT NULL,
    output_path TEXT NOT NULL,
    generated_at_utc TEXT NOT NULL,
    status TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id)
);

INSERT INTO generated_reports (
    report_id,
    run_id,
    snapshot_date,
    report_type,
    format,
    output_path,
    generated_at_utc,
    status
)
SELECT
    report_id,
    run_id,
    snapshot_date,
    report_type,
    format,
    output_path,
    generated_at_utc,
    status
FROM generated_reports_old;

DROP TABLE generated_reports_old;

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status_started
ON pipeline_runs (status, run_started_at_utc);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_snapshot_date
ON pipeline_runs (snapshot_date);

CREATE INDEX IF NOT EXISTS idx_source_imports_snapshot_date
ON source_imports (snapshot_date);

CREATE INDEX IF NOT EXISTS idx_source_imports_sha256
ON source_imports (sha256);

CREATE INDEX IF NOT EXISTS idx_generated_reports_run_id
ON generated_reports (run_id);

CREATE INDEX IF NOT EXISTS idx_generated_reports_snapshot_type_format
ON generated_reports (snapshot_date, report_type, format);

CREATE INDEX IF NOT EXISTS idx_ref_canonical_fitment_mapping_brand_size_root
ON ref_canonical_fitment_mapping (brand, size_root);

CREATE INDEX IF NOT EXISTS idx_ref_canonical_fitment_mapping_ipcode
ON ref_canonical_fitment_mapping (ipcode);

CREATE INDEX IF NOT EXISTS idx_ref_price_list_ipcode
ON ref_price_list (ipcode);

CREATE INDEX IF NOT EXISTS idx_ref_price_list_brand
ON ref_price_list (brand);

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
