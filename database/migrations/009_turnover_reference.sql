CREATE TABLE IF NOT EXISTS ref_turnover_weights (
    reference_version TEXT,
    source_sheet TEXT,
    source_file_name TEXT,
    period_start_date TEXT NOT NULL,
    period_end_date TEXT NOT NULL,
    period_month TEXT NOT NULL,
    analysis_fitment_key TEXT NOT NULL,
    turnover_weight REAL NOT NULL,
    imported_at_utc TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ref_turnover_weights_period_month
ON ref_turnover_weights (period_month);

CREATE INDEX IF NOT EXISTS idx_ref_turnover_weights_fitment
ON ref_turnover_weights (analysis_fitment_key);
