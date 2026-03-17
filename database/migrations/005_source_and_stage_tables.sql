-- Source-stage tables for motorcycle CSV ingestion.

CREATE TABLE IF NOT EXISTS stg_weekly_source_motorcycle (
    stage_row_id INTEGER PRIMARY KEY,
    run_id TEXT,
    snapshot_date TEXT NOT NULL,
    source_row_number INTEGER,
    product_code TEXT,
    EAN TEXT,
    price TEXT,
    price_eur TEXT,
    amount TEXT,
    realizationTime TEXT,
    productionYear TEXT,
    seller TEXT,
    actualization TEXT,
    is_retreaded TEXT,
    producer TEXT,
    size TEXT,
    width TEXT,
    rim TEXT,
    profil TEXT,
    speed TEXT,
    capacity TEXT,
    season TEXT,
    ROF TEXT,
    XL TEXT,
    name TEXT,
    type TEXT,
    date TEXT,
    imported_at_utc TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_stg_weekly_source_motorcycle_snapshot_date
ON stg_weekly_source_motorcycle (snapshot_date);

CREATE INDEX IF NOT EXISTS idx_stg_weekly_source_motorcycle_snapshot_product_seller
ON stg_weekly_source_motorcycle (snapshot_date, product_code, seller);
