-- Silver table for normalized motorcycle snapshot data.

CREATE TABLE IF NOT EXISTS silver_motorcycle_weekly (
    silver_row_id INTEGER PRIMARY KEY,
    run_id TEXT,
    snapshot_date TEXT NOT NULL,
    iso_year INTEGER,
    iso_week INTEGER,
    brand TEXT,
    production_year INTEGER,
    seller_norm TEXT,
    product_code TEXT,
    EAN TEXT,
    price_pln REAL,
    stock_qty REAL,
    size_norm TEXT,
    rim_num REAL,
    rim_group TEXT,
    season TEXT,
    fitment_position TEXT,
    pattern_family TEXT,
    name_norm TEXT,
    size_root TEXT,
    pattern_set TEXT,
    segment_reference_group TEXT,
    key_fitments TEXT,
    match_method TEXT,
    pattern_match_score REAL,
    is_canonical_match INTEGER,
    is_high_confidence_match INTEGER,
    list_price REAL,
    ipcode TEXT,
    is_extra_3pct_set INTEGER,
    extra_discount REAL,
    opon_all_in_discount REAL,
    effective_all_in_discount REAL,
    expected_net_price_from_list REAL,
    discount_vs_list_implied REAL,
    date TEXT,
    built_at_utc TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id),
    UNIQUE (snapshot_date, product_code, seller_norm, price_pln)
);

CREATE INDEX IF NOT EXISTS idx_silver_motorcycle_weekly_snapshot_date
ON silver_motorcycle_weekly (snapshot_date);

CREATE INDEX IF NOT EXISTS idx_silver_motorcycle_weekly_iso_year_week
ON silver_motorcycle_weekly (iso_year, iso_week);

CREATE INDEX IF NOT EXISTS idx_silver_motorcycle_weekly_brand_snapshot
ON silver_motorcycle_weekly (brand, snapshot_date);

CREATE INDEX IF NOT EXISTS idx_silver_motorcycle_weekly_seller_snapshot
ON silver_motorcycle_weekly (seller_norm, snapshot_date);
