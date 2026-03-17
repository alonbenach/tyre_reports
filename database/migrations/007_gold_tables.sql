-- Gold tables for report-ready weekly marts.

CREATE TABLE IF NOT EXISTS gold_market_weekly (
    gold_market_id INTEGER PRIMARY KEY,
    snapshot_date TEXT NOT NULL,
    rows INTEGER,
    unique_products INTEGER,
    unique_sellers INTEGER,
    stock_qty REAL,
    median_price REAL,
    mean_price REAL,
    canonical_rows INTEGER,
    canonical_match_rate REAL,
    built_at_utc TEXT NOT NULL,
    UNIQUE (snapshot_date)
);

CREATE TABLE IF NOT EXISTS gold_brand_weekly (
    gold_brand_id INTEGER PRIMARY KEY,
    snapshot_date TEXT NOT NULL,
    brand TEXT NOT NULL,
    rows INTEGER,
    unique_products INTEGER,
    unique_sellers INTEGER,
    stock_qty REAL,
    median_price REAL,
    mean_price REAL,
    median_price_prev_week REAL,
    median_price_prev_year REAL,
    median_price_wow_delta REAL,
    median_price_yoy_delta REAL,
    stock_qty_prev_week REAL,
    stock_qty_prev_year REAL,
    stock_qty_wow_delta REAL,
    stock_qty_yoy_delta REAL,
    built_at_utc TEXT NOT NULL,
    UNIQUE (snapshot_date, brand)
);

CREATE TABLE IF NOT EXISTS gold_segment_weekly (
    gold_segment_id INTEGER PRIMARY KEY,
    snapshot_date TEXT NOT NULL,
    analysis_fitment_key TEXT NOT NULL,
    brand TEXT NOT NULL,
    rows INTEGER,
    stock_qty REAL,
    median_price REAL,
    unique_products INTEGER,
    built_at_utc TEXT NOT NULL,
    UNIQUE (snapshot_date, analysis_fitment_key, brand)
);

CREATE TABLE IF NOT EXISTS gold_seller_weekly (
    gold_seller_id INTEGER PRIMARY KEY,
    snapshot_date TEXT NOT NULL,
    seller_norm TEXT NOT NULL,
    brand TEXT NOT NULL,
    rows INTEGER,
    stock_qty REAL,
    median_price REAL,
    built_at_utc TEXT NOT NULL,
    UNIQUE (snapshot_date, seller_norm, brand)
);

CREATE TABLE IF NOT EXISTS gold_fitment_weekly (
    gold_fitment_id INTEGER PRIMARY KEY,
    snapshot_date TEXT NOT NULL,
    fitment_position TEXT NOT NULL,
    brand TEXT NOT NULL,
    analysis_fitment_key TEXT NOT NULL,
    rows INTEGER,
    stock_qty REAL,
    median_price REAL,
    built_at_utc TEXT NOT NULL,
    UNIQUE (snapshot_date, fitment_position, brand, analysis_fitment_key)
);

CREATE TABLE IF NOT EXISTS gold_price_positioning_weekly (
    gold_positioning_id INTEGER PRIMARY KEY,
    snapshot_date TEXT NOT NULL,
    pirelli_median_price REAL,
    pirelli_stock_qty REAL,
    competitor_median_price REAL,
    competitor_stock_qty REAL,
    market_median_price REAL,
    market_stock_qty REAL,
    price_gap_vs_comp REAL,
    pirelli_price_index REAL,
    granularity TEXT NOT NULL,
    analysis_fitment_key TEXT NOT NULL,
    rim_group TEXT,
    price_gap_vs_comp_prev_week REAL,
    price_gap_vs_comp_prev_year REAL,
    price_gap_vs_comp_wow_delta REAL,
    price_gap_vs_comp_yoy_delta REAL,
    pirelli_stock_qty_prev_week REAL,
    pirelli_stock_qty_prev_year REAL,
    pirelli_stock_qty_wow_delta REAL,
    pirelli_stock_qty_yoy_delta REAL,
    built_at_utc TEXT NOT NULL,
    UNIQUE (snapshot_date, granularity, analysis_fitment_key)
);

CREATE TABLE IF NOT EXISTS gold_mapping_match_quality_weekly (
    gold_match_quality_id INTEGER PRIMARY KEY,
    snapshot_date TEXT NOT NULL,
    rows_x INTEGER,
    canonical_rows INTEGER,
    high_conf_rows INTEGER,
    canonical_match_rate REAL,
    high_conf_match_rate REAL,
    match_method TEXT,
    rows_y INTEGER,
    built_at_utc TEXT NOT NULL,
    UNIQUE (snapshot_date, match_method)
);

CREATE TABLE IF NOT EXISTS gold_keyfitment_checkpoint_weekly (
    gold_checkpoint_id INTEGER PRIMARY KEY,
    snapshot_date TEXT NOT NULL,
    segment_reference_group TEXT NOT NULL,
    key_fitments TEXT NOT NULL,
    brand TEXT NOT NULL,
    pattern_set TEXT NOT NULL,
    size_root TEXT NOT NULL,
    rows INTEGER,
    stock_qty REAL,
    median_price REAL,
    list_price REAL,
    avg_effective_discount REAL,
    implied_discount_vs_list REAL,
    median_price_prev_week REAL,
    median_price_prev_year REAL,
    median_price_wow_delta REAL,
    median_price_yoy_delta REAL,
    stock_qty_prev_week REAL,
    stock_qty_prev_year REAL,
    stock_qty_wow_delta REAL,
    stock_qty_yoy_delta REAL,
    built_at_utc TEXT NOT NULL,
    UNIQUE (snapshot_date, segment_reference_group, key_fitments, brand, pattern_set, size_root)
);

CREATE TABLE IF NOT EXISTS gold_recap_by_brand_weekly (
    gold_recap_id INTEGER PRIMARY KEY,
    snapshot_date TEXT NOT NULL,
    brand TEXT NOT NULL,
    weighted_brand_price REAL,
    weighted_pirelli_price REAL,
    used_weight REAL,
    fitments_used INTEGER,
    imputed_segments INTEGER,
    positioning_index REAL,
    target_segment_groups INTEGER,
    weight_coverage_pct REAL,
    positioning_index_prev_week REAL,
    positioning_index_prev_year REAL,
    positioning_index_wow_delta REAL,
    positioning_index_yoy_delta REAL,
    positioning_index_round INTEGER,
    vs_prev_week_round INTEGER,
    vs_py_round INTEGER,
    built_at_utc TEXT NOT NULL,
    UNIQUE (snapshot_date, brand)
);

CREATE TABLE IF NOT EXISTS gold_recap_by_brand_latest (
    gold_recap_latest_id INTEGER PRIMARY KEY,
    snapshot_date TEXT NOT NULL,
    week_label TEXT,
    brand TEXT NOT NULL,
    positioning_display TEXT,
    vs_prev_week_display TEXT,
    vs_py_display TEXT,
    positioning_index REAL,
    positioning_index_round INTEGER,
    vs_prev_week_round INTEGER,
    vs_py_round INTEGER,
    fitments_used INTEGER,
    weight_coverage_pct REAL,
    built_at_utc TEXT NOT NULL,
    UNIQUE (snapshot_date, brand)
);

CREATE INDEX IF NOT EXISTS idx_gold_market_weekly_snapshot_date ON gold_market_weekly (snapshot_date);
CREATE INDEX IF NOT EXISTS idx_gold_brand_weekly_snapshot_brand ON gold_brand_weekly (snapshot_date, brand);
CREATE INDEX IF NOT EXISTS idx_gold_segment_weekly_snapshot_brand ON gold_segment_weekly (snapshot_date, brand);
CREATE INDEX IF NOT EXISTS idx_gold_seller_weekly_snapshot_brand ON gold_seller_weekly (snapshot_date, brand);
CREATE INDEX IF NOT EXISTS idx_gold_fitment_weekly_snapshot_brand ON gold_fitment_weekly (snapshot_date, brand);
CREATE INDEX IF NOT EXISTS idx_gold_price_positioning_weekly_snapshot_granularity ON gold_price_positioning_weekly (snapshot_date, granularity);
CREATE INDEX IF NOT EXISTS idx_gold_match_quality_weekly_snapshot ON gold_mapping_match_quality_weekly (snapshot_date);
CREATE INDEX IF NOT EXISTS idx_gold_checkpoint_weekly_snapshot_brand ON gold_keyfitment_checkpoint_weekly (snapshot_date, brand);
CREATE INDEX IF NOT EXISTS idx_gold_recap_weekly_snapshot_brand ON gold_recap_by_brand_weekly (snapshot_date, brand);
CREATE INDEX IF NOT EXISTS idx_gold_recap_latest_snapshot_brand ON gold_recap_by_brand_latest (snapshot_date, brand);
