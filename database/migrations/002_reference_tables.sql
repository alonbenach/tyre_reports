-- Reference tables for SQL-backed runtime reads.
-- Column-level normalization may be refined after the explicit reference-data pass.

CREATE TABLE IF NOT EXISTS ref_canonical_fitment_mapping (
    reference_version TEXT,
    source_sheet TEXT,
    brand TEXT,
    product_family TEXT,
    pattern_set TEXT,
    size_root TEXT,
    segment_reference_group TEXT,
    key_fitments TEXT,
    ipcode TEXT,
    list_price REAL,
    is_extra_3pct_set INTEGER,
    extra_discount REAL,
    imported_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ref_campaign_rules (
    reference_version TEXT,
    source_sheet TEXT,
    campaign_name TEXT,
    rule_name TEXT,
    rule_value TEXT,
    imported_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ref_price_list (
    reference_version TEXT,
    source_sheet TEXT,
    brand TEXT,
    ipcode TEXT,
    product_description TEXT,
    list_price REAL,
    currency_code TEXT,
    imported_at_utc TEXT NOT NULL
);
