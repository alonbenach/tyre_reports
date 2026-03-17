-- Refine reference tables so they match the runtime data needed by the app.

DROP INDEX IF EXISTS idx_ref_canonical_fitment_mapping_brand_size_root;
DROP INDEX IF EXISTS idx_ref_canonical_fitment_mapping_ipcode;
DROP INDEX IF EXISTS idx_ref_price_list_ipcode;
DROP INDEX IF EXISTS idx_ref_price_list_brand;

ALTER TABLE ref_canonical_fitment_mapping RENAME TO ref_canonical_fitment_mapping_old;

CREATE TABLE ref_canonical_fitment_mapping (
    mapping_id INTEGER PRIMARY KEY,
    reference_version TEXT,
    source_sheet TEXT NOT NULL,
    brand TEXT NOT NULL,
    pattern_set TEXT NOT NULL,
    pattern_set_norm TEXT NOT NULL,
    segment_reference_group TEXT,
    key_fitments TEXT,
    size_text TEXT,
    size_root TEXT,
    imported_at_utc TEXT NOT NULL
);

INSERT INTO ref_canonical_fitment_mapping (
    reference_version,
    source_sheet,
    brand,
    pattern_set,
    segment_reference_group,
    key_fitments,
    size_text,
    size_root,
    imported_at_utc
)
SELECT
    reference_version,
    source_sheet,
    brand,
    product_family,
    segment_reference_group,
    key_fitments,
    NULL,
    size_root,
    imported_at_utc
FROM ref_canonical_fitment_mapping_old;

DROP TABLE ref_canonical_fitment_mapping_old;

UPDATE ref_canonical_fitment_mapping
SET pattern_set_norm = UPPER(TRIM(pattern_set))
WHERE pattern_set_norm = '';

ALTER TABLE ref_price_list RENAME TO ref_price_list_old;

CREATE TABLE ref_price_list (
    price_list_id INTEGER PRIMARY KEY,
    reference_version TEXT,
    source_sheet TEXT NOT NULL,
    brand TEXT NOT NULL,
    pattern_name TEXT,
    pattern_norm TEXT,
    size_text TEXT,
    size_root TEXT,
    segment_reference_group TEXT,
    list_price REAL,
    ipcode TEXT,
    imported_at_utc TEXT NOT NULL
);

INSERT INTO ref_price_list (
    reference_version,
    source_sheet,
    brand,
    list_price,
    ipcode,
    imported_at_utc
)
SELECT
    reference_version,
    source_sheet,
    brand,
    list_price,
    ipcode,
    imported_at_utc
FROM ref_price_list_old;

DROP TABLE ref_price_list_old;

DROP TABLE IF EXISTS ref_campaign_rules;

CREATE TABLE ref_campaign_customer_discounts (
    campaign_customer_discount_id INTEGER PRIMARY KEY,
    reference_version TEXT,
    source_sheet TEXT NOT NULL,
    customer TEXT NOT NULL,
    customer_norm TEXT NOT NULL,
    additional_discount_for_pattern_sets REAL,
    all_in_discount REAL,
    imported_at_utc TEXT NOT NULL
);

CREATE TABLE ref_campaign_pattern_extras (
    campaign_pattern_extra_id INTEGER PRIMARY KEY,
    reference_version TEXT,
    source_sheet TEXT NOT NULL,
    pattern_set TEXT NOT NULL,
    pattern_set_norm TEXT NOT NULL,
    short_form TEXT,
    extra_discount REAL,
    imported_at_utc TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ref_canonical_fitment_mapping_brand_size_root
ON ref_canonical_fitment_mapping (brand, size_root);

CREATE INDEX IF NOT EXISTS idx_ref_canonical_fitment_mapping_pattern_norm
ON ref_canonical_fitment_mapping (pattern_set_norm);

CREATE INDEX IF NOT EXISTS idx_ref_price_list_ipcode
ON ref_price_list (ipcode);

CREATE INDEX IF NOT EXISTS idx_ref_price_list_brand
ON ref_price_list (brand);

CREATE INDEX IF NOT EXISTS idx_ref_price_list_brand_size_root_pattern_norm
ON ref_price_list (brand, size_root, pattern_norm);

CREATE INDEX IF NOT EXISTS idx_ref_campaign_customer_discounts_customer_norm
ON ref_campaign_customer_discounts (customer_norm);

CREATE INDEX IF NOT EXISTS idx_ref_campaign_pattern_extras_pattern_set_norm
ON ref_campaign_pattern_extras (pattern_set_norm);
