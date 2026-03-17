-- Convenience views for latest gold outputs.

CREATE VIEW IF NOT EXISTS vw_latest_gold_recap_by_brand AS
SELECT gr.*
FROM gold_recap_by_brand_weekly gr
WHERE gr.snapshot_date = (
    SELECT MAX(snapshot_date) FROM gold_recap_by_brand_weekly
);

CREATE VIEW IF NOT EXISTS vw_latest_gold_recap_display AS
SELECT *
FROM gold_recap_by_brand_latest
WHERE snapshot_date = (
    SELECT MAX(snapshot_date) FROM gold_recap_by_brand_latest
);

CREATE VIEW IF NOT EXISTS vw_latest_gold_price_positioning AS
SELECT gp.*
FROM gold_price_positioning_weekly gp
WHERE gp.snapshot_date = (
    SELECT MAX(snapshot_date) FROM gold_price_positioning_weekly
);
