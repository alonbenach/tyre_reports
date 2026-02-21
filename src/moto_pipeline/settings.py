from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
REPORT_DIR = ROOT / "reports"
ASSETS_DIR = ROOT / "assets"
LOGOS_DIR = ASSETS_DIR / "logos"
CAMPAIGN_DIR = DATA_DIR / "campaign rules"
CAMPAIGN_FILE = CAMPAIGN_DIR / "campaign 2026.xlsx"
MAPPING_FILE = CAMPAIGN_DIR / "canonical fitment mapping.xlsx"
PRICE_LIST_FILE = CAMPAIGN_DIR / "price list Pirelli and competitors.xlsx"

MOTORCYCLE_TYPE = "Motocykle"
TOP_COMPETITORS = ["Michelin", "Bridgestone", "Dunlop", "Continental"]
FOCUS_BRANDS = ["Pirelli", *TOP_COMPETITORS]
RECAP_BRANDS = ["Pirelli", "Metzeler", "Michelin", "Continental", "Bridgestone", "Dunlop"]

INPUT_COLUMNS = [
    "product_code",
    "EAN",
    "price",
    "price \u20ac",
    "amount",
    "realizationTime",
    "productionYear",
    "seller",
    "actualization",
    "is_retreaded",
    "producer",
    "size",
    "width",
    "rim",
    "profil",
    "speed",
    "capacity",
    "season",
    "ROF",
    "XL",
    "name",
    "type",
    "date",
]
