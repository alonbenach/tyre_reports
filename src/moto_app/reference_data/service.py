from __future__ import annotations

import hashlib
import re
import sqlite3
import uuid
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from moto_app.db.runtime import connect_sqlite
from moto_app.observability import OperatorFacingError
from moto_pipeline.canonical import (
    load_campaign_customer_discounts,
    load_canonical_mapping,
    load_price_list,
    normalize_brand,
)


def _file_sha256(file_path: Path) -> str:
    digest = hashlib.sha256()
    with file_path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _norm_text(value: object) -> str:
    text = "" if value is None else str(value)
    text = text.upper()
    text = re.sub(r"[^A-Z0-9 ]+", " ", text)
    text = re.sub(r"(?<=[A-Z])(?=\d)", " ", text)
    text = re.sub(r"(?<=\d)(?=[A-Z])", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _norm_party_name(value: object) -> str:
    text = _norm_text(value)
    if not text:
        return ""
    tokens = [
        token
        for token in text.split()
        if token
        not in {
            "SP",
            "Z",
            "OO",
            "O",
            "SA",
            "SK",
            "K",
            "AG",
            "POLSKA",
            "SPOLKA",
            "OGRANICZONA",
            "ODPOWIEDZIALNOSCIA",
            "DAWNIEJ",
        }
    ]
    return " ".join(tokens)


def _reference_version(file_path: Path) -> str:
    return file_path.stem


def _read_campaign_pattern_extras(campaign_file: Path) -> pd.DataFrame:
    raw = pd.read_excel(campaign_file, sheet_name="rebate scheme", header=1)
    c0, c1, c2 = raw.columns[:3]
    marker = raw[c0].astype("string").str.upper().str.contains("ADDITIONAL DISCOUNT FOR PATTERN SETS", na=False)
    if not marker.any():
        return pd.DataFrame(columns=["pattern_set", "short_form", "pattern_set_norm", "extra_discount"])

    start_idx = marker[marker].index[0] + 1
    section = raw.loc[start_idx:, [c0, c1, c2]].copy()
    section.columns = ["pattern_set", "short_form", "extra_discount"]
    section["extra_discount"] = pd.to_numeric(section["extra_discount"], errors="coerce")
    section = section[section["extra_discount"].notna()].copy()
    section["pattern_set"] = section["pattern_set"].astype("string").str.strip()
    section["short_form"] = section["short_form"].astype("string").str.strip()
    section["pattern_set_norm"] = section["pattern_set"].map(_norm_text)
    section = section[section["pattern_set_norm"] != ""]
    return section.drop_duplicates().reset_index(drop=True)


@dataclass(frozen=True)
class ReferenceRefreshResult:
    db_path: Path
    refreshed_scopes: list[str]


def _replace_table(connection: sqlite3.Connection, table_name: str, rows: list[tuple], insert_sql: str) -> None:
    connection.execute(f"DELETE FROM {table_name}")
    if rows:
        connection.executemany(insert_sql, rows)


def _record_refresh(
    connection: sqlite3.Connection,
    refresh_scope: str,
    source_file_path: Path,
    source_file_sha256: str,
    status: str,
    error_message: str | None = None,
) -> None:
    connection.execute(
        """
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
        VALUES (?, ?, ?, ?, datetime('now'), datetime('now'), ?, ?)
        """,
        (
            str(uuid.uuid4()),
            refresh_scope,
            str(source_file_path),
            source_file_sha256,
            status,
            error_message,
        ),
    )


def refresh_reference_data(db_path: Path, source_dir: Path) -> ReferenceRefreshResult:
    mapping_file = source_dir / "canonical fitment mapping.xlsx"
    price_list_file = source_dir / "price list Pirelli and competitors.xlsx"
    campaign_file = source_dir / "campaign 2026.xlsx"
    expected_files = [mapping_file, price_list_file, campaign_file]
    missing_files = [str(path) for path in expected_files if not path.exists()]
    if missing_files:
        raise OperatorFacingError(
            f"Reference refresh cannot start because required workbook files are missing: {missing_files}"
        )

    with connect_sqlite(db_path) as connection:
        scopes: list[str] = []

        try:
            mapping = load_canonical_mapping(mapping_file)
            imported_at = connection.execute("SELECT datetime('now')").fetchone()[0]
            mapping_rows = [
                (
                    _reference_version(mapping_file),
                    "mapping",
                    str(row.brand),
                    str(row.pattern_set),
                    str(row.pattern_set_norm),
                    str(row.segment_reference_group),
                    str(row.key_fitments),
                    str(row.size_text),
                    str(row.size_root),
                    imported_at,
                )
                for row in mapping.itertuples(index=False)
            ]
            _replace_table(
                connection,
                "ref_canonical_fitment_mapping",
                mapping_rows,
                """
                INSERT INTO ref_canonical_fitment_mapping (
                    reference_version,
                    source_sheet,
                    brand,
                    pattern_set,
                    pattern_set_norm,
                    segment_reference_group,
                    key_fitments,
                    size_text,
                    size_root,
                    imported_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
            )
            _record_refresh(connection, "canonical_mapping", mapping_file, _file_sha256(mapping_file), "succeeded")
            scopes.append("canonical_mapping")

            price_list = load_price_list(price_list_file).copy()
            price_list["brand"] = price_list["brand"].map(normalize_brand)
            imported_at = connection.execute("SELECT datetime('now')").fetchone()[0]
            price_rows = [
                (
                    _reference_version(price_list_file),
                    "listing price",
                    str(row.brand),
                    str(row.pattern_name),
                    str(row.pattern_norm),
                    str(row.size_text),
                    str(row.size_root),
                    str(row.segment_reference_group),
                    None if pd.isna(row.list_price) else float(row.list_price),
                    str(row.ipcode),
                    imported_at,
                )
                for row in price_list.itertuples(index=False)
            ]
            _replace_table(
                connection,
                "ref_price_list",
                price_rows,
                """
                INSERT INTO ref_price_list (
                    reference_version,
                    source_sheet,
                    brand,
                    pattern_name,
                    pattern_norm,
                    size_text,
                    size_root,
                    segment_reference_group,
                    list_price,
                    ipcode,
                    imported_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
            )
            _record_refresh(connection, "price_list", price_list_file, _file_sha256(price_list_file), "succeeded")
            scopes.append("price_list")

            customer_discounts = load_campaign_customer_discounts(campaign_file)
            imported_at = connection.execute("SELECT datetime('now')").fetchone()[0]
            customer_rows = [
                (
                    _reference_version(campaign_file),
                    "rebate scheme",
                    str(row.customer),
                    str(_norm_party_name(row.customer)),
                    None
                    if pd.isna(row.additional_discount_for_pattern_sets)
                    else float(row.additional_discount_for_pattern_sets),
                    None if pd.isna(row.all_in_discount) else float(row.all_in_discount),
                    imported_at,
                )
                for row in customer_discounts.itertuples(index=False)
            ]
            _replace_table(
                connection,
                "ref_campaign_customer_discounts",
                customer_rows,
                """
                INSERT INTO ref_campaign_customer_discounts (
                    reference_version,
                    source_sheet,
                    customer,
                    customer_norm,
                    additional_discount_for_pattern_sets,
                    all_in_discount,
                    imported_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
            )

            pattern_extras = _read_campaign_pattern_extras(campaign_file)
            imported_at = connection.execute("SELECT datetime('now')").fetchone()[0]
            pattern_rows = [
                (
                    _reference_version(campaign_file),
                    "rebate scheme",
                    str(row.pattern_set),
                    str(row.pattern_set_norm),
                    str(row.short_form),
                    None if pd.isna(row.extra_discount) else float(row.extra_discount),
                    imported_at,
                )
                for row in pattern_extras.itertuples(index=False)
            ]
            _replace_table(
                connection,
                "ref_campaign_pattern_extras",
                pattern_rows,
                """
                INSERT INTO ref_campaign_pattern_extras (
                    reference_version,
                    source_sheet,
                    pattern_set,
                    pattern_set_norm,
                    short_form,
                    extra_discount,
                    imported_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
            )
            _record_refresh(connection, "campaign_workbook", campaign_file, _file_sha256(campaign_file), "succeeded")
            scopes.append("campaign_workbook")
        except Exception as exc:
            source_file = campaign_file
            if "mapping" in str(exc).lower():
                source_file = mapping_file
            elif "price" in str(exc).lower():
                source_file = price_list_file
            _record_refresh(connection, "reference_refresh", source_file, _file_sha256(source_file), "failed", str(exc))
            raise OperatorFacingError(
                "Reference refresh failed. Check that the campaign, canonical mapping, and price list workbooks still match the expected sheet structure.",
                cause=exc,
            ) from exc

        connection.commit()

    return ReferenceRefreshResult(db_path=db_path, refreshed_scopes=scopes)
