from __future__ import annotations

import hashlib
import re
import sqlite3
import uuid
from datetime import date
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from moto_app.db.runtime import connect_sqlite
from moto_app.observability import OperatorFacingError
from moto_pipeline.canonical import (
    load_campaign_customer_discounts,
    load_canonical_mapping,
    load_price_list,
    load_turnover_weights,
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


@dataclass(frozen=True)
class TurnoverReferenceStatus:
    expected_period_month: str
    latest_period_month: str | None
    latest_period_end_date: str | None
    latest_source_file_name: str | None
    is_missing_expected_month: bool


@dataclass(frozen=True)
class CoreReferenceStatus:
    missing_scopes: tuple[str, ...]

    @property
    def is_ready(self) -> bool:
        return not self.missing_scopes


def _replace_table(connection: sqlite3.Connection, table_name: str, rows: list[tuple], insert_sql: str) -> None:
    connection.execute(f"DELETE FROM {table_name}")
    if rows:
        connection.executemany(insert_sql, rows)


def _latest_turnover_workbook(source_dir: Path) -> Path | None:
    candidates = sorted(source_dir.glob("turnover report *.xls*"), key=lambda path: path.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None


def _previous_month_key(today_value: date | None = None) -> str:
    today_value = today_value or date.today()
    year = today_value.year
    month = today_value.month - 1
    if month == 0:
        year -= 1
        month = 12
    return f"{year:04d}-{month:02d}"


def _previous_month_key_for_snapshot(snapshot_value: object) -> str | None:
    snapshot_ts = pd.to_datetime(snapshot_value, errors="coerce")
    if pd.isna(snapshot_ts):
        return None
    first_of_month = pd.Timestamp(year=int(snapshot_ts.year), month=int(snapshot_ts.month), day=1)
    prev_month_end = first_of_month - pd.Timedelta(days=1)
    return prev_month_end.strftime("%Y-%m")


def _parse_turnover_period_from_filename(turnover_file: Path) -> tuple[pd.Timestamp, pd.Timestamp] | None:
    match = re.search(r"(\d{2})-(\d{2})\.(\d{2})(?:\.(\d{4}))?", turnover_file.name)
    if not match:
        return None
    start_day = int(match.group(1))
    end_day = int(match.group(2))
    month = int(match.group(3))
    year = int(match.group(4)) if match.group(4) else pd.Timestamp(turnover_file.stat().st_mtime, unit="s").year
    try:
        return pd.Timestamp(year=year, month=month, day=start_day), pd.Timestamp(year=year, month=month, day=end_day)
    except ValueError:
        return None


def _filename_period_month(turnover_file: Path) -> str | None:
    match = re.search(r"(20\d{2})-(\d{2})", turnover_file.name)
    if not match:
        return None
    return f"{int(match.group(1)):04d}-{int(match.group(2)):02d}"


def _turnover_period(turnover_file: Path) -> tuple[pd.Timestamp, pd.Timestamp]:
    workbook = pd.read_excel(turnover_file)
    for col in ("Bill Date", "Pricing dt", "Created on"):
        if col not in workbook.columns:
            continue
        dates = pd.to_datetime(workbook[col], errors="coerce", dayfirst=True)
        dates = dates.dropna()
        if not dates.empty:
            return pd.Timestamp(dates.min()).normalize(), pd.Timestamp(dates.max()).normalize()
    parsed = _parse_turnover_period_from_filename(turnover_file)
    if parsed is not None:
        return parsed
    raise OperatorFacingError(
        "Turnover workbook import failed because no monthly date range could be derived. Keep the SAP date columns or use a filename like 'turnover report 01-31.03.xlsx'."
    )


def _prepare_turnover_rows(connection: sqlite3.Connection, turnover_file: Path) -> tuple[str, list[tuple]]:
    weights = load_turnover_weights(turnover_file=turnover_file)
    if weights.empty:
        raise OperatorFacingError(
            "Turnover workbook import failed because no Pirelli fitment weights could be matched. Check Material values against the current price list and canonical mapping."
        )

    period_start, period_end = _turnover_period(turnover_file)
    period_month = period_end.strftime("%Y-%m")
    filename_period_month = _filename_period_month(turnover_file)
    if filename_period_month is not None and filename_period_month != period_month:
        raise OperatorFacingError(
            "Turnover workbook import failed because the filename month does not match the workbook billing period. "
            f"Filename suggests {filename_period_month}, but the workbook dates indicate {period_month}. "
            "Rename the file or upload the correct monthly SQ00 export."
        )
    imported_at = connection.execute("SELECT datetime('now')").fetchone()[0]
    rows = [
        (
            turnover_file.stem,
            "Sheet1",
            turnover_file.name,
            period_start.strftime("%Y-%m-%d"),
            period_end.strftime("%Y-%m-%d"),
            period_month,
            str(row.analysis_fitment_key),
            float(row.turnover_weight),
            imported_at,
        )
        for row in weights.itertuples(index=False)
    ]
    return period_month, rows


def _replace_turnover_rows(connection: sqlite3.Connection, turnover_file: Path) -> None:
    period_month, rows = _prepare_turnover_rows(connection, turnover_file)
    connection.execute("DELETE FROM ref_turnover_weights WHERE period_month = ?", (period_month,))
    if rows:
        connection.executemany(
            """
            INSERT INTO ref_turnover_weights (
                reference_version,
                source_sheet,
                source_file_name,
                period_start_date,
                period_end_date,
                period_month,
                analysis_fitment_key,
                turnover_weight,
                imported_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )


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


def refresh_turnover_reference_data(db_path: Path, turnover_file: Path) -> ReferenceRefreshResult:
    if not turnover_file.exists():
        raise OperatorFacingError(f"Turnover import cannot start because the workbook file is missing: {turnover_file}")

    with connect_sqlite(db_path) as connection:
        try:
            _replace_turnover_rows(connection, turnover_file)
            _record_refresh(connection, "turnover_workbook", turnover_file, _file_sha256(turnover_file), "succeeded")
        except OperatorFacingError as exc:
            _record_refresh(connection, "turnover_workbook", turnover_file, _file_sha256(turnover_file), "failed", str(exc))
            raise
        except Exception as exc:
            _record_refresh(connection, "turnover_workbook", turnover_file, _file_sha256(turnover_file), "failed", str(exc))
            raise OperatorFacingError(
                "Turnover workbook import failed. Check that the SAP SQ00 file still includes Material and billing dates and that Pirelli materials match the current reference price list.",
                cause=exc,
            ) from exc
        connection.commit()
    return ReferenceRefreshResult(db_path=db_path, refreshed_scopes=["turnover_workbook"])


def get_turnover_reference_status(
    db_path: Path,
    *,
    today_value: date | None = None,
    snapshot_date: object | None = None,
) -> TurnoverReferenceStatus:
    expected_period_month = (
        _previous_month_key_for_snapshot(snapshot_date)
        if snapshot_date is not None
        else _previous_month_key(today_value)
    )
    if expected_period_month is None:
        expected_period_month = _previous_month_key(today_value)
    with connect_sqlite(db_path) as connection:
        try:
            row = connection.execute(
                """
                SELECT period_month, period_end_date, source_file_name
                FROM ref_turnover_weights
                ORDER BY period_month DESC, imported_at_utc DESC
                LIMIT 1
                """
            ).fetchone()
            expected_count = connection.execute(
                "SELECT COUNT(*) FROM ref_turnover_weights WHERE period_month = ?",
                (expected_period_month,),
            ).fetchone()[0]
        except sqlite3.OperationalError:
            row = None
            expected_count = 0
    latest_period_month = str(row[0]) if row and row[0] is not None else None
    latest_period_end_date = str(row[1]) if row and row[1] is not None else None
    latest_source_file_name = str(row[2]) if row and row[2] is not None else None
    return TurnoverReferenceStatus(
        expected_period_month=expected_period_month,
        latest_period_month=latest_period_month,
        latest_period_end_date=latest_period_end_date,
        latest_source_file_name=latest_source_file_name,
        is_missing_expected_month=expected_count == 0,
    )


def get_core_reference_status(db_path: Path) -> CoreReferenceStatus:
    required_tables = {
        "canonical mapping": "ref_canonical_fitment_mapping",
        "price list": "ref_price_list",
        "campaign customers": "ref_campaign_customer_discounts",
        "campaign pattern extras": "ref_campaign_pattern_extras",
    }
    missing_scopes: list[str] = []
    with connect_sqlite(db_path) as connection:
        for scope_name, table_name in required_tables.items():
            try:
                count = connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            except sqlite3.OperationalError:
                count = 0
            if not count:
                missing_scopes.append(scope_name)
    return CoreReferenceStatus(missing_scopes=tuple(missing_scopes))


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

            turnover_file = _latest_turnover_workbook(source_dir)
            if turnover_file is not None:
                _replace_turnover_rows(connection, turnover_file)
                _record_refresh(connection, "turnover_workbook", turnover_file, _file_sha256(turnover_file), "succeeded")
                scopes.append("turnover_workbook")
        except Exception as exc:
            source_file = campaign_file
            if "mapping" in str(exc).lower():
                source_file = mapping_file
            elif "price" in str(exc).lower():
                source_file = price_list_file
            elif "turnover" in str(exc).lower():
                turnover_file = _latest_turnover_workbook(source_dir)
                if turnover_file is not None:
                    source_file = turnover_file
            _record_refresh(connection, "reference_refresh", source_file, _file_sha256(source_file), "failed", str(exc))
            raise OperatorFacingError(
                "Reference refresh failed. Check that the campaign, canonical mapping, price list, and turnover workbooks still match the expected sheet structure.",
                cause=exc,
            ) from exc

        connection.commit()

    return ReferenceRefreshResult(db_path=db_path, refreshed_scopes=scopes)
