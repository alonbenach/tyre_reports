from __future__ import annotations

import shutil
import sys
from pathlib import Path

from datetime import date

from PySide6.QtCore import QDate, QThread, QTimer, Qt, QUrl, Signal
from PySide6.QtGui import QColor, QDesktopServices, QFont, QPalette, QTextCursor
from PySide6.QtWidgets import (
    QAbstractSpinBox,
    QApplication,
    QCheckBox,
    QComboBox,
    QDateEdit,
    QFileDialog,
    QFrame,
    QGridLayout,
    QGroupBox,
    QHeaderView,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QPlainTextEdit,
    QProgressBar,
    QTabWidget,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from moto_app.access_control import (
    AccessControlError,
    AccessSession,
    acquire_access_session,
    enable_admin_mode,
    evaluate_access,
    lock_owner_summary,
    refresh_access_heartbeat,
    release_access_session,
    recover_stale_lock_session,
)
from moto_app.app import run_weekly_pipeline
from moto_app.config import AppConfig, ensure_runtime_dirs, load_config
from moto_app.exports import list_current_generated_reports, list_generated_reports
from moto_app.ingest import duplicate_snapshot_message
from moto_app.ingest import remove_staged_intake_file
from moto_app.observability import (
    YearCoverage,
    list_runs,
    list_year_coverage,
    latest_run_status,
    operator_message_for_exception,
)
from moto_app.ui.content import APP_TITLE, INSTRUCTIONS_TEXT

REPORT_OPTIONS = [
    ("positioning", "Price Positioning"),
    ("offeror_focus", "Offeror Focus"),
]
REPORT_SLUGS = {
    "positioning": "price_positioning",
    "offeror_focus": "offeror_focus",
}


class CsvDropZone(QFrame):
    file_dropped = Signal(str)

    def __init__(self) -> None:
        super().__init__()
        self.setAcceptDrops(True)
        self.setObjectName("csvDropZone")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 14, 16, 14)
        layout.setSpacing(4)
        title = QLabel("Drop weekly CSV here")
        title.setStyleSheet("font-weight: 700; color: #0f2229;")
        subtitle = QLabel("Drag a CSV from Downloads or click Browse below.")
        subtitle.setStyleSheet("color: #586a72;")
        subtitle.setWordWrap(True)
        layout.addWidget(title)
        layout.addWidget(subtitle)

    def dragEnterEvent(self, event) -> None:  # type: ignore[override]
        if self._extract_csv_path(event.mimeData()) is not None:
            event.acceptProposedAction()
        else:
            event.ignore()

    def dropEvent(self, event) -> None:  # type: ignore[override]
        file_path = self._extract_csv_path(event.mimeData())
        if file_path is None:
            event.ignore()
            return
        self.file_dropped.emit(file_path)
        event.acceptProposedAction()

    @staticmethod
    def _extract_csv_path(mime_data) -> str | None:
        if not mime_data.hasUrls():
            return None
        for url in mime_data.urls():
            local_path = url.toLocalFile()
            if local_path.lower().endswith(".csv"):
                return local_path
        return None


class SnapshotDateEdit(QDateEdit):
    def wheelEvent(self, event) -> None:  # type: ignore[override]
        event.ignore()


class RunWorker(QThread):
    finished_ok = Signal()
    failed = Signal(str)

    def __init__(
        self,
        *,
        config: AppConfig,
        source_file: Path,
        include_pdf: bool,
        replace_snapshot: bool,
        refresh_references: bool,
    ) -> None:
        super().__init__()
        self.config = config
        self.source_file = source_file
        self.include_pdf = include_pdf
        self.replace_snapshot = replace_snapshot
        self.refresh_references = refresh_references

    def run(self) -> None:
        try:
            ensure_runtime_dirs(self.config)
            run_weekly_pipeline(
                db_path=self.config.database_dir / "moto_pipeline_tmp.db",
                source_file=self.source_file,
                raw_dir=self.config.raw_archive_dir,
                report_dir=self.config.reports_dir,
                log_dir=self.config.logs_dir,
                include_pdf=self.include_pdf,
                replace_snapshot=self.replace_snapshot,
                refresh_references=self.refresh_references,
                reference_dir=self.config.reference_source_dir,
            )
            self.finished_ok.emit()
        except Exception as exc:
            self.failed.emit(operator_message_for_exception(exc))


class MotoOperatorWindow(QMainWindow):
    def __init__(self, config: AppConfig) -> None:
        super().__init__()
        self.config = config
        self.worker: RunWorker | None = None
        self.access_session: AccessSession | None = None
        self.last_log_path: Path | None = None
        self.pending_source_file: Path | None = None
        self.selected_source_label: QLabel | None = None
        self.staged_name_label: QLabel | None = None
        self.coverage_cards_layout: QHBoxLayout | None = None
        self.access_mode_detail: QLabel | None = None
        self.access_owner_detail: QLabel | None = None
        self.access_admin_detail: QLabel | None = None
        self.access_help_detail: QLabel | None = None
        self.admin_enable_button: QPushButton | None = None
        self.recover_lock_button: QPushButton | None = None

        self.setWindowTitle(APP_TITLE)
        self.resize(1220, 800)
        self.setMinimumSize(1080, 700)
        self._apply_palette()
        self._build_ui()
        self._initialize_access_mode()
        self._refresh_all()

        self.timer = QTimer(self)
        self.timer.timeout.connect(self._refresh_all)
        self.timer.start(1500)
        self.lock_timer = QTimer(self)
        self.lock_timer.timeout.connect(self._refresh_access_heartbeat)
        self.lock_timer.start(self.config.lock_heartbeat_seconds * 1000)

    def _apply_palette(self) -> None:
        palette = self.palette()
        palette.setColor(QPalette.Window, QColor("#eef2f0"))
        palette.setColor(QPalette.WindowText, QColor("#17313b"))
        palette.setColor(QPalette.Base, QColor("#ffffff"))
        palette.setColor(QPalette.AlternateBase, QColor("#f5f8f7"))
        palette.setColor(QPalette.Text, QColor("#17313b"))
        palette.setColor(QPalette.Button, QColor("#dfe8e4"))
        palette.setColor(QPalette.ButtonText, QColor("#17313b"))
        palette.setColor(QPalette.Highlight, QColor("#0f6d7a"))
        palette.setColor(QPalette.HighlightedText, QColor("#ffffff"))
        self.setPalette(palette)
        self.setStyleSheet(
            """
            QMainWindow { background: #eef2f0; }
            QWidget { color: #17313b; }
            QLabel { color: #17313b; background: transparent; }
            QTabWidget::pane { border: 0; }
            QTabBar::tab {
                background: #dfe8e4;
                color: #17313b;
                padding: 10px 18px;
                margin-right: 6px;
                border-top-left-radius: 8px;
                border-top-right-radius: 8px;
                font-weight: 600;
            }
            QTabBar::tab:selected { background: #ffffff; }
            QGroupBox {
                background: #ffffff;
                border: 1px solid #d8e2de;
                border-radius: 12px;
                margin-top: 14px;
                font-weight: 600;
                color: #17313b;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 14px;
                padding: 0 4px 0 4px;
            }
            QFrame#csvDropZone {
                background: #f7faf9;
                border: 2px dashed #8fb0b5;
                border-radius: 12px;
            }
            QCheckBox {
                color: #17313b;
                spacing: 8px;
            }
            QComboBox {
                border: 1px solid #d8e2de;
                border-radius: 8px;
                background: #ffffff;
                color: #17313b;
                padding: 6px 10px;
                min-width: 160px;
            }
            QComboBox::drop-down {
                subcontrol-origin: padding;
                subcontrol-position: top right;
                width: 28px;
                border-left: 1px solid #d8e2de;
                background: #f4f8f6;
                border-top-right-radius: 8px;
                border-bottom-right-radius: 8px;
            }
            QComboBox::down-arrow {
                width: 10px;
                height: 10px;
            }
            QComboBox QAbstractItemView {
                background: #ffffff;
                color: #17313b;
                selection-background-color: #0f6d7a;
                selection-color: #ffffff;
                border: 1px solid #d8e2de;
                outline: 0;
            }
            QDateEdit {
                border: 1px solid #d8e2de;
                border-radius: 8px;
                background: #ffffff;
                color: #17313b;
                padding: 6px 10px;
                min-width: 150px;
                max-width: 170px;
            }
            QDateEdit::drop-down {
                subcontrol-origin: padding;
                subcontrol-position: top right;
                width: 28px;
                border-left: 1px solid #d8e2de;
                background: #f4f8f6;
                border-top-right-radius: 8px;
                border-bottom-right-radius: 8px;
            }
            QDateEdit::down-arrow {
                width: 10px;
                height: 10px;
            }
            QMenu, QListView, QAbstractItemView {
                background: #ffffff;
                color: #17313b;
                selection-background-color: #0f6d7a;
                selection-color: #ffffff;
                border: 1px solid #d8e2de;
                outline: 0;
            }
            QCalendarWidget QWidget {
                background: #ffffff;
                color: #17313b;
            }
            QCalendarWidget QToolButton {
                background: #dfe8e4;
                color: #17313b;
                border: 0;
                border-radius: 6px;
                padding: 6px;
                font-weight: 600;
            }
            QCalendarWidget QMenu {
                background: #ffffff;
                color: #17313b;
            }
            QCalendarWidget QSpinBox {
                background: #ffffff;
                color: #17313b;
                border: 1px solid #d8e2de;
                border-radius: 6px;
            }
            QCalendarWidget QAbstractItemView {
                background: #ffffff;
                color: #17313b;
                selection-background-color: #0f6d7a;
                selection-color: #ffffff;
                alternate-background-color: #f4f8f6;
                outline: 0;
            }
            QPushButton {
                background: #0f6d7a;
                color: #ffffff;
                border: 0;
                border-radius: 8px;
                padding: 8px 14px;
                font-weight: 600;
            }
            QPushButton:disabled { background: #8aa5aa; color: #e8efef; }
            QLineEdit, QPlainTextEdit, QTextEdit, QTableWidget {
                border: 1px solid #d8e2de;
                border-radius: 8px;
                background: #ffffff;
                color: #17313b;
                selection-background-color: #0f6d7a;
                selection-color: #ffffff;
                gridline-color: #d8e2de;
            }
            QHeaderView::section {
                background: #dfe8e4;
                color: #17313b;
                padding: 8px;
                border: 0;
                border-right: 1px solid #d1dbd7;
                border-bottom: 1px solid #d1dbd7;
                font-weight: 600;
            }
            QTableWidget {
                alternate-background-color: #f4f8f6;
            }
            QTableWidget::item {
                color: #17313b;
                padding: 6px;
            }
            QTableWidget::item:selected {
                background: #0f6d7a;
                color: #ffffff;
            }
            QPlainTextEdit, QTextEdit {
                padding: 8px;
                line-height: 1.35em;
            }
            QProgressBar {
                border: 1px solid #d8e2de;
                border-radius: 7px;
                background: #f4f8f6;
                text-align: center;
                color: #17313b;
                min-height: 18px;
            }
            QProgressBar::chunk {
                background: #0f6d7a;
                border-radius: 6px;
            }
            QMessageBox {
                background: #eef2f0;
            }
            QMessageBox QLabel {
                color: #17313b;
                min-width: 260px;
            }
            QMessageBox QPushButton {
                min-width: 88px;
            }
            """
        )

    def _build_ui(self) -> None:
        central = QWidget()
        layout = QVBoxLayout(central)
        layout.setContentsMargins(18, 16, 18, 18)
        layout.setSpacing(12)

        title = QLabel(APP_TITLE)
        title.setStyleSheet("color: #0f2229;")
        title_font = QFont("Segoe UI", 18)
        title_font.setBold(True)
        title.setFont(title_font)
        layout.addWidget(title)

        subtitle = QLabel("Weekly motorcycle pipeline control for non-technical operators")
        subtitle.setStyleSheet("color: #586a72;")
        layout.addWidget(subtitle)

        self.access_banner = QLabel("")
        self.access_banner.setWordWrap(True)
        self.access_banner.setStyleSheet(
            "background: #dff1ee; border: 1px solid #b8ddd6; border-radius: 10px; padding: 10px 12px; color: #12424a; font-weight: 600;"
        )
        layout.addWidget(self.access_banner)

        self.tabs = QTabWidget()
        layout.addWidget(self.tabs, 1)

        self.tabs.addTab(self._build_home_tab(), "Home")
        self.tabs.addTab(self._build_run_tab(), "Weekly Run")
        self.tabs.addTab(self._build_history_tab(), "Run History")
        self.tabs.addTab(self._build_outputs_tab(), "Outputs")
        self.tabs.addTab(self._build_instructions_tab(), "Instructions")

        self.setCentralWidget(central)

    def _build_home_tab(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)

        cards_row = QHBoxLayout()
        cards_row.setSpacing(12)

        self.home_status_card = QLabel("No runs yet")
        self.home_status = QLabel("No runs yet")
        self.home_snapshot = QLabel("-")
        self.home_error = QLabel("-")
        self.home_db = QLabel(str(self.config.database_dir / "moto_pipeline_tmp.db"))
        self.home_reports = QLabel(str(self.config.reports_dir))
        self.home_history_count = QLabel("0")
        self.home_outputs_count = QLabel("0")

        cards = [
            ("Last run", self.home_status_card),
            ("Recent runs", self.home_history_count),
            ("Outputs", self.home_outputs_count),
        ]
        for title, widget in cards:
            cards_row.addWidget(self._metric_card(title, widget), 1)
        cards_row.addStretch(1)
        layout.addLayout(cards_row)

        coverage_group = QGroupBox("Database Coverage")
        coverage_layout = QVBoxLayout(coverage_group)
        coverage_subtitle = QLabel(
            "Loaded weeks in the SQLite store for the current year and previous year."
        )
        coverage_subtitle.setStyleSheet("color: #586a72;")
        coverage_layout.addWidget(coverage_subtitle)
        self.coverage_cards_layout = QHBoxLayout()
        self.coverage_cards_layout.setSpacing(12)
        coverage_layout.addLayout(self.coverage_cards_layout)
        layout.addWidget(coverage_group)

        detail_row = QHBoxLayout()
        detail_row.setSpacing(12)

        group = QGroupBox("Current Status Details")
        grid = QGridLayout(group)
        rows = [
            ("Last run status", self.home_status),
            ("Latest snapshot", self.home_snapshot),
            ("Last error", self.home_error),
            ("Database", self.home_db),
            ("Reports folder", self.home_reports),
        ]
        for row, (label, widget) in enumerate(rows):
            grid.addWidget(QLabel(label + ":"), row, 0)
            widget.setWordWrap(True)
            grid.addWidget(widget, row, 1)
        detail_row.addWidget(group, 2)

        access_group = QGroupBox("Access Control")
        access_layout = QGridLayout(access_group)
        self.access_mode_detail = QLabel("-")
        self.access_owner_detail = QLabel("-")
        self.access_admin_detail = QLabel("-")
        self.access_help_detail = QLabel("-")
        self.access_help_detail.setWordWrap(True)
        access_rows = [
            ("Session mode", self.access_mode_detail),
            ("Writable owner", self.access_owner_detail),
            ("Admin controls", self.access_admin_detail),
            ("Support note", self.access_help_detail),
        ]
        for row, (label, widget) in enumerate(access_rows):
            access_layout.addWidget(QLabel(label + ":"), row, 0)
            access_layout.addWidget(widget, row, 1)
        access_actions = QHBoxLayout()
        self.admin_enable_button = QPushButton("Enable Admin Controls")
        self.admin_enable_button.clicked.connect(self._enable_admin_mode)
        self.recover_lock_button = QPushButton("Recover Stale Lock")
        self.recover_lock_button.clicked.connect(self._recover_stale_lock)
        access_actions.addWidget(self.admin_enable_button)
        access_actions.addWidget(self.recover_lock_button)
        access_actions.addStretch(1)
        access_layout.addLayout(access_actions, len(access_rows), 0, 1, 2)
        detail_row.addWidget(access_group, 2)

        summary_group = QGroupBox("Latest Run Log Summary")
        summary_layout = QVBoxLayout(summary_group)
        self.home_log_summary = QPlainTextEdit()
        self.home_log_summary.setReadOnly(True)
        self.home_log_summary.setMaximumBlockCount(200)
        self.home_log_summary.setStyleSheet("background: #f7faf9; font-family: Consolas;")
        summary_layout.addWidget(self.home_log_summary)
        detail_row.addWidget(summary_group, 3)

        layout.addLayout(detail_row, 1)
        layout.addStretch(1)
        return tab

    def _metric_card(self, title: str, value_label: QLabel) -> QFrame:
        card = QFrame()
        card.setStyleSheet(
            "QFrame { background: #ffffff; border: 1px solid #d8e2de; border-radius: 12px; }"
        )
        layout = QVBoxLayout(card)
        title_label = QLabel(title)
        title_label.setStyleSheet("color: #586a72; font-size: 12px;")
        value_label.setStyleSheet("color: #0f2229; font-size: 24px; font-weight: 700;")
        value_label.setWordWrap(True)
        layout.addWidget(title_label)
        layout.addWidget(value_label)
        layout.addStretch(1)
        return card

    def _coverage_card(self, coverage: YearCoverage) -> QFrame:
        card = QFrame()
        card.setStyleSheet(
            "QFrame { background: #ffffff; border: 1px solid #d8e2de; border-radius: 12px; }"
        )
        layout = QVBoxLayout(card)
        title = QLabel(str(coverage.iso_year))
        title.setStyleSheet("color: #0f2229; font-size: 18px; font-weight: 700;")
        subtitle = QLabel(
            f"Data loaded through week {coverage.latest_week:02d} of 52"
        )
        subtitle.setStyleSheet("color: #586a72;")
        progress = QProgressBar()
        progress.setRange(0, 52)
        progress.setValue(len(coverage.weeks_present))
        progress.setFormat(f"{coverage.coverage_percent}% coverage")
        weeks = QLabel(
            "Weeks present: " + ", ".join(f"{week:02d}" for week in coverage.weeks_present)
        )
        weeks.setWordWrap(True)
        weeks.setStyleSheet("color: #17313b;")
        layout.addWidget(title)
        layout.addWidget(subtitle)
        layout.addWidget(progress)
        layout.addWidget(weeks)
        layout.addStretch(1)
        return card

    def _refresh_home_coverage(self, db_path: Path) -> None:
        if self.coverage_cards_layout is None:
            return
        while self.coverage_cards_layout.count():
            item = self.coverage_cards_layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()
        current_year = date.today().year
        years = (current_year, current_year - 1)
        coverage_rows = list_year_coverage(db_path, years=years)
        if not coverage_rows:
            empty = QLabel("No silver snapshots available yet.")
            empty.setStyleSheet("color: #586a72;")
            self.coverage_cards_layout.addWidget(empty)
            self.coverage_cards_layout.addStretch(1)
            return
        for coverage in coverage_rows:
            self.coverage_cards_layout.addWidget(self._coverage_card(coverage), 1)
        self.coverage_cards_layout.addStretch(1)

    def _build_run_tab(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)

        top = QHBoxLayout()
        top.setSpacing(12)

        controls = QGroupBox("Weekly Run Controls")
        controls_layout = QGridLayout(controls)

        self.snapshot_date_input = SnapshotDateEdit()
        self.snapshot_date_input.setCalendarPopup(True)
        self.snapshot_date_input.setDisplayFormat("yyyy-MM-dd")
        self.snapshot_date_input.setDate(QDate.currentDate())
        self.snapshot_date_input.setButtonSymbols(QAbstractSpinBox.UpDownArrows)
        self.snapshot_date_input.setKeyboardTracking(False)
        self.snapshot_date_input.lineEdit().setReadOnly(True)
        self.snapshot_date_input.dateChanged.connect(self._on_snapshot_date_changed)
        self.csv_path = QLineEdit(str(self._staged_target_path()))
        self.csv_path.setReadOnly(True)
        self.csv_drop_zone = CsvDropZone()
        self.csv_drop_zone.file_dropped.connect(self._try_set_csv_path)
        self.browse_button = QPushButton("Browse")
        self.browse_button.clicked.connect(self._browse_csv)
        self.stage_button = QPushButton("Stage CSV to Intake")
        self.stage_button.clicked.connect(self._stage_pending_source)
        self.remove_staged_button = QPushButton("Remove Selected Staged Snapshot")
        self.remove_staged_button.clicked.connect(self._remove_selected_staged_snapshot)
        self.run_snapshot_selector = QComboBox()
        self.include_pdf = QCheckBox("Generate PDF outputs")
        self.include_pdf.setChecked(self.config.include_pdf_by_default)
        self.replace_snapshot = QCheckBox("Replace snapshot if it already exists")
        self.replace_snapshot.setChecked(True)
        self.refresh_references = QCheckBox("Refresh reference data before run")
        self.start_button = QPushButton("Start Weekly Run")
        self.start_button.clicked.connect(self._start_run)
        self.selected_source_label = QLabel("No file selected yet.")
        self.selected_source_label.setWordWrap(True)
        self.staged_name_label = QLabel(self._selected_snapshot_date() + ".csv")
        self.staged_name_label.setWordWrap(True)

        controls_layout.addWidget(QLabel("1. Choose weekly CSV"), 0, 0, 1, 2)
        controls_layout.addWidget(self.csv_drop_zone, 1, 0, 1, 2)
        controls_layout.addWidget(QLabel("Snapshot date for staging"), 2, 0, 1, 2)
        controls_layout.addWidget(self.snapshot_date_input, 3, 0, 1, 1, alignment=Qt.AlignLeft)
        controls_layout.addWidget(QLabel("Pending source file"), 4, 0, 1, 2)
        controls_layout.addWidget(self.selected_source_label, 5, 0, 1, 2)
        controls_layout.addWidget(QLabel("Staged filename"), 6, 0, 1, 2)
        controls_layout.addWidget(self.staged_name_label, 7, 0, 1, 2)
        controls_layout.addWidget(QLabel("Staged intake path"), 8, 0, 1, 2)
        controls_layout.addWidget(self.csv_path, 9, 0)
        controls_layout.addWidget(self.browse_button, 9, 1)
        controls_layout.addWidget(self.stage_button, 10, 0, 1, 2, alignment=Qt.AlignLeft)
        controls_layout.addWidget(QLabel("2. Select staged snapshot to run"), 11, 0, 1, 2)
        controls_layout.addWidget(self.run_snapshot_selector, 12, 0, 1, 2)
        controls_layout.addWidget(self.remove_staged_button, 13, 0, 1, 2, alignment=Qt.AlignLeft)
        controls_layout.addWidget(self.include_pdf, 14, 0, 1, 2)
        controls_layout.addWidget(self.replace_snapshot, 15, 0, 1, 2)
        controls_layout.addWidget(self.refresh_references, 16, 0, 1, 2)
        controls_layout.addWidget(self.start_button, 17, 0, 1, 2, alignment=Qt.AlignLeft)

        self._refresh_staged_snapshots()

        status = QGroupBox("Live Run Status")
        status_layout = QGridLayout(status)
        self.run_status = QLabel("Idle")
        self.run_snapshot = QLabel("-")
        self.run_log = QLabel("-")
        self.run_log.setWordWrap(True)
        self.run_summary = QLabel("No run in progress.")
        self.run_summary.setWordWrap(True)
        rows = [
            ("Status", self.run_status),
            ("Snapshot", self.run_snapshot),
            ("Log file", self.run_log),
            ("Summary", self.run_summary),
        ]
        for row, (label, widget) in enumerate(rows):
            status_layout.addWidget(QLabel(label + ":"), row, 0)
            status_layout.addWidget(widget, row, 1)

        top.addWidget(controls, 1)
        top.addWidget(status, 1)
        layout.addLayout(top)

        log_group = QGroupBox("Run Log")
        log_layout = QVBoxLayout(log_group)
        self.log_text = QPlainTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setStyleSheet("background: #f7faf9; font-family: Consolas;")
        log_layout.addWidget(self.log_text)
        layout.addWidget(log_group, 1)
        return tab

    def _initialize_access_mode(self) -> None:
        self.access_session = acquire_access_session(self.config)
        self._apply_access_mode()

    def _apply_access_mode(self) -> None:
        session = self.access_session
        if session is None:
            return
        role = "Admin" if session.admin_mode_enabled else ("Admin-eligible" if session.is_admin_user else "Operator")
        if session.mode == "writable":
            self.access_banner.setStyleSheet(
                "background: #dff1ee; border: 1px solid #b8ddd6; border-radius: 10px; padding: 10px 12px; color: #12424a; font-weight: 600;"
            )
            banner_suffix = " Admin controls are enabled for this session." if session.admin_mode_enabled else ""
            self.access_banner.setText(f"{role} mode: writable session active for {session.user_name}@{session.machine_name}. {session.reason}{banner_suffix}")
            self.setWindowTitle(f"{APP_TITLE} [Admin]" if session.admin_mode_enabled else APP_TITLE)
        else:
            self.access_banner.setStyleSheet(
                "background: #fff4df; border: 1px solid #e4c88f; border-radius: 10px; padding: 10px 12px; color: #6b4b12; font-weight: 600;"
            )
            self.access_banner.setText(
                f"Read-only mode: {session.reason} Writable session owner: {lock_owner_summary(session.active_lock)}. Output viewing remains available, but staging and run actions are disabled."
            )
            self.setWindowTitle(f"{APP_TITLE} [Read-only]")

        can_write = session.mode == "writable"
        for widget in [
            self.csv_drop_zone,
            self.browse_button,
            self.stage_button,
            self.remove_staged_button,
            self.snapshot_date_input,
            self.run_snapshot_selector,
            self.include_pdf,
            self.replace_snapshot,
            self.start_button,
        ]:
            widget.setEnabled(can_write)
        self.refresh_references.setEnabled(can_write and session.admin_mode_enabled)
        self.remove_staged_button.setEnabled(can_write and session.admin_mode_enabled)
        if not (can_write and session.admin_mode_enabled):
            self.refresh_references.setChecked(False)

        latest_evaluation = evaluate_access(self.config) if session.mode == "read_only" else None
        if self.access_mode_detail is not None:
            self.access_mode_detail.setText("Writable" if session.mode == "writable" else "Read-only")
        if self.access_owner_detail is not None:
            self.access_owner_detail.setText(lock_owner_summary(session.active_lock))
        if self.access_admin_detail is not None:
            if session.admin_mode_enabled:
                self.access_admin_detail.setText("Enabled for this session")
            elif session.is_admin_user:
                self.access_admin_detail.setText("Eligible but not enabled")
            else:
                self.access_admin_detail.setText("Not available for this Windows user")
        if self.access_help_detail is not None:
            if session.mode == "writable":
                if session.admin_mode_enabled:
                    self.access_help_detail.setText("Admin controls are active. Reference refresh and stale-lock recovery actions are now available when relevant.")
                elif session.is_admin_user:
                    self.access_help_detail.setText("You may enable admin controls deliberately for support tasks. Normal weekly runs do not require admin mode.")
                else:
                    self.access_help_detail.setText("Normal operator mode is active. If another user reports read-only access, ask them to wait or contact an admin.")
            else:
                self.access_help_detail.setText(
                    "If read-only mode is unexpected, check who owns the writable session first. Clear a stale lock only after confirming the other session is no longer active."
                )
        if self.admin_enable_button is not None:
            self.admin_enable_button.setEnabled(session.mode == "writable" and session.is_admin_user and not session.admin_mode_enabled)
        if self.recover_lock_button is not None:
            self.recover_lock_button.setEnabled(
                session.mode == "read_only"
                and session.is_admin_user
                and latest_evaluation is not None
                and latest_evaluation.is_lock_stale
                and latest_evaluation.can_recover_stale_lock
            )

    def _refresh_access_heartbeat(self) -> None:
        if self.access_session is None:
            return
        self.access_session = refresh_access_heartbeat(self.config, self.access_session)
        self._apply_access_mode()

    def _enable_admin_mode(self) -> None:
        if self.access_session is None:
            return
        try:
            self.access_session = enable_admin_mode(self.config, self.access_session)
        except AccessControlError as exc:
            QMessageBox.information(self, APP_TITLE, str(exc))
            return
        self._apply_access_mode()
        QMessageBox.information(
            self,
            APP_TITLE,
            "Admin controls are now enabled for this session. Use them only for deliberate support or maintenance actions.",
        )

    def _recover_stale_lock(self) -> None:
        if self.access_session is None:
            return
        lock_summary = lock_owner_summary(self.access_session.active_lock)
        answer = QMessageBox.question(
            self,
            APP_TITLE,
            (
                "A stale writable-session lock can be recovered only after you confirm the other operator is no longer active.\n\n"
                f"Current lock owner: {lock_summary}\n\n"
                "Do you want to clear the stale lock and take the writable session?"
            ),
        )
        if answer != QMessageBox.Yes:
            return
        try:
            self.access_session = recover_stale_lock_session(self.config, self.access_session)
        except AccessControlError as exc:
            QMessageBox.information(self, APP_TITLE, str(exc))
            return
        self._apply_access_mode()
        QMessageBox.information(
            self,
            APP_TITLE,
            "The stale lock was recovered. This session now holds the writable lock and admin controls are enabled.",
        )

    def _build_history_tab(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        group = QGroupBox("Recent Runs")
        group_layout = QVBoxLayout(group)
        self.history_table = QTableWidget(0, 5)
        self.history_table.setHorizontalHeaderLabels(["Started", "Status", "Snapshot", "Source", "Error"])
        self._configure_table(
            self.history_table,
            stretch_last=True,
            widths=[160, 100, 110, 150, 520],
        )
        group_layout.addWidget(self.history_table)
        layout.addWidget(group)
        return tab

    def _build_outputs_tab(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)

        controls = QHBoxLayout()
        title = QLabel("Outputs by Report")
        title_font = QFont()
        title_font.setBold(True)
        title.setFont(title_font)
        controls.addWidget(title)
        controls.addSpacing(12)
        controls.addWidget(QLabel("Report"))
        self.report_selector = QComboBox()
        for report_type, label in REPORT_OPTIONS:
            self.report_selector.addItem(label, report_type)
        self.report_selector.currentIndexChanged.connect(self._refresh_outputs_view)
        self.report_selector.setMaxVisibleItems(len(REPORT_OPTIONS))
        controls.addWidget(self.report_selector)
        controls.addStretch(1)
        open_excel = QPushButton("Open Excel Folder")
        open_excel.clicked.connect(self._open_selected_excel_folder)
        controls.addWidget(open_excel)
        open_reports = QPushButton("Open Reports Folder")
        open_reports.clicked.connect(self._open_selected_report_folder)
        controls.addWidget(open_reports)
        layout.addLayout(controls)

        group = QGroupBox("Generated Files")
        group_layout = QVBoxLayout(group)
        self.outputs_table = QTableWidget(0, 4)
        self.outputs_table.setHorizontalHeaderLabels(["Generated", "Format", "Snapshot", "Path"])
        self._configure_table(
            self.outputs_table,
            stretch_last=True,
            widths=[180, 90, 110, 620],
        )
        group_layout.addWidget(self.outputs_table)
        open_selected = QPushButton("Open Selected Output")
        open_selected.clicked.connect(self._open_selected_output)
        group_layout.addWidget(open_selected, alignment=Qt.AlignRight)
        layout.addWidget(group, 1)
        return tab

    def _configure_table(
        self,
        table: QTableWidget,
        *,
        stretch_last: bool,
        widths: list[int],
    ) -> None:
        table.setAlternatingRowColors(True)
        table.setSelectionBehavior(QTableWidget.SelectRows)
        table.setSelectionMode(QTableWidget.SingleSelection)
        table.setEditTriggers(QTableWidget.NoEditTriggers)
        table.verticalHeader().setVisible(False)
        header = table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.Interactive)
        for idx, width in enumerate(widths):
            table.setColumnWidth(idx, width)
        if stretch_last:
            header.setStretchLastSection(True)

    def _build_instructions_tab(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        group = QGroupBox("Operator Instructions")
        group_layout = QVBoxLayout(group)
        text = QTextEdit()
        text.setReadOnly(True)
        text.setPlainText(INSTRUCTIONS_TEXT)
        text.setStyleSheet("background: #fffdf8;")
        group_layout.addWidget(text)
        layout.addWidget(group)
        return tab

    def _browse_csv(self) -> None:
        selected, _ = QFileDialog.getOpenFileName(
            self,
            "Select weekly CSV",
            str(self.config.data_dir),
            "CSV files (*.csv);;All files (*.*)",
        )
        if selected:
            self._try_set_csv_path(selected)

    def _try_set_csv_path(self, path: str) -> None:
        try:
            self.pending_source_file = Path(path)
            if self.selected_source_label is not None:
                self.selected_source_label.setText(self.pending_source_file.name)
        except Exception as exc:
            QMessageBox.critical(
                self,
                APP_TITLE,
                f"Could not stage the selected CSV for intake.\n\n{operator_message_for_exception(exc)}",
            )

    def _set_csv_path(self, path: str) -> None:
        self.csv_path.setText(path)

    def _on_snapshot_date_changed(self) -> None:
        if self.staged_name_label is not None:
            self.staged_name_label.setText(self._staged_target_path().name)
        self._set_csv_path(str(self._staged_target_path()))

    def _selected_snapshot_date(self) -> str:
        return self.snapshot_date_input.date().toString("yyyy-MM-dd")

    def _staged_target_path(self) -> Path:
        return self.config.intake_dir / f"{self._selected_snapshot_date()}.csv"

    def _stage_pending_source(self) -> None:
        if self.pending_source_file is None:
            QMessageBox.critical(
                self,
                APP_TITLE,
                "Select or drop a weekly CSV before staging it into intake.",
            )
            return
        try:
            staged_path = self._stage_source_file(self.pending_source_file)
        except Exception as exc:
            QMessageBox.critical(
                self,
                APP_TITLE,
                f"Could not stage the selected CSV for intake.\n\n{operator_message_for_exception(exc)}",
            )
            return
        self.csv_path.setText(str(staged_path))
        self._refresh_staged_snapshots(select_snapshot=staged_path.stem)
        if self.selected_source_label is not None:
            self.selected_source_label.setText(f"{self.pending_source_file.name} staged as {staged_path.name}")

    def _stage_source_file(self, source_path: Path) -> Path:
        if not source_path.exists():
            raise FileNotFoundError(source_path)
        if source_path.suffix.lower() != ".csv":
            raise ValueError("The selected file is not a CSV. Choose a .csv export before starting the run.")
        self.config.intake_dir.mkdir(parents=True, exist_ok=True)
        target_path = self._staged_target_path()
        resolved_source = source_path.resolve()
        resolved_target = target_path.resolve()
        if resolved_source == resolved_target:
            return target_path
        if target_path.exists():
            target_path.unlink()
        if source_path.parent == self.config.intake_dir and source_path.exists():
            shutil.move(str(source_path), str(target_path))
            return target_path
        shutil.copy2(source_path, target_path)
        return target_path

    def _refresh_staged_snapshots(self, select_snapshot: str | None = None) -> None:
        current_snapshot = select_snapshot or self.run_snapshot_selector.currentText()
        self.run_snapshot_selector.blockSignals(True)
        self.run_snapshot_selector.clear()
        staged_files = sorted(self.config.intake_dir.glob("*.csv"), reverse=True)
        for staged_file in staged_files:
            self.run_snapshot_selector.addItem(staged_file.stem, str(staged_file))
        if current_snapshot:
            index = self.run_snapshot_selector.findText(current_snapshot)
            if index >= 0:
                self.run_snapshot_selector.setCurrentIndex(index)
        self.run_snapshot_selector.blockSignals(False)

    def _remove_selected_staged_snapshot(self) -> None:
        if self.access_session is None or not self.access_session.admin_mode_enabled:
            QMessageBox.information(
                self,
                APP_TITLE,
                "Removing a staged intake file is an admin-only action. Enable admin controls first.",
            )
            return
        snapshot_date = self.run_snapshot_selector.currentText()
        if not snapshot_date:
            QMessageBox.information(self, APP_TITLE, "Select a staged snapshot first.")
            return
        answer = QMessageBox.question(
            self,
            APP_TITLE,
            (
                f"Remove the staged intake file for snapshot {snapshot_date}?\n\n"
                "Use this only when the wrong CSV was staged and you want to clear it before the next run."
            ),
        )
        if answer != QMessageBox.Yes:
            return
        try:
            removed_path = remove_staged_intake_file(self.config.intake_dir, snapshot_date)
        except Exception as exc:
            QMessageBox.critical(self, APP_TITLE, operator_message_for_exception(exc))
            return
        self._refresh_staged_snapshots()
        self.csv_path.setText(str(self._staged_target_path()))
        if self.selected_source_label is not None:
            self.selected_source_label.setText(f"Removed staged intake file {removed_path.name}.")
        QMessageBox.information(
            self,
            APP_TITLE,
            f"Removed staged intake file {removed_path.name}.",
        )

    def _start_run(self) -> None:
        if self.worker is not None and self.worker.isRunning():
            QMessageBox.information(self, APP_TITLE, "A run is already in progress.")
            return

        source_path_text = self.run_snapshot_selector.currentData()
        if not source_path_text:
            QMessageBox.critical(
                self,
                APP_TITLE,
                "Stage a CSV first, then choose the staged snapshot you want to run.",
            )
            return
        source_file = Path(str(source_path_text))
        if not source_file.exists():
            QMessageBox.critical(
                self,
                APP_TITLE,
                "The selected staged snapshot file does not exist. Stage the CSV again or choose another staged snapshot.",
            )
            return
        db_path = self.config.database_dir / "moto_pipeline_tmp.db"
        if not self.replace_snapshot.isChecked():
            duplicate_message = duplicate_snapshot_message(db_path, source_file)
            if duplicate_message is not None:
                QMessageBox.information(self, APP_TITLE, duplicate_message)
                return
        if self.refresh_references.isChecked() and (self.access_session is None or not self.access_session.admin_mode_enabled):
            QMessageBox.information(
                self,
                APP_TITLE,
                "Reference refresh is an admin-only action. Enable admin controls first if you intentionally need to refresh the reference workbooks.",
            )
            return

        self.start_button.setEnabled(False)
        self.run_summary.setText(
            f"Run started for snapshot {source_file.stem} from staged intake file {source_file.name}. Waiting for backend progress..."
        )
        self.worker = RunWorker(
            config=self.config,
            source_file=source_file,
            include_pdf=self.include_pdf.isChecked(),
            replace_snapshot=self.replace_snapshot.isChecked(),
            refresh_references=self.refresh_references.isChecked(),
        )
        self.worker.finished_ok.connect(self._on_run_finished)
        self.worker.failed.connect(self._on_run_failed)
        self.worker.finished.connect(self._on_worker_stopped)
        self.worker.start()

    def _on_run_finished(self) -> None:
        self.run_summary.setText("Latest run finished successfully.")
        self._refresh_all()

    def _on_run_failed(self, message: str) -> None:
        QMessageBox.critical(self, APP_TITLE, message)
        self.run_summary.setText(message)
        self._refresh_all()

    def _on_worker_stopped(self) -> None:
        self.start_button.setEnabled(True)

    def _refresh_all(self) -> None:
        db_path = self.config.database_dir / "moto_pipeline_tmp.db"
        self._refresh_staged_snapshots()
        self._refresh_home(db_path)
        self._refresh_history(db_path)
        self._refresh_outputs_view()
        self._refresh_run_panel(db_path)

    def _refresh_home(self, db_path: Path) -> None:
        status = latest_run_status(db_path)
        self.home_status_card.setText(status.status or "No runs yet")
        self.home_status.setText(status.status or "No runs yet")
        self.home_snapshot.setText(status.snapshot_date or "-")
        self.home_error.setText(status.error_message or "-")
        self.home_history_count.setText(str(len(list_runs(db_path, limit=20))))
        self.home_outputs_count.setText(str(len(list_generated_reports(db_path, limit=20))))
        self._refresh_home_coverage(db_path)
        if status.run_id:
            log_path = self.config.logs_dir / f"{status.run_id}.log"
            if log_path.exists():
                self.home_log_summary.setPlainText(log_path.read_text(encoding="utf-8"))
            else:
                self.home_log_summary.setPlainText("No log file found for the latest run.")
        else:
            self.home_log_summary.setPlainText("No run log available yet.")

    def _refresh_history(self, db_path: Path) -> None:
        rows = list_runs(db_path, limit=20)
        self.history_table.setRowCount(len(rows))
        for idx, run in enumerate(rows):
            values = [
                run.run_started_at_utc,
                run.status,
                run.snapshot_date or "-",
                run.source_file_name or "-",
                run.error_message or "",
            ]
            for col, value in enumerate(values):
                self.history_table.setItem(idx, col, QTableWidgetItem(str(value)))

    def _refresh_outputs_view(self) -> None:
        db_path = self.config.database_dir / "moto_pipeline_tmp.db"
        report_type = self.report_selector.currentData()
        rows = list_current_generated_reports(db_path, report_type=report_type)
        self.outputs_table.setRowCount(len(rows))
        for idx, report in enumerate(rows):
            values = [
                report.generated_at_utc,
                report.format,
                report.snapshot_date or "-",
                str(report.output_path),
            ]
            for col, value in enumerate(values):
                self.outputs_table.setItem(idx, col, QTableWidgetItem(str(value)))

    def _refresh_run_panel(self, db_path: Path) -> None:
        status = latest_run_status(db_path)
        self.run_status.setText(status.status or "Idle")
        self.run_snapshot.setText(status.snapshot_date or "-")
        if status.run_id:
            log_path = self.config.logs_dir / f"{status.run_id}.log"
            self.last_log_path = log_path
            self.run_log.setText(str(log_path))
            self._load_log(log_path)
        if status.error_message:
            self.run_summary.setText(status.error_message)
        elif status.status == "succeeded":
            self.run_summary.setText("Latest run finished successfully.")
        elif status.status == "running":
            self.run_summary.setText("Run in progress. Live log is updating below.")
        else:
            self.run_summary.setText("No run in progress.")

    def _load_log(self, log_path: Path) -> None:
        if not log_path.exists():
            return
        try:
            content = log_path.read_text(encoding="utf-8")
        except Exception:
            return
        if self.log_text.toPlainText() != content:
            self.log_text.setPlainText(content)
            self.log_text.moveCursor(QTextCursor.End)

    def _open_selected_output(self) -> None:
        selected = self.outputs_table.currentRow()
        if selected < 0:
            QMessageBox.information(self, APP_TITLE, "Select an output first.")
            return
        item = self.outputs_table.item(selected, 3)
        if item is None:
            return
        self._open_path(Path(item.text()))

    def _selected_report_type(self) -> str:
        return str(self.report_selector.currentData())

    def _selected_report_root(self) -> Path:
        return self.config.reports_dir / REPORT_SLUGS[self._selected_report_type()]

    def _open_selected_excel_folder(self) -> None:
        self._open_path(self._selected_report_root() / "excel")

    def _open_selected_report_folder(self) -> None:
        self._open_path(self._selected_report_root() / "reports")

    def _open_path(self, path: Path) -> None:
        if not path.exists():
            QMessageBox.critical(self, APP_TITLE, f"Path does not exist:\n{path}")
            return
        QDesktopServices.openUrl(QUrl.fromLocalFile(str(path.resolve())))

    def closeEvent(self, event) -> None:  # type: ignore[override]
        if self.access_session is not None:
            release_access_session(self.config, self.access_session)
        super().closeEvent(event)


def launch_operator_ui(app_root: Path | None = None) -> None:
    config = load_config(app_root)
    ensure_runtime_dirs(config)
    app = QApplication.instance() or QApplication(sys.argv)
    window = MotoOperatorWindow(config)
    window.showMaximized()
    app.exec()
