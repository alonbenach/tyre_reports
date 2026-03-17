from __future__ import annotations

import sys
from pathlib import Path

from PySide6.QtCore import QThread, QTimer, Qt, QUrl, Signal
from PySide6.QtGui import QColor, QDesktopServices, QFont, QPalette, QTextCursor
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
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
    QTabWidget,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from moto_app.app import run_weekly_pipeline
from moto_app.config import AppConfig, ensure_runtime_dirs, load_config
from moto_app.exports import list_generated_reports
from moto_app.observability import list_runs, latest_run_status, operator_message_for_exception
from moto_app.ui.content import APP_TITLE, INSTRUCTIONS_TEXT


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
        self.last_log_path: Path | None = None

        self.setWindowTitle(APP_TITLE)
        self.resize(1220, 800)
        self.setMinimumSize(1080, 700)
        self._apply_palette()
        self._build_ui()
        self._refresh_all()

        self.timer = QTimer(self)
        self.timer.timeout.connect(self._refresh_all)
        self.timer.start(1500)

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
            QCheckBox {
                color: #17313b;
                spacing: 8px;
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

        self.home_status = QLabel("No runs yet")
        self.home_snapshot = QLabel("-")
        self.home_error = QLabel("-")
        self.home_db = QLabel(str(self.config.database_dir / "moto_pipeline_tmp.db"))
        self.home_reports = QLabel(str(self.config.reports_dir))
        self.home_history_count = QLabel("0")
        self.home_outputs_count = QLabel("0")

        cards = [
            ("Last run", self.home_status),
            ("Snapshot", self.home_snapshot),
            ("Recent runs", self.home_history_count),
            ("Outputs", self.home_outputs_count),
        ]
        for title, widget in cards:
            cards_row.addWidget(self._metric_card(title, widget), 1)
        layout.addLayout(cards_row)

        detail_row = QHBoxLayout()
        detail_row.setSpacing(12)

        group = QGroupBox("Current Status Details")
        grid = QGridLayout(group)
        rows = [
            ("Last run status", self.home_status),
            ("Snapshot", self.home_snapshot),
            ("Last error", self.home_error),
            ("Database", self.home_db),
            ("Reports folder", self.home_reports),
        ]
        for row, (label, widget) in enumerate(rows):
            grid.addWidget(QLabel(label + ":"), row, 0)
            widget.setWordWrap(True)
            grid.addWidget(widget, row, 1)
        detail_row.addWidget(group, 2)

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

    def _build_run_tab(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)

        top = QHBoxLayout()
        top.setSpacing(12)

        controls = QGroupBox("Weekly Run Controls")
        controls_layout = QGridLayout(controls)

        self.csv_path = QLineEdit(str(self.config.data_dir / "2026-03-10.csv"))
        browse_button = QPushButton("Browse")
        browse_button.clicked.connect(self._browse_csv)
        self.include_pdf = QCheckBox("Generate PDF outputs")
        self.include_pdf.setChecked(self.config.include_pdf_by_default)
        self.replace_snapshot = QCheckBox("Replace snapshot if it already exists")
        self.replace_snapshot.setChecked(True)
        self.refresh_references = QCheckBox("Refresh reference data before run")
        self.start_button = QPushButton("Start Weekly Run")
        self.start_button.clicked.connect(self._start_run)

        controls_layout.addWidget(QLabel("Weekly CSV"), 0, 0, 1, 2)
        controls_layout.addWidget(self.csv_path, 1, 0)
        controls_layout.addWidget(browse_button, 1, 1)
        controls_layout.addWidget(self.include_pdf, 2, 0, 1, 2)
        controls_layout.addWidget(self.replace_snapshot, 3, 0, 1, 2)
        controls_layout.addWidget(self.refresh_references, 4, 0, 1, 2)
        controls_layout.addWidget(self.start_button, 5, 0, 1, 2, alignment=Qt.AlignLeft)

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
        title = QLabel("Latest Outputs")
        title_font = QFont()
        title_font.setBold(True)
        title.setFont(title_font)
        controls.addWidget(title)
        controls.addStretch(1)
        open_reports = QPushButton("Open Reports Folder")
        open_reports.clicked.connect(lambda: self._open_path(self.config.reports_dir))
        controls.addWidget(open_reports)
        layout.addLayout(controls)

        group = QGroupBox("Generated Reports")
        group_layout = QVBoxLayout(group)
        self.outputs_table = QTableWidget(0, 5)
        self.outputs_table.setHorizontalHeaderLabels(["Generated", "Report", "Format", "Snapshot", "Path"])
        self._configure_table(
            self.outputs_table,
            stretch_last=True,
            widths=[160, 130, 80, 110, 520],
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
            self.csv_path.setText(selected)

    def _start_run(self) -> None:
        if self.worker is not None and self.worker.isRunning():
            QMessageBox.information(self, APP_TITLE, "A run is already in progress.")
            return

        source_file = Path(self.csv_path.text().strip())
        if not source_file.exists():
            QMessageBox.critical(self, APP_TITLE, "The selected weekly CSV does not exist.")
            return

        self.start_button.setEnabled(False)
        self.run_summary.setText("Run started. Waiting for backend progress...")
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
        self._refresh_home(db_path)
        self._refresh_history(db_path)
        self._refresh_outputs(db_path)
        self._refresh_run_panel(db_path)

    def _refresh_home(self, db_path: Path) -> None:
        status = latest_run_status(db_path)
        self.home_status.setText(status.status or "No runs yet")
        self.home_snapshot.setText(status.snapshot_date or "-")
        self.home_error.setText(status.error_message or "-")
        self.home_history_count.setText(str(len(list_runs(db_path, limit=20))))
        self.home_outputs_count.setText(str(len(list_generated_reports(db_path, limit=20))))
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

    def _refresh_outputs(self, db_path: Path) -> None:
        rows = list_generated_reports(db_path, limit=20)
        self.outputs_table.setRowCount(len(rows))
        for idx, report in enumerate(rows):
            values = [
                report.generated_at_utc,
                report.report_type,
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
        item = self.outputs_table.item(selected, 4)
        if item is None:
            return
        self._open_path(Path(item.text()))

    def _open_path(self, path: Path) -> None:
        if not path.exists():
            QMessageBox.critical(self, APP_TITLE, f"Path does not exist:\n{path}")
            return
        QDesktopServices.openUrl(QUrl.fromLocalFile(str(path.resolve())))


def launch_operator_ui(app_root: Path | None = None) -> None:
    config = load_config(app_root)
    ensure_runtime_dirs(config)
    app = QApplication.instance() or QApplication(sys.argv)
    window = MotoOperatorWindow(config)
    window.showMaximized()
    app.exec()
