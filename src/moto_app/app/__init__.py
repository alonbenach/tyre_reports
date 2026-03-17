"""Backend app shell for headless and future UI-triggered execution."""

from .service import WeeklyRunResult, run_weekly_pipeline

__all__ = ["WeeklyRunResult", "run_weekly_pipeline"]
