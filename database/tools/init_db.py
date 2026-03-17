from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from database.tools import DatabasePaths, initialize_database  # noqa: E402


def main() -> None:
    result = initialize_database(
        DatabasePaths(
            db_path=ROOT / "database" / "moto_pipeline.db",
            migrations_dir=ROOT / "database" / "migrations",
        )
    )
    print(f"Database: {result.db_path}")
    print(f"Applied migrations: {', '.join(result.applied_versions) or 'none'}")
    print(f"Skipped migrations: {', '.join(result.skipped_versions) or 'none'}")


if __name__ == "__main__":
    main()
