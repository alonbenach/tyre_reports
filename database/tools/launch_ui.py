from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from moto_app.ui import launch_operator_ui  # noqa: E402


if __name__ == "__main__":
    launch_operator_ui(ROOT)
