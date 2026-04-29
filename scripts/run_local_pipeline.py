"""Local dry-run entry point for the MVP pipeline."""

from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.common.parser import parse_trade_message


def main() -> None:
    sample_path = PROJECT_ROOT / "sample_data" / "raw_messages.log"
    for raw_message in sample_path.read_text(encoding="utf-8").splitlines():
        if not raw_message.strip():
            continue
        print(parse_trade_message(raw_message, trade_date="20240115"))


if __name__ == "__main__":
    main()
