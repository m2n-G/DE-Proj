"""WebSocket to Kafka collector entry point."""

from src.common.logger import get_logger

logger = get_logger(__name__)


def main() -> None:
    logger.info("Collector template ready. Implement WebSocket subscription here.")


if __name__ == "__main__":
    main()

