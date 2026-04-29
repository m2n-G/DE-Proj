"""Kafka to S3 Bronze consumer entry point."""

from src.common.logger import get_logger

logger = get_logger(__name__)


def main() -> None:
    logger.info("Bronze consumer template ready. Implement Kafka consume and S3 put here.")


if __name__ == "__main__":
    main()

