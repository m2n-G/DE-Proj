"""Bronze to Silver cleaning Lambda."""

from src.common.logger import get_logger

logger = get_logger(__name__)


def lambda_handler(event, context):
    logger.info("Clean Lambda invoked")
    return {"statusCode": 200, "body": "clean lambda template ready"}

