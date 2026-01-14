import sys
import os

from datetime import timezone

from loguru import logger

logger.remove()


def to_utc(record):
    if not os.getenv("DEBUG"):
        record["time"] = record["time"].astimezone(timezone.utc)
    return True


logger.add(
    sys.stdout,
    colorize=True,
    filter=to_utc
)
