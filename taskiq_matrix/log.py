import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logging.Formatter("%(threadname)s - %(asctime)s - %(name)s - %(levelname)s - %(message)s")


class Logger:
    def log(self, message: str, log_level: str = "info"):
        level = logging.getLevelName(log_level.upper())
        logger.log(level, message)
