# utils/logger.py
import logging
import structlog
import sys

def configure_logging(level: str = "INFO"):
    timestamper = structlog.processors.TimeStamper(fmt="iso")
    pre_chain = [
        structlog.processors.add_log_level,
        timestamper,
    ]
    structlog.configure(
        processors=[
            structlog.stdlib.add_logger_name,
            structlog.stdlib.filter_by_level,
            *pre_chain,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.make_filtering_bound_logger(getattr(logging, level)),
        cache_logger_on_first_use=True,
    )

    # basic logging config
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, level),
    )

# 用法：
# from utils.logger import configure_logging
# configure_logging("INFO")
# import structlog
# log = structlog.get_logger(__name__)
# log.info("app_started", port=8000)