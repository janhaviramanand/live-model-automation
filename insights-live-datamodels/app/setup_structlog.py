import logging
import os
import sys

import structlog
from pythonjsonlogger import jsonlogger
from structlog.threadlocal import wrap_dict

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.stdlib.render_to_log_kwargs,
    ],
    context_class=wrap_dict(dict),
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(jsonlogger.JsonFormatter(timestamp=True))


def get_logger(name):
    logger = structlog.getLogger(name)
    logger.addHandler(handler)
    logger.setLevel(os.getenv('LOG_LEVEL', 'INFO'))
    return logger
