import logging
import structlog
from config.settings import settings


class InterceptHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            level = structlog.stdlib.NAME_TO_LEVEL[record.levelname.lower()]
        except KeyError:
            level = record.levelno
        structlog.get_logger(record.name).log(
            level,
            record.getMessage(),
            exc_info=record.exc_info,
        )


def configure_logging() -> None:
    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)

    processors = [
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if settings.is_development:
        processors.append(structlog.dev.ConsoleRenderer())
    else:
        processors.append(structlog.processors.JSONRenderer())

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
    )

    intercept = InterceptHandler()
    logging.basicConfig(handlers=[intercept], level=log_level, force=True)

    for uvicorn_logger in ["uvicorn", "uvicorn.error", "uvicorn.access"]:
        uv_log = logging.getLogger(uvicorn_logger)
        uv_log.handlers = [intercept]
        uv_log.propagate = False

    # Silence noisy internal loggers
    for noisy in ["asyncio", "asyncpg", "websockets"]:
        logging.getLogger(noisy).setLevel(logging.WARNING)
