import logging


def setup_logging(level: int = logging.INFO) -> None:
    """Configure root logging once for the whole application."""
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )
    # requests/urllib3 are noisy at DEBUG; keep them at WARNING
    logging.getLogger("urllib3").setLevel(logging.WARNING)
