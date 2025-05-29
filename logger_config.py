import logging


def setup_logging(level=logging.INFO):
    logging.basicConfig(
        level=level,
        # format="%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s",
        format="%(asctime)s - %(levelname)s - %(funcName)s - %(message)s",
        force=True,  # This ensures your config is applied even if basicConfig was called elsewhere
    )


# Default setup with INFO level
setup_logging()
