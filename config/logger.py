# Databricks notebook source
import logging
from logging.handlers import RotatingFileHandler

def get_logger(name="app", level=logging.DEBUG, log_file="app.log"):
    """
    Configures and returns a logger with rotating file handler.

    Args:
        name (str): Name of the logger.
        level (int): Logging level (e.g., logging.DEBUG, logging.INFO).
        log_file (str): Path to the file where logs will be stored.

    Returns:
        logger: Configured logger object.
    """

    # ✅ Create the logger
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Avoid adding multiple handlers if already configured
    if not logger.hasHandlers():
        # ✅ Format for log messages
        log_format = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        # ✅ Rotating file handler (2MB max, 5 backups)
        file_handler = RotatingFileHandler(
            filename=log_file,
            maxBytes=2 * 1024 * 1024,  # 2 MB
            backupCount=5,            # keep 5 old logs
            encoding="utf-8"
        )
        file_handler.setFormatter(log_format)
        file_handler.setLevel(level)

        # ✅ Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(log_format)
        console_handler.setLevel(level)

        # ✅ Add handlers
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger
