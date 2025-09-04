import datetime

from carrottransform.tools.logger import logger_setup

logger = logger_setup()


def valid_value(value: str) -> bool:
    """Check if a value is valid (not empty/null)"""
    return value.strip() != ""


def valid_date_value(item: str) -> bool:
    """
    Check if a date item is non null and parses as ISO (YYYY-MM-DD), reverse-ISO
    or dd/mm/yyyy or mm/dd/yyyy
    """
    if item.strip() == "":
        return False
    if (
        not _valid_iso_date(item)
        and not _valid_reverse_iso_date(item)
        and not _valid_uk_date(item)
    ):
        logger.warning(f"{item} is not a valid/supported date format")
        return False
    return True


def _valid_iso_date(item: str) -> bool:
    """
    Check if a date item is non null and parses as ISO (YYYY-MM-DD)
    """
    try:
        datetime.datetime.strptime(item, "%Y-%m-%d")
    except ValueError:
        return False

    return True


def _valid_reverse_iso_date(item: str) -> bool:
    """
    Check if a date item is non null and parses as reverse ISO (DD-MM-YYYY)
    """
    try:
        datetime.datetime.strptime(item, "%d-%m-%Y")
    except ValueError:
        return False

    return True


def _valid_uk_date(item: str) -> bool:
    """
    Check if a date item is non null and parses as UK format (DD/MM/YYYY)
    """
    try:
        datetime.datetime.strptime(item, "%d/%m/%Y")
    except ValueError:
        return False

    return True
