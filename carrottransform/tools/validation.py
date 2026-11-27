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


_TIME_FORMATS = ("", " %H:%M", " %H:%M:%S", " %H:%M:%S.%f")


def _valid_date_format(item: str, date_format: str) -> bool:
    """
    Check if a date or datetime item parses with the given date format and an optional time suffix.
    """
    for time_format in _TIME_FORMATS:
        try:
            datetime.datetime.strptime(item, date_format + time_format)
            return True
        except ValueError:
            continue

    return False


def _valid_iso_date(item: str) -> bool:
    """
    Check if a date/datetime item is non null and parses as ISO (YYYY-MM-DD[ HH:MM[:SS[.ffffff]]])
    """
    return _valid_date_format(item, "%Y-%m-%d")


def _valid_reverse_iso_date(item: str) -> bool:
    """
    Check if a date/datetime item is non null and parses as reverse ISO (DD-MM-YYYY[ HH:MM[:SS[.ffffff]]])
    """
    return _valid_date_format(item, "%d-%m-%Y")


def _valid_uk_date(item: str) -> bool:
    """
    Check if a date/datetime item is non null and parses as UK format (DD/MM/YYYY[ HH:MM[:SS[.ffffff]]])
    """
    return _valid_date_format(item, "%d/%m/%Y")
