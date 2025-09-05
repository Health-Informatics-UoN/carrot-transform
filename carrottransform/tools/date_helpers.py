import datetime
import re

from carrottransform.tools.logger import logger_setup

logger = logger_setup()


def get_datetime_value(item: str) -> datetime.datetime | None:
    """
    Check if a date item is non-null and parses as ISO (YYYY-MM-DD), reverse-ISO (DD-MM-YYYY),
    or UK format (DD/MM/YYYY).
    Returns a datetime object if successful, None otherwise.
    """
    date_formats = [
        "%Y-%m-%d",  # ISO format (YYYY-MM-DD)
        "%d-%m-%Y",  # Reverse ISO format (DD-MM-YYYY)
        "%d/%m/%Y",  # UK old-style format (DD/MM/YYYY)
    ]

    for date_format in date_formats:
        try:
            return datetime.datetime.strptime(item, date_format)
        except ValueError:
            continue

    # If we get here, none of the formats worked
    return None


def normalise_to8601(item: str) -> str | None:
    """parses, normalises, and formats a date value using regexes

    could use just one regex but that seems bad.
    """

    both = item.split(" ")

    match = re.match(r"(?P<year>\d{4})[-/](?P<month>\d{2})[-/](?P<day>\d{2})", both[0])
    if not match:
        match = re.match(
            r"(?P<day>\d{2})[-/](?P<month>\d{2})[-/](?P<year>\d{4})", both[0]
        )

    if not match:
        logger.warning(f"{item} couldn't be normalised to ISO 8601 date format")
        return None
    data = match.groupdict()
    year, month, day = data["year"], data["month"], data["day"]
    value = str(int(year)).zfill(4)
    value += "-"
    value += str(int(month)).zfill(2)
    value += "-"
    value += str(int(day)).zfill(2)
    value += " "

    if 2 == len(both):
        match = re.match(
            r"(?P<hour>\d{2}):(?P<minute>\d{2})(:(?P<second>\d{2})(\.\d{6})?)?", both[1]
        )
        if match:
            data = match.groupdict()
            hour, minute, second = data["hour"], data["minute"], data["second"]
        else:
            hour, minute, second = None, None, None

        # concat the time_suffix
        if hour is not None:
            if minute is None:
                raise Exception(
                    f"unrecognized format seems to have 'hours' but not 'minutes' {item=}"
                )

            value += str(int(hour)).zfill(2)
            value += ":"
            value += str(int(minute)).zfill(2)
            value += ":"
            value += str(int(second if second is not None else "0")).zfill(2)

    if ":" not in value:
        value += "00:00:00"

    return value
