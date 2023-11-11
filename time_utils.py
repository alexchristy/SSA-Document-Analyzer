from datetime import datetime

import pytz


def get_local_time(timezone_key: str) -> datetime:
    """Get the current local time in the specified timezone.

    Args:
    ----
        timezone_key (str): The timezone key to use.

    Returns:
    -------
        datetime: The current local time in the specified timezone.
    """
    # Ensure the timezone key is valid
    if timezone_key not in pytz.all_timezones:
        msg = f"Invalid timezone key: {timezone_key}"
        raise ValueError(msg)

    # Get the timezone object
    timezone = pytz.timezone(timezone_key)

    # Get the current time in the specified timezone
    return datetime.now(tz=timezone)


def pad_time_string(time_str: str) -> str:
    """Pad the input time string with zeros if it's not 4 characters long.

    Args:
    ----
        time_str (str): The time string in 24-hour format.

    Returns:
    -------
        str: The time string padded with leading zeros if necessary.
    """
    if not isinstance(time_str, str):
        msg = "time_str must be a string"
        raise AttributeError(msg)

    time_str = str(time_str)

    # Needs to be at least 4 characters long
    # e.g. 0000, 0100, 1000, 2359
    min_valid_length = 4

    # Check if the length of the string is less than 4
    if len(time_str) < min_valid_length:
        # Pad the string with zeros on the left to make it 4 characters long
        time_str = time_str.zfill(4)

    return time_str
