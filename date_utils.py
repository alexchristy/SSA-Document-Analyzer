import datetime
import re
from typing import Optional, Union


def reformat_date(date_str: str, current_date: datetime.datetime) -> str:
    """Reformat a date string to the format YYYYMMDD.

    Args:
    ----
        date_str: A string representing a date in one of several formats.
        current_date: A datetime object representing the current date.

    Returns:
    -------
        A string representing the reformatted date in the format YYYYMMDD, or the original date string if it could not be parsed.

    Raises:
    ------
        None.

    Examples:
    --------
        >>> reformat_date("1st January, 2022", datetime(2022, 1, 1))
        '20220101'
        >>> reformat_date("2022-13-31", datetime(2022, 1, 1))
        '2022-13-31'
    """
    # Original pattern
    original_pattern = r"(?P<day>\d{1,2})(?:th|st|nd|rd)?(?:\s*,?\s*)?(?P<month>jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"

    # New pattern for the specific case (month-day-year)
    new_pattern = r"(?P<month>[a-zA-Z]+)\s*(?P<day>\d{1,2})[,\s]*(?P<year>\d{4})?"

    try:
        # First, try to match using the original pattern
        search_result = re.search(original_pattern, date_str, re.IGNORECASE)

        # If the original pattern doesn't match, try the new pattern
        if not search_result:
            search_result = re.search(new_pattern, date_str, re.IGNORECASE)

        # If still no match, raise an error
        if not search_result:
            msg = "Could not parse date"
            raise ValueError(msg)

        day = int(search_result.group("day"))
        month = search_result.group("month")[:3].lower()

        current_year = current_date.year
        current_month = current_date.month

        # Special case for early January when the current month is December
        december = 12
        january_threshold = 4
        if month == "jan" and day <= january_threshold and current_month == december:
            inferred_year = current_year + 1
        else:
            inferred_year = current_year

        date_obj = datetime.datetime.strptime(
            f"{day} {month} {inferred_year}", "%d %b %Y"
        ).replace(tzinfo=datetime.timezone.utc)

        return date_obj.strftime("%Y%m%d")

    except Exception as e:
        print(f"An error occurred: {e}")
        return date_str


def create_datetime_from_str(date_str: str) -> Optional[datetime.datetime]:
    """Create a datetime object from a string in the format YYYYMMDD.

    Args:
    ----
        date_str: A string representing a date in the format YYYYMMDD.

    Returns:
    -------
        A datetime object representing the input date.

    Raises:
    ------
        None.

    Examples:
    --------
        >>> create_datetime_from_str("20220101")
        datetime.datetime(2022, 1, 1, 0, 0)
    """
    try:
        # Parse the input string to extract year, month, and day
        year = int(date_str[:4])
        month = int(date_str[4:6])
        day = int(date_str[6:8])

        # Create a datetime object using the extracted values
        return datetime.datetime(
            year=year, month=month, day=day, tzinfo=datetime.timezone.utc
        )

    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def check_date_string(
    input_string: str, return_match: bool = False
) -> Optional[Union[bool, str]]:
    """Check if a given string is a valid date string in one of several formats.

    Args:
    ----
        input_string: A string to check for a valid date string.
        return_match: A boolean indicating whether to return the matched date string.

    Returns:
    -------
        If `return_match` is False (default), returns a boolean indicating whether the input string is a valid date string.
        If `return_match` is True, returns the matched date string as a string, or None if no match was found.

    Raises:
    ------
        None.

    Examples:
    --------
        >>> check_date_string("1st January, 2022")
        True
        >>> check_date_string("2022-13-31")
        False
        >>> check_date_string("2022/12/31", return_match=True)
        None
        >>> check_date_string("2022/12/31", return_match=True)
        '2022/12/31'
    """
    date_patterns = [
        r"(?i)\d{1,2}(?:th|st|nd|rd)?\s*(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s*,?\s*\d{4}",
        r"(?i)\d{4}\s*(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s*\d{1,2}(?:th|st|nd|rd)?",
        r"(?i)\d{1,2}(?:th|st|nd|rd)?\s+(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?),\s+\d{4}",
        r"(?i)\d{1,2}(?:th|st|nd|rd)?\s+(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+\d{4}",
        r"(?i)\d{4}\s+(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+\d{1,2}(?:th|st|nd|rd)?",
        r"(?i)\d{1,2}\s+(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)(?:,\s+\d{4})?",
        r"(?i)(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+\d{1,2}(?:th|st|nd|rd)?(?:,\s+\d{4})?",
    ]

    for pattern in date_patterns:
        match = re.search(pattern, input_string)
        if return_match and match:
            return match.group(0)

        if match:
            return True

    if return_match:
        return None

    return False
