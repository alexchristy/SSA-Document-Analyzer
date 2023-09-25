from datetime import datetime
import re

def reformat_date(date_str, current_date):
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
            raise ValueError("Could not parse date")
        
        day = int(search_result.group('day'))
        month = search_result.group('month')[:3].lower()
        
        current_year = current_date.year
        current_month = current_date.month
        
        # Special case for early January when the current month is December
        if month == "jan" and day <= 4 and current_month == 12:
            inferred_year = current_year + 1
        else:
            inferred_year = current_year

        date_obj = datetime.strptime(f"{day} {month} {inferred_year}", "%d %b %Y")
        return date_obj.strftime('%Y%m%d')
        
    except Exception as e:
        print(f"An error occurred: {e}")
        return date_str

def create_datetime_from_str(date_str):

    try:
        # Parse the input string to extract year, month, and day
        year = int(date_str[:4])
        month = int(date_str[4:6])
        day = int(date_str[6:8])

        # Create a datetime object using the extracted values
        custom_date = datetime(year=year, month=month, day=day)

        return custom_date

    except Exception as e:
        print(f"An error occurred: {e}")
        return None