import itertools
import json
import logging
import re
from collections import Counter
from typing import Dict, List


def parse_rollcall_time(time_str: str) -> str or None:
    """Parse a string representing a rollcall time and returns the time as a string in the format 'HHMM'.

    Args:
    ----
        time_str (str): A string representing a rollcall time, in one of the following formats:
            - 'HH:MM'
            - 'H:MM'
            - 'HHMM'
            - 'HMM'

    Returns:
    -------
        str or None: The rollcall time as a string in the format 'HHMM', or None if the input string is not a valid rollcall time.

    """
    if not time_str:
        logging.info("Rollcall time is empty or None.")
        return None

    # Regex patterns to exclude unwanted matches
    exclusion_patterns = [
        re.compile(r"(?:\s|-)\d{3}(?:\D|$)"),  # exclude " 123" or "-123"
    ]

    for pattern in exclusion_patterns:
        match = pattern.search(time_str)
        if match:
            logging.info(
                "Excluding match due to exclusion pattern: '%s'", match.group()
            )
            return None

    # Two different regex patterns for valid time formats
    patterns = [
        re.compile(r"(?<=\D)(\d{1,2}):?(\d{2})(?=\D|$)"),
        re.compile(r"^(?:\D)*(\d{1,2}):?(\d{2})(?=\D|$)"),
    ]

    match = None
    for pattern in patterns:
        match = pattern.search(time_str)
        if match:
            break

    if not match:
        logging.error("Failed to parse rollcall time: Invalid format '%s'", time_str)
        return None

    hour, minute = map(int, match.groups())

    largest_valid_hour = 23
    largest_valid_minute = 59
    if hour > largest_valid_hour or minute > largest_valid_minute:
        logging.error("Failed to parse rollcall time: Invalid time '%s'", time_str)
        return None

    rollcall_time = f"{hour:02d}{minute:02d}"
    logging.info("Parsed rollcall time: %s", rollcall_time)

    return rollcall_time


def has_multiple_rollcall_times(time_str: str) -> bool:
    """Check if a string contains multiple rollcall times.

    Args:
    ----
        time_str (str): A string that may contain one or more rollcall times, in one of the following formats:
            - 'HH:MM'
            - 'H:MM'
            - 'HHMM'
            - 'HMM'

    Returns:
    -------
        bool: True if the input string contains multiple rollcall times, False otherwise.

    """
    logging.info("Checking if '%s' has multiple rollcall times", time_str)
    count = 0
    while time_str.strip():  # Skip empty or white-space only strings
        parsed_time = parse_rollcall_time(time_str)
        if parsed_time:
            count += 1
            # Remove the parsed time and strip
            old_time_str = time_str
            time_str = time_str.replace(parsed_time, "", 1).strip()

            if old_time_str == time_str:
                logging.error(
                    "Failed to remove parsed time '%s' from '%s'", parsed_time, time_str
                )

                # Try 'HH:MM' format
                time_str = time_str.replace(
                    f"{parsed_time[:2]}:{parsed_time[2:]}", "", 1
                ).strip()
        else:
            break

    logging.info("Found %d rollcall times", count)

    # If there are more than one rollcall times, return True
    return count > 1


def ocr_combo_correction(input_str: str, correction_map: Dict[str, str]) -> List[str]:
    """Correct OCR errors in a string by generating all possible combinations of corrections.

    Args:
    ----
        input_str (str): The input string to correct.
        correction_map (Dict[str, str]): A dictionary mapping OCR errors to their corrected values.

    Returns:
    -------
        List[str]: A list of all possible corrected versions of the input string.

    """
    input_str = input_str.replace("TDB", "TBD")
    possibilities = []
    for i, char in enumerate(input_str):
        # Special case: replace '8' if it's between 'T' and 'D'
        if char == "8" and i > 0 and i < len(input_str) - 1:
            prev_char = input_str[i - 1].upper()
            next_char = input_str[i + 1].upper()
            if prev_char == "T" and next_char == "D":
                possibilities.append(["B"])
                continue
        # Special case: keep 'B' if it's between 'T' and 'D'
        elif char.upper() == "B" and i > 0 and i < len(input_str) - 1:
            prev_char = input_str[i - 1].upper()
            next_char = input_str[i + 1].upper()
            if prev_char == "T" and next_char == "D":
                possibilities.append([char])
                continue
        # General case
        possibilities.append([correction_map.get(char, char), char])

    return ["".join(p) for p in itertools.product(*possibilities)]


def parse_seat_data(seat_data: str) -> List[List[int or str]]:
    """Parse a string of seat data and returns a list of lists containing the seat number and status.

    Args:
    ----
        seat_data (str): A string containing seat data in the format '1t 2f 3t'.

    Returns:
    -------
        List[List[int or str]]: A list of lists containing the seat number and status, e.g. [[1, 'T'], [2, 'F'], [3, 'T']].
    """
    correction_map = {"O": "0", "I": "1", "l": "1", "S": "5", "Z": "2", "B": "8"}

    seat_data = seat_data.replace("_", "-")
    format_freq = Counter()

    combined_pattern = r"\b(?P<num>\d+)(?P<status>[tf])\b|\b(?P<status1>[tf])-?(?P<num1>\d+)\b|\b(?P<num2>\d+)\s*(?P<status2>[tf])\b|\b(?P<status3>[tf])\.?(?P<num3>\d+)\b"

    all_results = []

    def try_parsing(corrected_str: str) -> None:
        nonlocal all_results, format_freq
        results = []
        index = 0
        while True:
            tbd_pos = corrected_str.upper().find("TBD", index)
            if tbd_pos == -1:
                break
            results.append({"data": [0, "TBD"], "format": "TBD", "index": tbd_pos})
            format_freq["TBD"] += 1
            index = tbd_pos + 3

        for match in re.finditer(combined_pattern, corrected_str, re.IGNORECASE):
            num_of_seats = int(
                match.group("num")
                or match.group("num1")
                or match.group("num2")
                or match.group("num3")
            )
            seat_status = (
                match.group("status")
                or match.group("status1")
                or match.group("status2")
                or match.group("status3")
            ).upper()
            fmt = match.lastgroup
            index = match.start()
            format_freq[fmt] += 1
            results.append(
                {"data": [num_of_seats, seat_status], "format": fmt, "index": index}
            )

        results.sort(
            key=lambda x: x["index"]
        )  # Sort by index to preserve original order
        all_results.append(results)

    corrected_versions = ocr_combo_correction(seat_data, correction_map)[
        :50
    ]  # Limit to the first 50 versions for performance
    for corrected_str in corrected_versions:
        try_parsing(corrected_str)

    most_frequent_format = format_freq.most_common(1)[0][0] if format_freq else None

    # Choose the most frequent format for the result
    if most_frequent_format:
        final_results = next(
            (r for r in all_results if r[0]["format"] == most_frequent_format),
            all_results[0],
        )
    else:
        final_results = all_results[0]

    # Deduplicate results, but keep all "TBD"
    final_results = [
        list(x["data"])
        for i, x in enumerate(final_results)
        if x["data"][1] == "TBD"
        or all(x["data"] != y["data"] for y in final_results[:i])
    ]

    if final_results:
        logging.info("Parsed seat data: %s", final_results)
    else:
        logging.error("Failed to parse seat data: Invalid format '%s'", seat_data)

    return final_results


def ocr_correction(input_str: str) -> str:
    """Correct common OCR errors in a string.

    Args:
    ----
        input_str (str): The input string to be corrected.

    Returns:
    -------
        str: The corrected string.
    """
    correction_map = {"O": "0", "I": "1", "l": "1", "S": "5", "Z": "2", "B": "8"}
    return "".join(correction_map.get(char, char) for char in input_str)


def combine_sequential_duplicates(destinations: List[str]) -> List[str]:
    """Combine sequential duplicates in a list of destinations.

    Args:
    ----
        destinations (List[str]): A list of destination strings.

    Returns:
    -------
        List[str]: A list of destination strings with sequential duplicates combined.
    """
    combined = []
    prev_dest = None

    for dest in destinations:
        if dest == prev_dest:
            combined[-1] = f"{combined[-1]}, {dest}"
        else:
            combined.append(dest)
        prev_dest = dest

    return combined


def parse_destination(destination_data: str) -> str:
    """Parse the destination data using GPT-3 Turbo.

    Args:
    ----
        destination_data (str): The destination data to be parsed.

    Returns:
    -------
        str: The parsed destination data.
    """
    from gpt3_turbo_analysis import GPT3TurboAnalysis
    from note_extract_utils import _extract_parenthesis_notes

    # If destination data is empty, return empty string
    if destination_data == "":
        logging.info("Destination data is empty.")
        return None

    # Initialize GPT analysis object
    gpt3_turbo_analysis = GPT3TurboAnalysis()

    # Use the extract_notes function to remove any notes from the input text
    notes = _extract_parenthesis_notes(destination_data)
    for note in notes:
        destination_data = destination_data.replace(note, "")

    # Analyze the input text using GPT-3 Turbo
    returned_str = gpt3_turbo_analysis.get_destination_analysis(destination_data)

    # If GPT determines there are no destinations, return None
    if returned_str is None or returned_str == "None":
        return None

    # Capitalize all the destinations
    returned_str = returned_str.upper()

    # Parse the returned string into a list of destinations
    try:
        destinations = json.loads(returned_str)
    except Exception as e:
        logging.error(
            "An error occurred processing GPT returned destinations. Error: %s", e
        )
        logging.error("Returned string: %s", returned_str)
        return None

    # Combine sequential duplicates
    return combine_sequential_duplicates(destinations)
