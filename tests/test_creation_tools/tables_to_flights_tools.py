import json
import logging
import os
import pickle
import re
import sys
from typing import List, Optional

# Current file's directory
current_dir = os.path.dirname(os.path.abspath(__file__))
# Parent directory of `current_dir` (tests)
parent_dir = os.path.dirname(current_dir)
# Parent of `parent_dir` (SSA-Document-Analyzer)
grandparent_dir = os.path.dirname(parent_dir)

# Add grandparent directory to sys.path
sys.path.append(grandparent_dir)

from table import Table  # noqa: E402 (Requires sys.path to be appended)


def create_tables_from_lambda_event(event_dict_str: str, output_path: str) -> None:
    """Create pickled table objects and creates pretty printed table representations in text files.

    Args:
    ----
        event_dict_str (str): A string representation of the event from the Process-72HR-Flights lambda function cloudwatch logs.
        output_path (str): The path to the directory where the pickled tables and text files will be saved.

    Returns:
    -------
        None
    """
    if not event_dict_str:
        msg = "No event dictionary string provided"
        raise ValueError(msg)

    if not output_path:
        msg = "No output path provided"
        raise ValueError(msg)

    try:
        event = json.loads(event_dict_str)
    except json.JSONDecodeError as e:
        print(f"Error decoding the event dictionary string: {e!s}")
        raise

    event_tables = event.get("tables", [])
    pdf_hash = event.get("pdf_hash", "")
    job_id = event.get("job_id", "")

    if not event_tables:
        response_msg = f"No tables found in payload: {event}"
        raise ValueError(response_msg)

    if not pdf_hash:
        response_msg = f"No pdf_hash found in payload: {event}"
        raise ValueError(response_msg)

    if not job_id:
        response_msg = f"No job_id found in payload: {event}"
        raise ValueError(response_msg)

    tables: List[Table] = []

    for i, table_dict in enumerate(event_tables):
        curr_table = Table.from_dict(table_dict)

        if curr_table is None:
            logging.info("Failed to convert table %d from a dictionary", i)
            continue

        tables.append(curr_table)

    if not tables:
        logging.info("No tables were successfully converted from the event dictionary")
        return

    for i, table in enumerate(tables):
        table_name = f"table_{i}"
        table_path = os.path.join(output_path, table_name)

        with open(table_path + ".pkl", "wb") as f:
            pickle.dump(table, f)

        with open(table_path + ".txt", "w") as f:
            f.write(table.to_markdown())


def convert_to_dict(data_str: str) -> Optional[dict]:
    """Convert a string that looks like a dictionary into a Python dictionary.

    This function replaces single quotes with double quotes, except for single quotes within strings already enclosed in double quotes.

    Args:
    ----
        data_str (str): The string to convert to a dictionary.

    Returns:
    -------
        dict: The converted dictionary.
    """

    # Function to replace single quotes with double quotes, ignoring already double-quoted strings
    def replace_outside_quotes(match: re.Match) -> str:
        # Check if the matched string is within double quotes
        if '"' in match.group(0):
            return match.group(
                0
            )  # Return the original string if it's within double quotes
        return match.group(0).replace(
            "'", '"'
        )  # Replace single quotes with double quotes otherwise

    # Regular expression to match all instances of single quotes, considering the presence of double quotes
    pattern = r"(\".*?\"|'.*?')"

    # Replace single quotes with double quotes where appropriate
    formatted_str = re.sub(pattern, replace_outside_quotes, data_str, flags=re.DOTALL)

    # Convert the string to a dictionary
    try:
        return json.loads(formatted_str)
    except json.JSONDecodeError as e:
        print(f"Error in converting to dictionary: {e}")
        return None


def remove_cloudwatch_prefix(data_str: str) -> str:
    """Remove the CloudWatch prefix from the beginning of the string.

    Args:
    ----
        data_str (str): The string to remove the CloudWatch prefix from.

    Returns:
    -------
        str: The string with the CloudWatch prefix removed.
    """
    # Regular expression to match the CloudWatch prefix
    pattern = (
        r"\[INFO\]\t\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z\t[a-f0-9-]+\tEvent: "
    )

    # Remove the CloudWatch prefix
    return re.sub(pattern, "", data_str)


def create_tables_from_cloudwatch_log(
    data_str: str, output_path: str = current_dir
) -> None:
    """Create pickled table objects and creates pretty printed table representations in text files."""
    if not data_str:
        msg = "No data string provided"
        raise ValueError(msg)

    # Remove the CloudWatch prefix from the data string
    data_str = remove_cloudwatch_prefix(data_str)

    # Convert the data string to a dictionary
    event_dict = convert_to_dict(data_str)

    if event_dict is None:
        msg = "Error in converting to dictionary"
        raise ValueError(msg)

    # Create the tables from the event dictionary
    create_tables_from_lambda_event(
        event_dict_str=json.dumps(event_dict), output_path=current_dir
    )


create_tables_from_cloudwatch_log(
    """[INFO]	2023-12-28T12:17:28.126Z	a54e08a1-ff7a-4ef3-9cc8-0a8f37f8fce0	Event: {'tables': [{'title': 'DEPARTURES FROM: YOKOTAAB, JAPAN (OKO) SUNDAY, DEC 31ST', 'title_confidence': 65.771484375, 'footer': '', 'footer_confidence': 0.0, 'table_confidence': 99.609375, 'page_number': 7, 'rows': [[['ROLL CALL', 89.697265625], ['DESTINATION', 96.337890625], ['SEATS', 87.255859375]], [['-', 89.55078125], ['**PASSENGER TERMINAL WILL BE CLOSED** **NORMAL OPERATIONS WILL RESUME ON TUESDAY, JAN 2ND AT 0600L**', 96.142578125], ['-', 87.109375]]], 'table_number': 3}, {'title': 'DEPARTURES FROM : YOKOTAAB, JAPAN (OKO) FRIDAY, DEC 29TH', 'title_confidence': 82.275390625, 'footer': '', 'footer_confidence': 0.0, 'table_confidence': 99.853515625, 'page_number': 1, 'rows': [[['ROLL CALL', 94.23828125], ['DESTINATION', 96.240234375], ['SEATS', 92.041015625]], [['', 82.91015625], ['MCAS IWAKUNI, JAPAN **PATRIOT EXPRESS** **PRE-BOOK PASSENGERS REPORT NO LATER THAN 1200(L)** **EARLY BIRD CHECK-IN IS ONLY AVAILABLE FOR PRE-BOOKED PASSENGERS @ 1800-2000L (Day prior to departure) BRING ALL CHECKED BAGGAGE (PET w/KENNEL)**', 92.333984375], ['TBD', 88.28125]], [['1035L', 82.91015625], ['KADENA AIR BASE, JAPAN **PATRIOT EXPRESS** **PRE-BOOK PASSENGERS REPORT NO LATER THAN 1200(L)** **EARLY BIRD CHECK-IN IS ONLY AVAILABLE FOR PRE-BOOKED PASSENGERS @ 1800-2000L (Day prior to departure) BRING ALL CHECKED BAGGAGE (PET w/KENNEL)**', 77.099609375], ['TBD', 73.779296875]], [['', 87.939453125], ['JB ELMENDORF-RICHARDSON, ALASKA **ORGANIC FLIGHT** **ALL PASSENGERS ARE REQUIRED TO WEAR CLOSED TOED SHOES ON BOARD THIS AIRCRAFT**', 51.123046875], ['25F', 82.080078125]], [['1500L', 87.939453125], ['TRAVIS AIR FORCE BASE, CALIFORNIA **ORGANIC FLIGHT** **ALL PASSENGERS ARE REQUIRED TO WEAR CLOSED TOED SHOES ON BOARD THIS AIRCRAFT**', 58.837890625], ['', 82.080078125]]], 'table_number': 1}, {'title': 'DEPARTURES FROM: YOKOTAAB, JAPAN (OKO) SATURDAY, DEC 30TH', 'title_confidence': 78.125, 'footer': '', 'footer_confidence': 0.0, 'table_confidence': 99.853515625, 'page_number': 6, 'rows': [[['ROLL CALL', 89.35546875], ['DESTINATION', 94.7265625], ['SEATS', 88.232421875]], [['1305L', 63.330078125], ["SEATTLE-TACOMA INT'L, WASHINGTON **PATRIOT EXPRESS** **PRE-BOOK PASSENGERS REPORT NO LATER THAN 1430(L)** **EARLY BIRD CHECK-IN IS ONLY AVAILABLE FOR PRE-BOOKED PASSENGERS @ 1800-2000L (Day prior to departure) BRING ALL CHECKED BAGGAGE (PET w/KENNEL)**", 67.138671875], ['TBD', 62.5]]], 'table_number': 2}], 'pdf_hash': 'a28f3893b7b495286443710b0c8fc01f049123d72c038dc50c97da21a6453c01', 'job_id': '831d480ceeb1e69b1541896ef632ce117ab403758613b89e4c91c39af30e3c42'}"""
)
