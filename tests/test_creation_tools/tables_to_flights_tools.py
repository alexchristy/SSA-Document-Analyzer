import json
import logging
import os
import pickle
import sys
from typing import List

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


event_tables_string = """{"tables": [{"title": "DEPARTURES FROM: RAMSTEIN AB, Germany (RMS) Wednesday, December 27th 2023", "title_confidence": 62.060546875, "footer": "Seats: T - Tentative, F - Firm, TBD - To Be Determined", "footer_confidence": 86.71875, "table_confidence": 99.853515625, "page_number": 3, "rows": [[["ROLL CALL", 94.189453125], ["DESTINATION", 96.484375], ["SEATS", 92.919921875]], [["0540", 91.259765625], ["Al Udeid AB, Qatar", 93.505859375], ["19F", 90.0390625]], [["0545", 95.1171875], ["Baltimore Washington INT'L, MD Early Check-in available starting 26 December 2023, @0930L for Pre-Booked passengers on mission 1LT2 destined Baltimore Washington International, MD", 97.412109375], ["178T", 93.84765625]], [["0815", 89.84375], ["Kuwait INT'L, Kuwait", 92.041015625], ["324T", 88.671875]], [["0815", 94.62890625], ["Al Udeid AB, Qatar", 96.923828125], ["324T 1 Stop", 93.408203125]], [["0935", 89.2578125], ["Sigonella, Italy", 91.40625], ["TBD", 88.0859375]], [["", 95.068359375], ["Early Check-in available December 27,2023 @1835L ; for Pre- Booked passengers on mission VLY6 destined Baltimore Washington International, MD", 97.36328125], ["", 93.798828125]], [["0935", 92.3828125], ["Signolla, Italy", 94.62890625], ["19F", 91.162109375]]], "table_number": 1}, {"title": "Thursday, December 28th 2023", "title_confidence": 99.94161987304688, "footer": "TBD - To Be Determined", "footer_confidence": 63.134765625, "table_confidence": 99.853515625, "page_number": 4, "rows": [[["ROLL CALL", 94.3359375], ["DESTINATION", 96.77734375], ["SEATS", 94.482421875]], [["0440", 92.28515625], ["Rota, Spain", 94.677734375], ["TBD", 92.48046875]], [["0510", 91.748046875], ["Muwaffaq Salti AB, Jordan", 94.091796875], ["TBD", 91.89453125]], [["0510", 94.189453125], ["Al Udeid AB, Qatar", 96.630859375], ["TBD 1 Stop", 94.384765625]], [["0650", 94.04296875], ["Joint Base McGuire-Dix-Lakehurst, NJ", 96.484375], ["TBD 1 Stop", 94.23828125]], [["0830", 88.8671875], ["Muwaffaq Salti AB, Qatar", 91.162109375], ["TBD", 89.0625]], [["0830", 94.53125], ["Rota, Spain", 96.97265625], ["TBD 1 Stop", 94.7265625]], [["0912", 88.623046875], ["Joint Base Andrews, MD", 90.91796875], ["TBD", 88.818359375]], [["1415", 95.166015625], ["Baltimore Washington INT'L, MD Early Check-in available December 27, 2023 @1835L for Pre- Booked passengers on mission VLY6 destined Baltimore Washington International, MD", 97.607421875], ["TBD", 95.361328125]]], "table_number": 2}, {"title": "Friday, December 29th 2023", "title_confidence": 73.095703125, "footer": "Seats : T - Tentative, F - - Firm, TBD - To Be Determined", "footer_confidence": 76.953125, "table_confidence": 99.853515625, "page_number": 5, "rows": [[["ROLL CALL", 94.580078125], ["DESTINATION", 97.265625], ["SEATS", 93.9453125]], [["0230", 90.673828125], ["Muwaffaq Salti AB, Jordan", 93.26171875], ["TBD", 90.0390625]], [["0326", 91.89453125], ["Al Udeid AB, Qatar", 94.482421875], ["TBD", 91.259765625]], [["0336", 92.138671875], ["Al Udeid AB, Qatar", 94.775390625], ["TBD", 91.552734375]], [["0340", 91.845703125], ["Powidz, Poland", 94.43359375], ["TBD", 91.2109375]], [["0346", 91.796875], ["Al Udeid AB, Qatar", 94.384765625], ["TBD", 91.162109375]], [["0356", 91.845703125], ["Al Udeid AB, Qatar", 94.43359375], ["TBD", 91.2109375]], [["0540", 91.50390625], ["Powidz, Poland", 94.091796875], ["TBD", 90.869140625]], [["0720", 91.6015625], ["Agades, Niger", 94.189453125], ["TBD", 90.966796875]], [["1320", 90.869140625], ["Djibouti, Djibouti", 93.505859375], ["TBD", 90.283203125]], [["1320", 94.482421875], ["Al Udeid AB, Qatar", 97.16796875], ["TBD 1 Stop", 93.84765625]]], "table_number": 3}, {"title": "ARRIVALS TO: RAMSTEIN AB, Germany (RMS) Wednesday, December 27th 2023", "title_confidence": 95.166015625, "footer": "", "footer_confidence": 0.0, "table_confidence": 99.8046875, "page_number": 6, "rows": [[["DESTINATION", 96.2890625], ["PICK UP TIME", 93.359375]], [["Adana AB, Turkey", 93.408203125], ["0720L", 90.576171875]], [["Baltimore Washington Int'l, MD", 96.142578125], ["0945L", 93.212890625]]], "table_number": 4}], "pdf_hash": "9e6ac4d8da9087c6175cbd71e5739c2262d7c1ed3335e60b8509f96d61640042", "job_id": "8e52772203e691a915b799720ba5af723efd62cf26b0ca8ebf15d8dd3bbfaffd"}"""
create_tables_from_lambda_event(event_tables_string, "./")
