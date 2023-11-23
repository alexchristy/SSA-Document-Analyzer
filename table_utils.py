import logging
import random
import re
from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple

from cell_parsing_utils import (
    has_multiple_rollcall_times,
    parse_destination,
    parse_rollcall_time,
    parse_seat_data,
)
from date_utils import check_date_string
from gpt3_turbo_analysis import GPT3TurboAnalysis
from table import Table

MIN_NUM_COLUMNS = 3


def is_valid_72hr_table(table: Table) -> bool:
    """Return whether or not the table is a valid 72-hour table.

    Args:
    ----
        table: The table to check.

    Returns:
    -------
        True if the table is a valid 72-hour table, False otherwise.
    """
    if table.title is None:
        logging.info(
            "Table title is empty. Not enough information to determine if table is a 72-hour table. Exiting..."
        )
        return False

    # Check there is a valid date in the table title
    match = check_date_string(table.title, return_match=True)

    if match is None:
        logging.info(
            "Failed to get date from table title. Not enough information to determine if table is a 72-hour table. Exiting..."
        )
        return False

    num_rows = len(table.rows)
    num_columns = table.get_num_of_columns()

    # If no rows, log it and return
    if num_rows == 0 or num_columns == 0:
        logging.info("The table is empty. Cannot determine if it is a 72-hour table.")
        return False

    # If there are less than 3 columns, log it and return
    if num_columns < MIN_NUM_COLUMNS:
        logging.info(
            "The table has less than 3 columns. Cannot determine if it is a 72-hour table."
        )
        return False

    return True


def convert_textract_response_to_tables(
    json_response: List[Dict[str, Any]]
) -> List[Table]:
    """Convert the Textract JSON response to a list of Table objects.

    Args:
    ----
        json_response: The Textract JSON response.

    Returns:
    -------
        A list of Table objects.

    Raises:
    ------
        None.
    """
    # Your code here
    table_title_exclusion_list = ["updated", "current"]

    try:
        tables: List[Table] = []
        # Check if the input is paginated (list of blocks) or not (single page JSON response)
        if isinstance(json_response, list):
            blocks = json_response
        else:
            blocks = json_response.get("Blocks", [])

        # Create a dictionary mapping block IDs to blocks
        block_id_to_block = {block["Id"]: block for block in blocks if "Id" in block}

        # Helper function to collect text from child blocks
        def collect_text_from_children(block: Dict[str, Any]) -> str:
            text = block.get("Text", "")
            for relationship in block.get("Relationships", []):
                if relationship.get("Type") == "CHILD":
                    for child_id in relationship.get("Ids", []):
                        child_block = block_id_to_block.get(child_id, {})
                        text += " " + collect_text_from_children(child_block)
            return text.strip()

        current_table = None
        for block in blocks:
            block_type = block.get("BlockType")

            if block_type == "TABLE":
                current_table = Table()

                # Use find_table_title_with_date function if there are no TABLE_TITLE blocks
                found_title, found_title_confidence = find_table_title_with_date(
                    blocks, block
                )
                if found_title:
                    current_table.title = found_title
                    current_table.title_confidence = (
                        found_title_confidence  # Set the confidence value
                    )
                else:
                    current_table.title = "No title found"
                    current_table.title_confidence = (
                        0.0  # Set confidence to 0 as no title was found
                    )

                current_table.table_number = len(tables) + 1
                current_table.table_confidence = block.get("Confidence", 0.0)
                current_table.page_number = block.get(
                    "Page", 1
                )  # Setting the page number
                tables.append(current_table)
            elif block_type == "CELL":
                if current_table is None:
                    logging.warning("Encountered a CELL block before a TABLE block.")
                    continue
                cell_text = collect_text_from_children(block)
                cell_confidence = block.get("Confidence", 0.0)
                row_index = block.get("RowIndex", 0) - 1
                while len(current_table.rows) <= row_index:
                    current_table.add_row([])
                current_row = current_table.rows[row_index]
                current_row.append((cell_text, cell_confidence))
            elif block_type == "TABLE_TITLE":
                if current_table:
                    title_text = collect_text_from_children(block)

                    # Check if title has the date in it and no words in the exclusion list
                    if check_date_string(title_text) and all(
                        exclude not in title_text.lower()
                        for exclude in table_title_exclusion_list
                    ):
                        current_table.title = title_text
                        current_table.title_confidence = block.get("Confidence", 0.0)
            elif block_type == "TABLE_FOOTER" and current_table:
                footer_text = collect_text_from_children(block)
                current_table.footer = footer_text
                current_table.footer_confidence = block.get("Confidence", 0.0)
        return tables if tables else []
    except Exception as e:
        msg = f"An error occurred while converting to table: {e}"
        raise RuntimeError(msg) from e


def find_table_title_with_date(
    blocks: List[dict], table_block: dict
) -> Tuple[Optional[str], float]:
    """Find the title of a table that contains a date.

    Args:
    ----
        blocks: A list of blocks returned by the Amazon Textract API.
        table_block: The block representing the table.

    Returns:
    -------
        A tuple containing the title of the table (if found) and the confidence score of the title.
    """
    table_title_exclusion_list = ["updated", "current"]

    table_top = table_block["Geometry"]["BoundingBox"]["Top"]
    table_page = table_block.get("Page", None)

    # Filter out blocks that are text lines, are located above the table, and are on the same page
    text_lines_above_table = [
        block
        for block in blocks
        if block["BlockType"] == "LINE"
        and block["Geometry"]["BoundingBox"]["Top"] < table_top
        and block.get("Page", None) == table_page
    ]

    # Sort the lines by their vertical position, so that we can search from nearest to farthest from the table
    text_lines_above_table.sort(
        key=lambda x: table_top - x["Geometry"]["BoundingBox"]["Top"]
    )

    # Search for a title containing a date
    for line_block in text_lines_above_table:
        line_text = line_block["Text"]

        # Exclude any lines that contain the words in the exclusion list
        if check_date_string(line_text) and all(
            exclude not in line_text.lower() for exclude in table_title_exclusion_list
        ):
            return line_text, line_block.get(
                "Confidence", 0.0
            )  # Found a title with a date, and it should be the closest one based on sorting

    return None, 0.0  # No suitable title found


def remove_incorrect_column_header_rows(table: Table) -> Table:
    """Remove rows from the top of the table that do not contain column headers.

    Args:
    ----
        table: The table to remove rows from.

    Returns:
    -------
        The modified table.
    """
    rollcall_regex_list = [
        re.compile(r"rollcall", re.IGNORECASE),
        re.compile(r"roll call", re.IGNORECASE),
    ]
    destination_regex_list = [
        re.compile(r"destination", re.IGNORECASE),
        re.compile(r"destinations", re.IGNORECASE),
    ]
    seats_regex_list = [re.compile(r"seats", re.IGNORECASE)]

    match_row_index = None

    # Search for a row that meets the conditions
    for row_index, row in enumerate(table.rows):
        rollcall_found = any(
            regex.search(cell[0]) for regex in rollcall_regex_list for cell in row
        )
        destination_found = any(
            regex.search(cell[0]) for regex in destination_regex_list for cell in row
        )
        seats_found = any(
            regex.search(cell[0]) for regex in seats_regex_list for cell in row
        )

        if rollcall_found and destination_found and seats_found:
            match_row_index = row_index
            break

    if match_row_index is not None:
        del table.rows[:match_row_index]
    else:
        logging.warning("No matching header row found. Table headers may be incorrect.")

    return table


def rearrange_columns(table: Table) -> Table:
    """Rearrange the columns of the table.

    This make it so that the first column is the Roll Call column,
    the second column is the Destination column, and the third column is the Seats column.

    Args:
    ----
        table: The table to rearrange.

    Returns:
    -------
        The modified table.
    """
    rollcall_regex_list = [
        re.compile(r"rollcall", re.IGNORECASE),
        re.compile(r"roll call", re.IGNORECASE),
    ]
    dest_regex_list = [
        re.compile(r"destination", re.IGNORECASE),
        re.compile(r"destinations", re.IGNORECASE),
    ]
    seats_regex_list = [re.compile(r"seats", re.IGNORECASE)]

    if not table.rows:
        return table

    rollcall_index = destination_index = seats_index = -1

    for col_index, (cell, _) in enumerate(table.rows[0]):
        if any(regex.search(cell) for regex in rollcall_regex_list):
            rollcall_index = col_index
        elif any(regex.search(cell) for regex in dest_regex_list):
            destination_index = col_index
        elif any(regex.search(cell) for regex in seats_regex_list):
            seats_index = col_index

    if rollcall_index == -1 or destination_index == -1 or seats_index == -1:
        return table

    new_rows = []
    for row in table.rows:
        new_row = [row[rollcall_index], row[destination_index], row[seats_index]] + [
            cell
            for i, cell in enumerate(row)
            if i not in [rollcall_index, destination_index, seats_index]
        ]

        new_rows.append(new_row)

    table.rows = new_rows
    return table


def gen_tables_from_textract_response(
    textract_response: List[Dict[str, Any]]
) -> List[Table]:
    """Generate tables from Textract response.

    Args:
    ----
        textract_response: The Textract response.

    Returns:
    -------
        A list of processed tables.
    """
    tables = convert_textract_response_to_tables(textract_response)

    if not tables:
        logging.info("No tables returned from convert_textract_response_to_tables.")

    if not isinstance(tables, list):
        logging.info("Expected a list of tables, got something else.")

    processed_tables = []
    for table in tables:
        processed_table = remove_incorrect_column_header_rows(table)
        processed_table = rearrange_columns(processed_table)
        processed_tables.append(processed_table)

    return processed_tables


def get_destination_column_index(table: Table) -> int:
    """Return the index number of the column containing destinations.

    Args:
    ----
        table: Table object.

    Returns:
    -------
        The index number of the column with header that matches the destination regex.

    """
    logging.info("Retrieving destination column index.")

    if table is None:
        logging.info("Exiting function! Table is empty.")
        return -1

    if table.rows is None:
        logging.info("Exiting function! There are no rows in the table.")
        return -1

    if table.get_num_of_columns() == 0:
        logging.info("Exiting function! There are no columns in the table.")
        return -1

    # Define regex pattern to match destination
    # column header
    patterns = [r"(?i)\bdestination(s)?\b"]

    # Get column index
    for index, column_header in enumerate(table.rows[0]):
        for pattern in patterns:
            if re.search(pattern, column_header[0]):
                logging.info("Found destination column header: %s", column_header[0])
                return index

    return -1


def get_seats_column_index(table: Table) -> int:
    """Return the index number of the column containing seat data.

    Args:
    ----
        table: Table object.

    Returns:
    -------
        The index number of the column with header that matches the seat regex.
    """
    logging.info("Retrieving seat data column index.")

    if table is None:
        logging.info("Exiting function! Table is empty.")
        return -1

    if table.rows is None:
        logging.info("Exiting function! There are no rows in the table.")
        return -1

    if table.get_num_of_columns() == 0:
        logging.info("Exiting function! There are no columns in the table.")
        return -1

    # Define regex pattern to match seats
    # column header
    patterns = [r"(?i)\bseat(s)?\b", r"(?i)\bst\/r\b"]

    # Get column index
    for index, column_header in enumerate(table.rows[0]):
        for pattern in patterns:
            if re.search(pattern, column_header[0]):
                logging.info("Found seat data column header: %s", column_header[0])
                return index

    return -1


def convert_note_column_to_notes(
    table: Table, current_row: int, note_columns: List[int]
) -> dict:
    """Convert note columns in a table to a dictionary of notes.

    Args:
    ----
        table (Table): The table to convert.
        current_row (int): The current row to convert.
        note_columns (List[int]): A list of note columns to convert.

    Returns:
    -------
        dict: A dictionary of notes.
    """
    # Check if table is empty
    if table is None:
        logging.error("Nothing to covert to notes. Table is empty.")
        return ""

    # Check if note columns is empty
    if note_columns is None:
        logging.error("Nothing to convert to notes. There are no note columns.")
        return ""

    # Create a dictionary to store notes
    notes = {}

    # Get notes from each cell
    for note_column in note_columns:
        note = table.get_cell_text(note_column, current_row)

        note_column_header_text = table.get_cell_text(note_column, 0)

        if note_column_header_text is None:
            logging.error(
                "Failed to get note column header text from cell (0, %s).", note_column
            )
            return ""

        if note is None:
            logging.error(
                "Failed to get note from cell (%s, %s).", current_row, note_column
            )
            return ""

        notes[note_column_header_text] = note

    return notes


def get_roll_call_column_index(table: Table) -> int:
    """Get the index of the roll call time column in a table.

    Args:
    ----
        table (Table): The table to search.

    Returns:
    -------
        int: The index of the roll call time column, or None if not found.
    """
    logging.info("Retrieving roll call column index.")

    if table is None:
        logging.info("Exiting function! Table is empty.")
        return -1

    if table.rows is None:
        logging.info("Exiting function! There are no rows in the table.")
        return -1

    if table.get_num_of_columns() == 0:
        logging.info("Exiting function! There are no columns in the table.")
        return -1

    # Define regex pattern to match roll call time
    # column header
    patterns = [r"(?i)\broll\s*call\s*(time)?\b", r"(?i)\br\/c\b"]

    # Get column index
    for index, column_header in enumerate(table.rows[0]):
        for pattern in patterns:
            if re.search(pattern, column_header[0]):
                logging.info("Found roll call time column header: %s", column_header[0])
                return index

    return -1


def merge_table_rows(table: Table) -> Optional[Table]:
    """Merge vertically adjacent rows in a given table that have cells with the same confidence value.

    The merged cell's confidence value becomes the average of the merged cells, rounded to 8 decimal places.

    Args:
    ----
        table (Table): The table object containing rows and columns to be merged.

    Returns:
    -------
        Table: A new table object with merged rows.
    """
    logging.info("Merging table rows...")

    try:
        if not table.rows:
            logging.info("No rows in the table to merge.")
            return table

        # Get merge groups
        merge_groups = _get_merge_row_groups(table)

        # If there are no merge groups, return the original table
        if not merge_groups:
            logging.info("No merge groups found.")
            return table

        # Populate merged row seat columns
        groups_merged = populate_merged_row_seat_columns(table, merge_groups)

        # Merge rows in each group
        return _merge_grouped_rows(table, groups_merged)

    except Exception as e:
        logging.error("An error occurred while merging table rows: %s", e)
        return None


def _get_merge_row_groups(
    table: Table,
) -> List[List[Tuple[int, List[Tuple[str, float]]]]]:
    """Group rows together that should be merged.

    Find groups of vertically adjacent rows in a given table that have cells with the same confidence value and
    group them together.

    Args:
    ----
        table (Table): The table object containing rows and columns to be grouped.

    Returns:
    -------
        merge_groups (List[List[Tuple]]): Returns an array of rows grouped together that should be merged.
    """
    try:
        if not table.rows:
            logging.info("No rows in the table to merge.")
            return []

        # Initialize a list to keep track of row groups to merge
        merge_groups = []

        # Iterate through each column to identify merge groups
        num_columns = len(table.rows[0])
        for col in range(num_columns):
            merge_group = []
            prev_conf = None
            for idx, row in enumerate(table.rows):
                cur_conf = row[col][1]
                if cur_conf == prev_conf:
                    merge_group.append((idx, row))
                else:
                    if len(merge_group) > 1:
                        merge_groups.append(merge_group)
                    merge_group = [(idx, row)]
                prev_conf = cur_conf
            if len(merge_group) > 1:
                merge_groups.append(merge_group)

    except Exception as e:
        logging.error("An error occurred while identifying merge groups: %s", e)
        return []

    return merge_groups


def _merge_grouped_rows(
    table: Table, merge_groups: List[List[Tuple[int, List[Tuple[str, float]]]]]
) -> Optional[Table]:
    """Take in a table and a list of merge groups and merges the rows in the table based on the merge groups.

    Args:
    ----
        table (Table): The table object containing rows and columns to be merged.
        merge_groups (List[List]): A list of merge groups to be merged.

    Returns:
    -------
        Table: A new table object with merged rows.
    """
    try:
        if not table.rows:
            logging.info("No rows in the table to merge.")
            return table

        num_columns = len(table.rows[0])
        # Perform row merging for identified merge groups
        for group in merge_groups:
            merged_row = [("", 0.0)] * num_columns
            first_row_index = group[0][0]

            for _, row in group:
                for col in range(num_columns):
                    merged_text = f"{merged_row[col][0]} {row[col][0]}".strip()
                    merged_conf = (merged_row[col][1] + row[col][1]) / 2
                    # Round to 8 decimal places
                    merged_conf = round(merged_conf, 8)
                    merged_row[col] = (merged_text, merged_conf)

            # Do not merge rows that have multiple roll call times
            roll_call_col_index = get_roll_call_column_index(table)

            if roll_call_col_index == -1:
                logging.info(
                    "Skipping merging rows %s to %s because there is no roll call column.",
                    first_row_index,
                    group[-1][0],
                )
                continue

            if has_multiple_rollcall_times(merged_row[roll_call_col_index][0]):
                logging.info(
                    "Skipping merging rows %s to %s because they have multiple roll call times.",
                    first_row_index,
                    group[-1][0],
                )
                continue

            # Do not merge rows that have different number of destinations and seats if there is more than one seat data
            # Number of seat data points can be 1 as that one point would be applicable to all destinations for the flight or
            # number of seat data points can be 0 which we will interpret as TDB (to be determined) when we store the data
            seat_col_index = get_seats_column_index(table)
            dest_col_index = get_destination_column_index(table)
            dests = parse_destination(merged_row[dest_col_index][0])
            seats = parse_seat_data(merged_row[seat_col_index][0])

            if dest_col_index == -1:
                logging.info(
                    "Skipping merging rows %s to %s because there is no destination column.",
                    first_row_index,
                    group[-1][0],
                )
                continue

            if seat_col_index == -1:
                logging.info(
                    "Skipping merging rows %s to %s because there is no seat data column.",
                    first_row_index,
                    group[-1][0],
                )
                continue

            # Do not merge row with no destinations
            if dests is None:
                continue

            if not ((len(dests) == len(seats)) or len(seats) == 1 or len(seats) == 0):
                logging.info(
                    "Skipping merging rows %s to %s because they have different number of destinations and seats.",
                    first_row_index,
                    group[-1][0],
                )
                continue

            # Insert the merged row back to its original position
            table.rows[first_row_index] = merged_row

            # Remove the merged rows from the original table, except for the first one which we've replaced
            indices_to_remove = sorted(
                [idx for idx, _ in group[1:]]
            )  # Sort to delete from end
            for idx in reversed(indices_to_remove):  # Delete from end
                del table.rows[idx]

        # Remove None rows (which are placeholders for the merged rows)
        table.rows = [row for row in table.rows if row is not None]

        return table

    except Exception as e:
        logging.error("An error occurred while merging table rows: %s", e)
        return None


def populate_merged_row_seat_columns(
    table: Table, merge_groups: List[List[Tuple[int, List[Tuple[str, float]]]]]
) -> List[List[Tuple[int, List[Tuple[str, float]]]]]:
    """Fill empty seat data cells in rows to be merged with 0T.

    This function takes in a list of merge groups and populates the seat data for rows that will be merged only
    if they have multiple seat data points. It is assumed that if a row has multiple seat data points, then the
    table is following the Kadena organization. See kadena_1_72hr tables 1 and 2 for examples.

    Args:
    ----
        table (Table): The table object containing rows and columns to be merged.
        merge_groups (List[List]): A list of merge groups to be merged.

    Returns:
    -------
        merge_groups (List[List]): Returns an array of rows grouped together that should be merged.
    """
    gpt_analyzer = GPT3TurboAnalysis()

    dest_column_index = get_destination_column_index(table)
    seat_column_index = get_seats_column_index(table)

    if dest_column_index == -1:
        logging.error("No destination column found. Exiting function!")
        return merge_groups

    if seat_column_index == -1:
        logging.error("No seat data column found. Exiting function!")
        return merge_groups

    for group in merge_groups:
        # Search to see if there are two different
        # seat data points in the grouped rows
        seat_cell_text_data = set()
        for _, row in group:
            seat_text = row[seat_column_index][0]
            # If row has seat data, add it to the list
            if seat_text.strip():
                seat_cell_text_data.add(seat_text)

        # If there is more than one seat data, using Kadena organization
        if len(seat_cell_text_data) <= 1:
            continue

        # If there is more than one seat data, using Kadena organization
        for _, row in group:
            seat_text = row[seat_column_index][0]
            dest_text = row[dest_column_index][0]
            dest_analyzed = gpt_analyzer.get_destination_analysis(dest_text)

            # Not a destination row skip
            if dest_analyzed == "None" or dest_analyzed is None:
                continue

            seats = parse_seat_data(seat_text)

            # No seats found, update cell to 0T
            if not seats:
                logging.info(
                    "No seats found in row %s in table using Kadena organizaton. Updating cell to 0T.",
                    row,
                )
                confidence_val = row[seat_column_index][1]
                new_seat_cell_tuple = ("0T", confidence_val)
                row[seat_column_index] = new_seat_cell_tuple

    return merge_groups


def scramble_columns(input_table: Table) -> Optional[Table]:
    """Scrambles the columns of a table in a random order.

    Args:
    ----
        input_table (Table): The table to scramble.

    Returns:
    -------
        Table: A new table object with the columns scrambled.
    """
    try:
        # Deep copy the original table to create a new table object
        new_table = deepcopy(input_table)

        # Get the number of rows and columns in the table
        num_rows = len(new_table.rows)
        num_columns = new_table.get_num_of_columns()

        # If no rows, log it and return the original table
        if num_rows == 0 or num_columns == 0:
            logging.info("The table is empty. Returning the original table.")
            return new_table

        # Generate a list of column indices and shuffle it
        column_indices = list(range(num_columns))
        random.shuffle(column_indices)

        # Create a new list to hold rows with scrambled columns
        new_rows = []

        # Loop through each row
        for row in new_table.rows:
            new_row = []
            # Use the shuffled column indices to rearrange the columns
            for idx in column_indices:
                new_row.append(row[idx])
            new_rows.append(new_row)

        # Update the rows in the new table
        new_table.rows = new_rows
        logging.info(
            "Columns have been scrambled. New column order: %s", column_indices
        )

        return new_table

    except Exception as e:
        logging.error("An error occurred while scrambling the columns: %s", e)
        return None


def infer_roll_call_column_index(table: Table) -> int:
    """Get index of the roll call column in a given table.

    Infers the index of the roll call column in a given table. The function initially
    looks for a roll call column by checking if there are headers that match predefined
    roll call headers. If it doesn't find one, it uses the table's title and cell contents
    to make an educated guess on which column contains roll call times.

    Args:
    ----
        table (Table): A Table object that holds the table data including rows, title, and possibly footer.

    Returns:
    -------
        int: Returns the index of the inferred roll call column if found.
             Returns -1 if inference was unsuccessful or an error occurred.

    Logging:
        Logs informative messages and errors using Python's built-in logging framework.

    Exceptions:
        Logs any exceptions that occur during the execution and returns -1.
    """
    # First check if we can find roll call column by searching for a column header
    roll_call_col_index = get_roll_call_column_index(table)
    if roll_call_col_index != -1:
        logging.info(
            "No inference. Found roll call column by searching for column header."
        )
        return roll_call_col_index

    logging.info("Attempting to infer roll call column index...")

    if not is_valid_72hr_table(table):
        logging.info("Table is not a 72-hour table. Skipping inference...")
        return -1

    # Search cells for a colukmn of roll call times
    # that can be parsed as roll call times
    try:
        num_rows = len(table.rows)
        num_columns = table.get_num_of_columns()

        for col in range(num_columns):
            logging.info("Traversing column %d...", col)

            invalid_cell_count = 0
            valid_cell_count = 0
            empty_cell_count = 0
            for row in range(num_rows):
                cell_text = table.get_cell_text(col, row)

                if row == 0:
                    # If the header cell for a column is not empty or None, then it is not a roll call column
                    # as we searched for a roll call column header and didn't find one
                    if not (cell_text == "" or cell_text is None):
                        logging.info(
                            "Header Cell (%d, %d) is not empty. Is not a roll call column. Skipping column...",
                            col,
                            row,
                        )
                        return -1

                    continue

                if cell_text is None or cell_text == "":
                    logging.debug("Cell (%d, %d) is empty. Skipping...", col, row)
                    empty_cell_count += 1
                    continue

                # Attempt to parse cell text as roll call time
                roll_call_time = parse_rollcall_time(cell_text)

                if roll_call_time is None:
                    logging.debug(
                        "Cell (%d, %d) is not a valid roll call time. Skipping column...",
                        col,
                        row,
                    )
                    invalid_cell_count += 1
                    break

                logging.debug(
                    "Cell (%d, %d) is a valid roll call time.",
                    col,
                    row,
                )
                valid_cell_count += 1

            if invalid_cell_count > 0:
                logging.info(
                    "Column %d is not a valid roll call time column. Checking next column...",
                    col,
                )
                continue

            if (valid_cell_count + empty_cell_count) == (num_rows - 1):
                logging.info("Column %d is a valid roll call time column.", col)
                return col

    except Exception as e:
        logging.error("An error occurred while inferring roll call column index: %s", e)
        return -1

    return -1


def infer_seats_column_index(table: Table) -> int:
    """Get index of the seats column in a given table.

    Infers the index of the seat column in a given table. The function initially
    looks for a seat column by checking if there are headers that match predefined
    seat headers. If it doesn't find one, it uses the table's title and cell contents
    to make an educated guess on which column contains seat data.

    Args:
    ----
        table (Table): A Table object that holds the table data including rows, title, and possibly footer.

    Returns:
    -------
        int: Returns the index of the inferred seat column if found.
             Returns -1 if inference was unsuccessful or an error occurred.

    Logging:
        Logs informative messages and errors using Python's built-in logging framework.

    Exceptions:
        Logs any exceptions that occur during the execution and returns -1.
    """
    # First check if we can find seat column by searching for a column header
    seat_col_index = get_seats_column_index(table)
    if seat_col_index != -1:
        logging.info("No inference. Found seat column by searching for column header.")
        return seat_col_index

    logging.info("Attempting to infer seat column index...")

    if not is_valid_72hr_table(table):
        logging.info("Table is not a 72-hour table. Skipping inference...")
        return -1

    # Search cells for a colukmn of seats
    # that can be parsed as seats
    try:
        num_rows = len(table.rows)
        num_columns = table.get_num_of_columns()

        for col in range(num_columns):
            logging.info("Traversing column %d...", col)

            invalid_cell_count = 0
            valid_cell_count = 0
            empty_cell_count = 0
            for row in range(num_rows):
                cell_text = table.get_cell_text(col, row)

                if row == 0:
                    # If the header cell for a column is not empty or None, then it is not a roll call column
                    # as we searched for a roll call column header and didn't find one
                    if not (cell_text == "" or cell_text is None):
                        logging.info(
                            "Header Cell (%d, %d) is not empty. Is not a seats column. Skipping column...",
                            col,
                            row,
                        )
                        return -1

                    continue

                if cell_text is None or cell_text == "":
                    logging.debug(
                        "Cell (%d, %d) is empty. Skipping...",
                        col,
                        row,
                    )
                    empty_cell_count += 1
                    continue

                # Attempt to parse cell text as seat data
                seat_data = parse_seat_data(cell_text)

                if not seat_data:
                    logging.debug(
                        "Cell (%d, %d) is not a valid seat data point. Skipping column...",
                        col,
                        row,
                    )
                    invalid_cell_count += 1
                    break

                logging.debug("Cell (%d, %d) is a valid seat data point.", col, row)
                valid_cell_count += 1

            if invalid_cell_count > 0:
                logging.info(
                    "Column %d is not a valid seat column. Checking next column...",
                    col,
                )
                continue

            if (valid_cell_count + empty_cell_count) == (num_rows - 1):
                logging.info("Column %d is a valid seat column.", col)
                return col

    except Exception as e:
        logging.error("An error occurred while inferring seat column index: %s", e)
        return -1

    return -1


def infer_destinations_column_index(table: Table) -> int:
    """Get index of the destinations column in a given table.

    Infers the index of the destination column in a given table. The function initially
    looks for a destination column by checking if there are headers that match predefined
    destination headers. If it doesn't find one, it uses the table's title and cell contents
    to make an educated guess on which column contains destination data.

    Args:
    ----
        table (Table): A Table object that holds the table data including rows, title, and possibly footer.

    Returns:
    -------
        int: Returns the index of the inferred destination column if found.
             Returns -1 if inference was unsuccessful or an error occurred.

    Logging:
        Logs informative messages and errors using Python's built-in logging framework.

    Exceptions:
        Logs any exceptions that occur during the execution and returns -1.
    """
    # First check if we can find seat column by searching for a column header
    dest_col_index = get_destination_column_index(table)
    if dest_col_index != -1:
        logging.info(
            "No inference. Found destination column by searching for column header."
        )
        return dest_col_index

    logging.info("Attempting to infer destination column index...")

    if not is_valid_72hr_table(table):
        logging.info("Table is not a 72-hour table. Skipping inference...")
        return -1

    # Search cells for a column of destinations
    # that can be parsed as destinations
    try:
        num_rows = len(table.rows)
        num_columns = table.get_num_of_columns()

        for col in range(num_columns):
            logging.info("Traversing column %d...", col)

            invalid_cell_count = 0
            valid_cell_count = 0
            empty_cell_count = 0
            for row in range(num_rows):
                cell_text = table.get_cell_text(col, row)

                if row == 0:
                    # If the header cell for a column is not empty or None, then it is not a roll call column
                    # as we searched for a roll call column header and didn't find one
                    if not (cell_text == "" or cell_text is None):
                        logging.info(
                            "Header Cell (%d, %d) is not empty. Is not a roll call column. Skipping column...",
                            col,
                            row,
                        )
                        return -1

                    continue

                if cell_text is None or cell_text == "":
                    logging.debug("Cell (%d, %d) is empty. Skipping...", col, row)
                    empty_cell_count += 1
                    continue

                # Attempt to parse cell text as seat data
                dest_data = parse_destination(cell_text)

                if dest_data is None:
                    logging.debug(
                        "Cell (%d, %d) is not a valid destination cell. Skipping column...",
                        col,
                        row,
                    )
                    invalid_cell_count += 1
                    break

                logging.debug("Cell (%d, %d) is a valid destination cell.", col, row)
                valid_cell_count += 1

            if invalid_cell_count > 0:
                logging.info(
                    "Column %d is not a valid destination column. Checking next column...",
                    col,
                )
                continue

            if (valid_cell_count + empty_cell_count) == (num_rows - 1):
                logging.info("Column %d is a valid destination column.", col)
                return col

    except Exception as e:
        logging.error(
            "An error occurred while inferring destination column index: %s", e
        )
        return -1

    return -1


def delete_column(input_table: Table, col_index: int) -> Optional[Table]:
    """Delete a column from a table.

    Args:
    ----
        input_table (Table): The table to delete the column from.
        col_index (int): The index of the column to delete.

    Returns:
    -------
        Table: A new table with the specified column removed.
    """
    try:
        # Validate the table and column index
        if not isinstance(input_table, Table):
            logging.error("Input is not an instance of the Table class.")
            return None

        num_columns = input_table.get_num_of_columns()
        if col_index < 0 or (col_index >= num_columns and num_columns > 0):
            logging.error(
                "Invalid column index %d. Valid range is 0 to %d.",
                col_index,
                num_columns - 1,
            )
            return None

        # Deep copy the original table to create a new table object
        new_table = deepcopy(input_table)

        # Create a new list to hold rows with the column removed
        new_rows = []

        # Loop through each row
        for row in new_table.rows:
            new_row = [cell for i, cell in enumerate(row) if i != col_index]
            new_rows.append(new_row)

        # Update the rows in the new table
        new_table.rows = new_rows
        logging.info("Column %d has been deleted.", col_index)

        return new_table

    except Exception as e:
        logging.error("An error occurred while deleting the column: %s", e)
        return None
