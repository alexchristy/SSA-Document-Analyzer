from table import Table
import logging
import re
from typing import List
from date_utils import check_date_string
from gpt3_turbo_analysis import GPT3TurboAnalysis
from cell_parsing_utils import parse_seat_data, has_multiple_rollcall_times, parse_destination

def convert_textract_response_to_tables(json_response):
    """
    Convert AWS Textract JSON response to a list of Table objects.
    """

    table_title_exclusion_list = ['updated', 'current']

    try:
        tables = []
        # Check if the input is paginated (list of blocks) or not (single page JSON response)
        if isinstance(json_response, list):
            blocks = json_response
        else:
            blocks = json_response.get('Blocks', [])

        # Create a dictionary mapping block IDs to blocks
        block_id_to_block = {block['Id']: block for block in blocks if 'Id' in block}

        # Helper function to collect text from child blocks
        def collect_text_from_children(block):
            text = block.get('Text', '')
            for relationship in block.get('Relationships', []):
                if relationship.get('Type') == 'CHILD':
                    for child_id in relationship.get('Ids', []):
                        child_block = block_id_to_block.get(child_id, {})
                        text += ' ' + collect_text_from_children(child_block)
            return text.strip()
        
        current_table = None
        for block in blocks:
            block_type = block.get('BlockType')
            
            if block_type == 'TABLE':
                current_table = Table()

                # Use find_table_title_with_date function if there are no TABLE_TITLE blocks
                found_title, found_title_confidence = find_table_title_with_date(blocks, block)
                if found_title:
                    current_table.title = found_title
                    current_table.title_confidence = found_title_confidence  # Set the confidence value
                else:
                    current_table.title = 'No title found'
                    current_table.title_confidence = 0.0  # Set confidence to 0 as no title was found

                current_table.table_number = len(tables) + 1
                current_table.table_confidence = block.get('Confidence', 0.0)
                current_table.page_number = block.get('Page', 1)  # Setting the page number
                tables.append(current_table)
            elif block_type == 'CELL':
                if current_table is None:
                    logging.warning('Encountered a CELL block before a TABLE block.')
                    continue
                cell_text = collect_text_from_children(block)
                cell_confidence = block.get('Confidence', 0.0)
                row_index = block.get('RowIndex', 0) - 1
                while len(current_table.rows) <= row_index:
                    current_table.add_row([])
                current_row = current_table.rows[row_index]
                current_row.append((cell_text, cell_confidence))
            elif block_type == 'TABLE_TITLE':
                if current_table:
                    title_text = collect_text_from_children(block)

                    # Check if title has the date in it and no words in the exclusion list
                    if check_date_string(title_text) and all(exclude not in title_text.lower() for exclude in table_title_exclusion_list):
                        current_table.title = title_text
                        current_table.title_confidence = block.get('Confidence', 0.0)
            elif block_type == 'TABLE_FOOTER':
                if current_table:
                    footer_text = collect_text_from_children(block)
                    current_table.footer = footer_text
                    current_table.footer_confidence = block.get('Confidence', 0.0)
        return tables if tables else None
    except Exception as e:
        logging.error(f'An error occurred while converting to table: {e}')
        return None
   
def find_table_title_with_date(blocks, table_block):
    """
    Returns the table title if found, otherwise returns None. Also returns the confidence level of the found title.
    """
    table_title_exclusion_list = ['updated', 'current']

    table_top = table_block['Geometry']['BoundingBox']['Top']
    table_page = table_block.get('Page', None)
    
    # Filter out blocks that are text lines, are located above the table, and are on the same page
    text_lines_above_table = [
        block for block in blocks
        if block['BlockType'] == 'LINE'
        and block['Geometry']['BoundingBox']['Top'] < table_top
        and block.get('Page', None) == table_page
    ]
    
    # Sort the lines by their vertical position, so that we can search from nearest to farthest from the table
    text_lines_above_table.sort(key=lambda x: table_top - x['Geometry']['BoundingBox']['Top'])
    
    # Search for a title containing a date
    for line_block in text_lines_above_table:
        line_text = line_block['Text']

        # Exclude any lines that contain the words in the exclusion list
        if check_date_string(line_text) and all(exclude not in line_text.lower() for exclude in table_title_exclusion_list):
            return line_text, line_block.get('Confidence', 0.0)  # Found a title with a date, and it should be the closest one based on sorting
    
    return None, 0.0  # No suitable title found

def remove_incorrect_column_header_rows(table):
    rollcallRegexList = [re.compile(r'rollcall', re.IGNORECASE), re.compile(r'roll call', re.IGNORECASE)]
    destinationRegexList = [re.compile(r'destination', re.IGNORECASE), re.compile(r'destinations', re.IGNORECASE)]
    seatsRegexList = [re.compile(r'seats', re.IGNORECASE)]

    match_row_index = None

    # Search for a row that meets the conditions
    for row_index, row in enumerate(table.rows):
        rollcall_found = any(regex.search(cell[0]) for regex in rollcallRegexList for cell in row)
        destination_found = any(regex.search(cell[0]) for regex in destinationRegexList for cell in row)
        seats_found = any(regex.search(cell[0]) for regex in seatsRegexList for cell in row)

        if rollcall_found and destination_found and seats_found:
            match_row_index = row_index
            break

    if match_row_index is not None:
        del table.rows[:match_row_index]
    else:
        logging.warning("No matching header row found. Table headers may be incorrect.")

    return table

def rearrange_columns(table):

    rollcallRegexList = [re.compile(r'rollcall', re.IGNORECASE), re.compile(r'roll call', re.IGNORECASE)]
    destinationRegexList = [re.compile(r'destination', re.IGNORECASE), re.compile(r'destinations', re.IGNORECASE)]
    seatsRegexList = [re.compile(r'seats', re.IGNORECASE)]

    if not table.rows:
        return table
    
    rollcall_index = destination_index = seats_index = -1

    for col_index, (cell, _) in enumerate(table.rows[0]):
        if any(regex.search(cell) for regex in rollcallRegexList):
            rollcall_index = col_index
        elif any(regex.search(cell) for regex in destinationRegexList):
            destination_index = col_index
        elif any(regex.search(cell) for regex in seatsRegexList):
            seats_index = col_index

    if rollcall_index == -1 or destination_index == -1 or seats_index == -1:
        return table

    new_rows = []
    for row in table.rows:
        new_row = [
            row[rollcall_index],
            row[destination_index],
            row[seats_index]
        ] + [cell for i, cell in enumerate(row) if i not in [rollcall_index, destination_index, seats_index]]
        
        new_rows.append(new_row)
        
    table.rows = new_rows
    return table

def gen_tables_from_textract_response(textract_response):
    tables = convert_textract_response_to_tables(textract_response)
    
    if not isinstance(tables, list):
        logging.error("Expected a list of tables, got something else.")
        return []
    
    processed_tables = []
    for table in tables:
        processed_table = remove_incorrect_column_header_rows(table)
        processed_table = rearrange_columns(processed_table)
        processed_tables.append(processed_table)
    
    return processed_tables

def get_destination_column_index(table: Table) -> int:
    """
    This function returns the index number of the column containing destinations.
    """
    
    logging.info(f"Retrieving destination column index.")

    if table is None:
        logging.error(f"Exiting function! Table is empty.")
        return None
    
    if table.rows is None:
        logging.error(f"Exiting function! There are no rows in the table.")
        return None
    
    if table.get_num_of_columns() == 0:
        logging.error(f"Exiting function! There are no columns in the table.")
        return None

    # Define regex pattern to match destination
    # column header
    patterns = [r'(?i)\bdestination(s)?\b']

    # Get column index
    for index, column_header in enumerate(table.rows[0]):
        for pattern in patterns:
            if re.search(pattern, column_header[0]):
                logging.info(f"Found destination column header: {column_header[0]}")
                return index
            
    return None

def get_seats_column_index(table: Table) -> int:
    """
    This function returns the index number of the column containing seat data.
    """
    
    logging.info(f"Retrieving seat data column index.")

    if table is None:
        logging.error(f"Exiting function! Table is empty.")
        return None
    
    if table.rows is None:
        logging.error(f"Exiting function! There are no rows in the table.")
        return None
    
    if table.get_num_of_columns() == 0:
        logging.error(f"Exiting function! There are no columns in the table.")
        return None

    # Define regex pattern to match seats
    # column header
    patterns = [r'(?i)\bseat(s)?\b', r'(?i)\bst\/r\b']
    
    # Get column index
    for index, column_header in enumerate(table.rows[0]):
        for pattern in patterns:
            if re.search(pattern, column_header[0]):
                logging.info(f"Found seat data column header: {column_header[0]}")
                return index
            
    return None

def convert_note_column_to_notes(table: Table, current_row: int, note_columns: List[int]) -> dict:
    """
    This function takes in a list of columns that contain information not related
    to roll call time, seats, or destinations and turns them into a json string.
    """

    # Check if table is empty
    if table is None:
        logging.error(f"Nothing to covert to notes. Table is empty.")
        return ''
    
    # Check if note columns is empty
    if note_columns is None:
        logging.error(f"Nothing to convert to notes. There are no note columns.")
        return ''
    
    # Create a dictionary to store notes
    notes = {}

    # Get notes from each cell
    for note_column in note_columns:
        note = table.get_cell_text(note_column, current_row)

        note_column_header_text = table.get_cell_text(note_column, 0)

        if note_column_header_text is None:
            logging.error(f"Failed to get note column header text from cell (0, {note_column}).")
            return ''
        
        if note is None:
            logging.error(f"Failed to get note from cell ({current_row}, {note_column}).")
            return ''
        
        notes[note_column_header_text] = note
    
    return notes

def get_roll_call_column_index(table: Table) -> int:
    """
    This function returns the index number of the column containing roll call times.
    """
    
    logging.info(f"Retrieving roll call column index.")

    if table is None:
        logging.error(f"Exiting function! Table is empty.")
        return None
    
    if table.rows is None:
        logging.error(f"Exiting function! There are no rows in the table.")
        return None
    
    if table.get_num_of_columns() == 0:
        logging.error(f"Exiting function! There are no columns in the table.")
        return None

    # Define regex pattern to match roll call time
    # column header
    patterns = [r'(?i)\broll\s*call\s*(time)?\b', r'(?i)\br\/c\b']

    # Get column index
    for index, column_header in enumerate(table.rows[0]):
        for pattern in patterns:
            if re.search(pattern, column_header[0]):
                logging.info(f"Found roll call time column header: {column_header[0]}")
                return index
    
    return None

def merge_table_rows(table: Table) -> Table:
    """
    Merges vertically adjacent rows in a given table that have cells with the same confidence value.
    The merged cell's confidence value becomes the average of the merged cells, rounded to 8 decimal places.

    Parameters:
        table (Table): The table object containing rows and columns to be merged.

    Returns:
        Table: A new table object with merged rows.
    """
    logging.info(f"Merging table rows...")

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
        merge_groups = populate_merged_row_seat_columns(table, merge_groups)

        # Merge rows in each group
        merged_table = _merge_grouped_rows(table, merge_groups)

        return merged_table

    except Exception as e:
        logging.error(f"An error occurred while merging table rows: {e}")
        return None

def _get_merge_row_groups(table: Table) -> List[List[tuple]]:
    """
    Finds groups of vertically adjacent rows in a given table that have cells with the same confidence value and 
    groups them together

    Parameters:
        table (Table): The table object containing rows and columns to be grouped.

    Returns:
        merge_groups (List[List]): Returns an array of rows grouped together that should be merged.
    """
    try:
        if not table.rows:
            logging.info("No rows in the table to merge.")
            return table

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
        logging.error(f"An error occurred while identifying merge groups: {e}")
        return None

    return merge_groups

def _merge_grouped_rows(table: Table, merge_groups: List[List[tuple]]) -> Table:
    """
    Takes in a table and a list of merge groups and merges the rows in the table based on the merge groups.

    Parameters:
        table (Table): The table object containing rows and columns to be merged.
        merge_groups (List[List]): A list of merge groups to be merged.

    Returns:
        Table: A new table object with merged rows.
    """
    
    try:
        if not table.rows:
            logging.info("No rows in the table to merge.")
            return table
        
        num_columns = len(table.rows[0])
        # Perform row merging for identified merge groups
        for group in merge_groups:
            merged_row = [('', 0)] * num_columns
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
            if has_multiple_rollcall_times(merged_row[roll_call_col_index][0]):
                logging.info(f'Skipping merging rows {first_row_index} to {group[-1][0]} because they have multiple roll call times.')
                continue

            # Do not merge rows that have different number of destinations and seats if there is more than one seat data
            # Number of seat data points can be 1 as that one point would be applicable to all destinations for the flight or
            # number of seat data points can be 0 which we will interpret as TDB (to be determined) when we store the data
            seat_col_index = get_seats_column_index(table)
            dest_col_index = get_destination_column_index(table)
            dests = parse_destination(merged_row[dest_col_index][0])
            seats = parse_seat_data(merged_row[seat_col_index][0])

            if not ((len(dests) == len(seats)) or len(seats) == 1 or len(seats) == 0) :
                logging.info(f'Skipping merging rows {first_row_index} to {group[-1][0]} because they have different number of destinations and seats.')
                continue

            # Insert the merged row back to its original position
            table.rows[first_row_index] = merged_row

            # Remove the merged rows from the original table, except for the first one which we've replaced
            for idx, _ in group[1:]:
                table.rows[idx] = None

        # Remove None rows (which are placeholders for the merged rows)
        table.rows = [row for row in table.rows if row is not None]

        return table

    except Exception as e:
        logging.error(f"An error occurred while merging table rows: {e}")
        return None
    
def populate_merged_row_seat_columns(table: Table, merge_groups: List[List[tuple]]) -> List[List[tuple]]:
    """
    This function takes in a list of merge groups and populates the seat data for rows that will be merged only
    if they have multiple seat data points. It is assumed that if a row has multiple seat data points, then the
    table is following the Kadena organization. See kadena_1_72hr tables 1 and 2 for examples.

    Parameters:
        table (Table): The table object containing rows and columns to be merged.
        merge_groups (List[List]): A list of merge groups to be merged.

    Returns:
        merge_groups (List[List]): Returns an array of rows grouped together that should be merged.
    """

    gpt_analyzer = GPT3TurboAnalysis()

    dest_column_index = get_destination_column_index(table)
    seat_column_index = get_seats_column_index(table)

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
            if dest_analyzed == 'None':
                continue

            seats = parse_seat_data(seat_text)

            # No seats found, update cell to 0T
            if not seats:
                logging.info(f'No seats found in row {row} in table using Kadena organizaton. Updating cell to 0T.')
                confidence_val = row[seat_column_index][1]
                new_seat_cell_tuple = ('0T', confidence_val)
                row[seat_column_index] = new_seat_cell_tuple

    return merge_groups