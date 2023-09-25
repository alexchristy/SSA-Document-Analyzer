from table import Table
import logging
import re
from typing import List
from date_utils import check_date_string

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