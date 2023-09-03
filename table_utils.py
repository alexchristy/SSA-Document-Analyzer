from table import Table
import logging
import re

def convert_textract_response_to_tables(json_response):
    """
    Convert AWS Textract JSON response to a list of Table objects.
    """

    table_title_exclusion_list = ['updated']

    try:
        tables = []
        block_id_to_block = {block['Id']: block for block in json_response.get('Blocks', []) if 'Id' in block}
        
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
        for block in json_response.get('Blocks', []):
            block_type = block.get('BlockType')
            
            if block_type == 'TABLE':
                current_table = Table()

                # Use find_table_title_with_date function if there are no TABLE_TITLE blocks
                found_title, found_title_confidence = find_table_title_with_date(json_response.get('Blocks', []), block)
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

def check_date_string(input_string):
    date_patterns = [
        r"(?i)(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+\d{1,2}(?:th|st|nd|rd)?(?:,\s+\d{4})?",
        r"(?i)\d{1,2}\s+(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)(?:,\s+\d{4})?",
        r"(?i)\d{4}\s+(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+\d{1,2}(?:th|st|nd|rd)?",
        r"(?i)\d{1,2}(?:th|st|nd|rd)?\s+(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+\d{4}"
    ]

    for pattern in date_patterns:
        if re.search(pattern, input_string):
            return True
    return False
    
def find_table_title_with_date(blocks, table_block):
    """
    Returns the table title if found, otherwise returns None. Also returns the confidence level of the found title.
    """
    table_title_exclusion_list = ['updated']

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