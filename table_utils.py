from table import Table
import logging
import re

def convert_textract_response_to_tables(json_response):
    """Convert AWS Textract JSON response to a list of Table objects."""
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
                current_table.table_number = len(tables) + 1
                current_table.table_confidence = block.get('Confidence', 0.0)
                current_table.page_number = block.get('Page', 1)  # Setting the page number
                tables.append(current_table)
                
            elif block_type == 'CELL':
                if current_table is None:
                    logging.warning("Encountered a CELL block before a TABLE block.")
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
                    current_table.title = title_text
                    current_table.title_confidence = block.get('Confidence', 0.0)

            elif block_type == 'TABLE_FOOTER':
                if current_table:
                    footer_text = collect_text_from_children(block)
                    current_table.footer = footer_text
                    current_table.footer_confidence = block.get('Confidence', 0.0)
                    
        return tables if tables else None
    except Exception as e:
        logging.error(f"An error occurred while converting to table: {e}")
        return None

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