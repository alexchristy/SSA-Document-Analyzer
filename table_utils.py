from table import Table
import logging
import re

# Modifying the function to set the table confidence
def convert_textract_response_to_tables(json_response):
    """Convert AWS Textract JSON response to a list of Table objects."""
    try:
        tables = []
        block_id_to_block = {block['Id']: block for block in json_response.get('Blocks', [])}

        # Helper function to collect text from child blocks
        def collect_text_from_children(block):
            text = block.get('Text', '')
            for relationship in block.get('Relationships', []):
                if relationship['Type'] == 'CHILD':
                    for child_id in relationship.get('Ids', []):
                        child_block = block_id_to_block.get(child_id, {})
                        text += ' ' + collect_text_from_children(child_block)
            return text.strip()

        current_table = None

        for block in json_response.get('Blocks', []):
            if block.get('BlockType') == 'TABLE':
                current_table = Table()
                current_table.table_number = len(tables) + 1
                current_table.table_confidence = block.get('Confidence', 0.0)  # Set the table confidence
                tables.append(current_table)

            elif block.get('BlockType') == 'CELL':
                cell_text = collect_text_from_children(block)
                cell_confidence = block.get('Confidence', 0.0)
                row_index = block.get('RowIndex', 0) - 1

                while len(current_table.rows) <= row_index:
                    current_table.add_row([])

                current_row = current_table.rows[row_index]
                current_row.append((cell_text, cell_confidence))

            elif block.get('BlockType') == 'TABLE_TITLE':
                title_text = collect_text_from_children(block)
                current_table.title = title_text
                current_table.title_confidence = block.get('Confidence', 0.0)

            elif block.get('BlockType') == 'TABLE_FOOTER':
                footer_text = collect_text_from_children(block)
                current_table.footer = footer_text
                current_table.footer_confidence = block.get('Confidence', 0.0)
                    
        return tables
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
        rollcall_found = destination_found = seats_found = 0

        for cell, _ in row:
            if any(regex.search(cell) for regex in rollcallRegexList):
                rollcall_found += 1
            if any(regex.search(cell) for regex in destinationRegexList):
                destination_found += 1
            if any(regex.search(cell) for regex in seatsRegexList):
                seats_found += 1

        if rollcall_found == 1 and destination_found == 1 and seats_found == 1:
            match_row_index = row_index
            break

    # If a match is found, rebuild the table
    if match_row_index is not None:
        new_rows = table.rows[match_row_index:]
        table.rows = new_rows
    
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
