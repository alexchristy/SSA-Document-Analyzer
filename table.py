# Importing the logging module for logging configuration and error handling
import logging

# Configuring the logging
logging.basicConfig(level=logging.INFO)

# Defining the Table class
class Table:
    def __init__(self):
        self.title = ""
        self.title_confidence = 0.0
        self.footer = ""
        self.footer_confidence = 0.0
        self.rows = []  # Each row is a list of tuples (cell_text, confidence)
        self.table_number = 0  # Table number, to be set externally

    def add_row(self, row):
        """Add a row to the table."""
        self.rows.append(row)

    def to_markdown(self):
        """Convert the table to a Markdown string."""
        try:
            md = []
            if self.title:
                md.append(f"## Table {self.table_number} - {self.title} (Confidence: {self.title_confidence})")
            # Calculate maximum width for each column for alignment
            column_count = len(self.rows[0]) if self.rows else 0
            max_widths = [0] * column_count
            for row in self.rows:
                for i, cell in enumerate(row):
                    if i < column_count:  # Check to prevent IndexError
                        max_widths[i] = max(max_widths[i], len(f"{cell[0]} ({cell[1]})"))
            # Create header row from the first row of the table
            if self.rows:
                header_row = [f"{cell[0]} ({cell[1]})".ljust(max_widths[i]) for i, cell in enumerate(self.rows[0])]
                md.append("| " + " | ".join(header_row) + " |")
                separator_row = ["-" * max_widths[i] for i in range(column_count)]
                md.append("| " + " | ".join(separator_row) + " |")
            # Create table rows
            for row in self.rows[1:]:
                formatted_row = [f"{cell[0]} ({cell[1]})".ljust(max_widths[i]) for i, cell in enumerate(row)]
                md.append("| " + " | ".join(formatted_row) + " |")
            # Add footer
            if self.footer:
                md.append(f"*{self.footer}* (Confidence: {self.footer_confidence})")
            return "\n".join(md)
        except Exception as e:
            logging.error(f"An error occurred while converting the table to Markdown: {e}")
            return None

# Defining the function to convert AWS Textract JSON to Table objects
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