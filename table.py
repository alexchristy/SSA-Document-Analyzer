import logging

class Table:
    def __init__(self):
        self.title = ""
        self.title_confidence = 0.0
        self.footer = ""
        self.footer_confidence = 0.0
        self.table_confidence = 0.0  # New field for table confidence
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
                md.append(f"## Table {self.table_number} (Confidence: {self.table_confidence})\n") 
                md.append(f"{self.title} (Confidence: {self.title_confidence})\n")
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
    
    def average_row_confidence(self, row_index):
        """
        Calculate and return the average confidence of a row specified by row_index.
        
        Parameters:
        row_index (int): The index of the row for which to calculate the average confidence.
        
        Returns:
        float: The average confidence of the row, or None if an error occurs.
        """
        try:
            # Check if the row index is out of range
            if row_index < 0 or row_index >= len(self.rows):
                logging.error(f"Row index {row_index} out of range. Valid range is 0 to {len(self.rows) - 1}.")
                return None

            # Extract the row based on the index
            row = self.rows[row_index]

            # Calculate the total confidence for the row
            total_confidence = sum(cell[1] for cell in row)

            # Calculate the average confidence
            avg_confidence = total_confidence / len(row) if len(row) > 0 else 0.0

            return avg_confidence

        except IndexError:
            # Catch IndexError specifically
            logging.error(f"Row index {row_index} is out of bounds.")
            return None

        except Exception as e:
            # Log the exception
            logging.error(f"An error occurred while calculating the average confidence for row {row_index}: {e}")
            return None

