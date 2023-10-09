import logging
import pickle

class Table:
    def __init__(self):
        self.title = ""
        self.title_confidence = 0.0
        self.footer = ""
        self.footer_confidence = 0.0
        self.table_confidence = 0.0
        self.page_number = 0  # New field for storing the page number
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
                md.append(f"## Table {self.table_number} (Page: {self.page_number}, Confidence: {self.table_confidence})\n") 
                md.append(f"{self.title} (Confidence: {self.title_confidence})\n")
            # Calculate maximum width for each column for alignment
            column_count = len(self.rows[0]) if self.rows else 0
            max_widths = [0] * column_count
            for row in self.rows:
                for i, cell in enumerate(row):
                    if i < column_count:
                        max_widths[i] = max(max_widths[i], len(f"{cell[0]} ({cell[1]})"))
            # Create header and rows
            if self.rows:
                header_row = [f"{cell[0]} ({cell[1]})".ljust(max_widths[i]) for i, cell in enumerate(self.rows[0])]
                md.append("| " + " | ".join(header_row) + " |")
                separator_row = ["-" * max_widths[i] for i in range(column_count)]
                md.append("| " + " | ".join(separator_row) + " |")
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

    def get_average_row_confidence(self, row_index, ignore_empty_cells=False):
        """
        Calculate and return the average confidence of a row specified by row_index.
        
        Parameters:
        row_index (int): The index of the row for which to calculate the average confidence.
        ignore_empty_cells (bool): Whether to ignore empty cells when calculating average confidence.
        
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

            # Filter out empty cells if ignore_empty_cells is True
            if ignore_empty_cells:
                row = [cell for cell in row if cell[0].strip() != ""]

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
        
    def save_state(self, filename="table_state.pkl"):
        try:
            with open(filename, "wb") as f:
                pickle.dump(self, f)
            return True
        except Exception as e:
            logging.error(f"An error occurred while saving the table state: {e}")
            return False
        
    def get_row(self,index: int):

        if index < 0 or index >= len(self.rows):
            logging.error(f"Row index {index} out of range. Valid range is 0 to {len(self.rows) - 1}.")
            return None

        return self.rows[index]

    def get_num_of_columns(self):

        if len(self.rows) == 0:
            return 0

        if len(self.rows[0]):
            return len(self.rows[0])
        else:
            return 0
      
    def get_cell_text(self, column_index: int, row_index: int) -> str:

        # Check if the row index is out of range
        if row_index < 0 or row_index >= len(self.rows):
            logging.error(f"Row index {row_index} out of range. Valid range is 0 to {len(self.rows) - 1}.")
            return None
        
        # Check if the column index is out of range
        if column_index < 0 or column_index >= len(self.rows[row_index]):
            logging.error(f"Column index {column_index} out of range. Valid range is 0 to {len(self.rows[row_index]) - 1}.")
            return None
        
        # Extract the cell text
        cell_text = self.rows[row_index][column_index][0]

        return cell_text

    def __eq__(self, other):
        if not isinstance(other, Table):
            logging.info("The other object is not an instance of Table.")
            return False

        try:
            # Get all attributes as dictionaries
            attrs1 = vars(self)
            attrs2 = vars(other)

            # Check if both objects have the same set of attributes
            if set(attrs1.keys()) != set(attrs2.keys()):
                logging.info(f"Different sets of attributes. Self: {set(attrs1.keys())}, Other: {set(attrs2.keys())}")
                return False

            # Initialize a flag to keep track of equality
            are_equal = True

            # Check if the values of all attributes are equal
            for attr, value1 in attrs1.items():
                value2 = attrs2.get(attr)
                if value1 != value2:
                    logging.info(f"Different values for attribute '{attr}'. Self: {value1}, Other: {value2}")
                    are_equal = False  # Set flag to false if any attribute is different

            return are_equal

        except Exception as e:
            logging.error(f"An error occurred while comparing tables: {e}")
            return False


    @classmethod
    def load_state(cls, filename="table_state.pkl") -> "Table":
        try:
            with open(filename, "rb") as f:
                loaded_table = pickle.load(f)
            return loaded_table
        except Exception as e:
            logging.error(f"An error occurred while loading the table state: {e}")
            return None

