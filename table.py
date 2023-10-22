import logging
import pickle
from typing import List, Optional, Tuple, Type


class Table:
    """Represents a table extracted from a document."""

    def __init__(self: "Table") -> None:
        """Initialize a new Table object."""
        self.title = ""
        self.title_confidence = 0.0
        self.footer = ""
        self.footer_confidence = 0.0
        self.table_confidence = 0.0
        self.page_number = 0  # New field for storing the page number
        self.rows: List[
            List[Tuple[str, float]]
        ] = []  # Each row is a list of tuples (cell_text, confidence)
        self.table_number = 0  # Table number, to be set externally

    def add_row(self: "Table", row: List[Tuple[str, float]]) -> None:
        """Add a row to the table."""
        self.rows.append(row)

    def to_markdown(self: "Table") -> str:
        """Convert the table to a Markdown string."""
        try:
            md = []

            # Add title if available
            if self.title:
                md.append(
                    f"## Table {self.table_number} (Page: {self.page_number}, Confidence: {self.table_confidence})\n"
                )
                md.append(f"{self.title} (Confidence: {self.title_confidence})\n")

            # Check for an empty table
            if not self.rows or all(len(row) == 0 for row in self.rows):
                md.append("(empty table)\n")
            else:
                # Calculate maximum width for each column for alignment
                column_count = len(self.rows[0]) if self.rows else 0
                max_widths = [0] * column_count
                for row in self.rows:
                    for i, cell in enumerate(row):
                        if i < column_count:
                            max_widths[i] = max(
                                max_widths[i], len(f"{cell[0]} ({cell[1]})")
                            )

                # Create header and rows
                if self.rows:
                    header_row = [
                        f"{cell[0]} ({cell[1]})".ljust(max_widths[i])
                        for i, cell in enumerate(self.rows[0])
                    ]
                    md.append("| " + " | ".join(header_row) + " |")
                    separator_row = ["-" * max_widths[i] for i in range(column_count)]
                    md.append("| " + " | ".join(separator_row) + " |")

                for row in self.rows[1:]:
                    formatted_row = [
                        f"{cell[0]} ({cell[1]})".ljust(max_widths[i])
                        for i, cell in enumerate(row)
                    ]
                    md.append("| " + " | ".join(formatted_row) + " |")

            # Add footer if available
            if self.footer:
                md.append(f"*{self.footer}* (Confidence: {self.footer_confidence})")

            return "\n".join(md)

        except Exception as e:
            logging.error(
                "An error occurred while converting the table to Markdown: %s", e
            )
            return "Error converting table to Markdown."

    def get_average_row_confidence(
        self: "Table", row_index: int, ignore_empty_cells: bool = False
    ) -> float:
        """Calculate and return the average confidence of a row specified by row_index.

        Args:
        ----
        row_index (int): The index of the row for which to calculate the average confidence.
        ignore_empty_cells (bool): Whether to ignore empty cells when calculating average confidence.

        Returns:
        -------
        float: The average confidence of the row, or None if an error occurs.
        """
        try:
            # Check if the row index is out of range
            if row_index < 0 or row_index >= len(self.rows):
                logging.error(
                    "Row index %s out of range. Valid range is 0 to %s.",
                    row_index,
                    len(self.rows) - 1,
                )
                return -1.0

            # Extract the row based on the index
            row = self.rows[row_index]

            # Filter out empty cells if ignore_empty_cells is True
            if ignore_empty_cells:
                row = [cell for cell in row if cell[0].strip() != ""]

            # Calculate the average confidence of the row
            if len(row) == 0:
                return 0.0

            return sum(cell[1] for cell in row) / len(row)

        except Exception as e:
            logging.error(
                "An error occurred while calculating the average row confidence: %s", e
            )
            return -1.0

    def save_state(self: "Table", filename: str = "table_state.pkl") -> bool:
        """Save the state of the table to a file.

        Args:
        ----
        filename (str): The name of the file to save the state to.

        Returns:
        -------
        bool: True if the state was saved successfully, False otherwise.
        """
        try:
            with open(filename, "wb") as f:
                pickle.dump(self, f)
            return True
        except Exception as e:
            logging.error("An error occurred while saving the table state: %s", e)
            return False

    def get_row(self: "Table", index: int) -> List[Tuple[str, float]]:
        """Return the row at the specified index.

        Args:
        ----
        index (int): The index of the row to return.

        Returns:
        -------
        List[Tuple[str, float]]: The row at the specified index, or None if an error occurs.
        """
        if index < 0 or index >= len(self.rows):
            logging.error(
                "Row index %s out of range. Valid range is 0 to %s.",
                index,
                len(self.rows) - 1,
            )
            return []

        return self.rows[index]

    def get_num_of_columns(self: "Table") -> int:
        """Return the number of columns in the table.

        Returns
        -------
        int: The number of columns in the table.
        """
        if len(self.rows) == 0:
            return 0

        if len(self.rows[0]):
            return len(self.rows[0])

        return 0

    def get_cell_text(self: "Table", column_index: int, row_index: int) -> str:
        """Return the text of the cell at the specified column and row indices.

        Args:
        ----
            column_index (int): The index of the column containing the cell.
            row_index (int): The index of the row containing the cell.

        Returns:
        -------
        str: The text of the cell at the specified column and row indices, or None if an error occurs.
        """
        # Check if the row index is out of range
        if row_index < 0 or row_index >= len(self.rows):
            logging.error(
                "Row index %s out of range. Valid range is 0 to %s.",
                row_index,
                len(self.rows) - 1,
            )
            return ""

        # Check if the column index is out of range
        if column_index < 0 or column_index >= len(self.rows[row_index]):
            logging.error(
                "Column index %s out of range. Valid range is 0 to %s.",
                column_index,
                len(self.rows[row_index]) - 1,
            )
            return ""

        # Extract the cell text
        return self.rows[row_index][column_index][0]

    def __eq__(self: "Table", other: object) -> bool:
        """Return True if this table is equal to the other table, False otherwise.

        Args:
        ----
            other (Table): The other table to compare to.

        Returns:
        -------
        bool: True if this table is equal to the other table, False otherwise.
        """
        if not isinstance(other, Table):
            logging.info("The other object is not an instance of Table.")
            return False

        try:
            # Get all attributes as dictionaries
            attrs1 = vars(self)
            attrs2 = vars(other)

            # Check if both objects have the same set of attributes
            if set(attrs1.keys()) != set(attrs2.keys()):
                logging.info(
                    "Different sets of attributes. Self: %s, Other: %s",
                    set(attrs1.keys()),
                    set(attrs2.keys()),
                )
                return False

            # Initialize a flag to keep track of equality
            are_equal = True

            # Check if the values of all attributes are equal
            for attr, value1 in attrs1.items():
                value2 = attrs2.get(attr)
                if value1 != value2:
                    logging.info(
                        "Different values for attribute '%s'. Self: %s, Other: %s",
                        attr,
                        value1,
                        value2,
                    )
                    are_equal = False  # Set flag to false if any attribute is different

            return are_equal

        except Exception as e:
            logging.error("An error occurred while comparing tables: %s", e)
            return False

    @classmethod
    def load_state(
        cls: Type["Table"], filename: str = "table_state.pkl"
    ) -> Optional["Table"]:
        """Load the state of the table from a file.

        Args:
        ----
            filename (str): The name of the file to load the state from.

        Returns:
        -------
        Table: The table object with the loaded state, or None if an error occurs.
        """
        try:
            with open(filename, "rb") as file:
                return pickle.load(  # noqa: S301 (pickle.load is safe for this func as it's only used for testing)
                    file
                )
        except Exception as e:
            logging.error("An error occurred while loading the table state: %s", e)
            return None
