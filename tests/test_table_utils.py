import unittest
from table import Table
from table_utils import populate_merged_row_seat_columns, _get_merge_row_groups, _merge_grouped_rows, merge_table_rows, infer_roll_call_column_index, delete_column, infer_seats_column_index, infer_destinations_column_index

class TestPopulateMergedRowSeatColumns(unittest.TestCase):
    
    def test_kadena_1_72hr_table_1(self):

        # Load merged table
        kadena_1_72hr_table_1_merged = Table.load_state('tests/unit-test-assets/populate_merged_row_seat_columns/kadena_1_72hr_table-1-merged.pkl')

        # Load unmerged table
        kadena_1_72hr_table_1 = Table.load_state('tests/unit-test-assets/populate_merged_row_seat_columns/kadena_1_72hr_table-1.pkl')

        # Get merge groups
        table_1_merge_groups = _get_merge_row_groups(kadena_1_72hr_table_1)

        # Populate merged row seat columns
        table_1_merge_groups = populate_merged_row_seat_columns(kadena_1_72hr_table_1, table_1_merge_groups)

        # Generate merged tables
        table_1_merged = _merge_grouped_rows(kadena_1_72hr_table_1, table_1_merge_groups)

        self.assertEqual(kadena_1_72hr_table_1_merged, table_1_merged)

    def test_kadena_1_72hr_table_2(self):

        # Load merged table
        kadena_1_72hr_table_2_merged = Table.load_state('tests/unit-test-assets/populate_merged_row_seat_columns/kadena_1_72hr_table-2-merged.pkl')

        # Load unmerged table
        kadena_1_72hr_table_2 = Table.load_state('tests/unit-test-assets/populate_merged_row_seat_columns/kadena_1_72hr_table-2.pkl')

        # Get merge groups
        table_2_merge_groups = _get_merge_row_groups(kadena_1_72hr_table_2)

        # Populate merged row seat columns
        table_2_merge_groups = populate_merged_row_seat_columns(kadena_1_72hr_table_2, table_2_merge_groups)

        # Generate merged tables
        table_2_merged = _merge_grouped_rows(kadena_1_72hr_table_2, table_2_merge_groups)

        self.assertEqual(kadena_1_72hr_table_2_merged, table_2_merged)

    def test_kadena_1_72hr_table_3(self):

        # Load merged table
        kadena_1_72hr_table_3_merged = Table.load_state('tests/unit-test-assets/populate_merged_row_seat_columns/kadena_1_72hr_table-3-merged.pkl')

        # Load unmerged table
        kadena_1_72hr_table_3 = Table.load_state('tests/unit-test-assets/populate_merged_row_seat_columns/kadena_1_72hr_table-3.pkl')

        # Get merge groups
        table_3_merge_groups = _get_merge_row_groups(kadena_1_72hr_table_3)

        # Populate merged row seat columns
        table_3_merge_groups = populate_merged_row_seat_columns(kadena_1_72hr_table_3, table_3_merge_groups)

        # Generate merged tables
        table_3_merged = _merge_grouped_rows(kadena_1_72hr_table_3, table_3_merge_groups)

        self.assertEqual(kadena_1_72hr_table_3_merged, table_3_merged)

class TestMergeTableRows(unittest.TestCase):

    def test_norfolk_1_72hr_table_1(self):

        # Load merged table
        merged_table = Table.load_state('tests/unit-test-assets/merge_table_rows/norfolk_1_72hr_table-1-merged.pkl')

        # Load unmerged table
        unmerged_table = Table.load_state('tests/unit-test-assets/merge_table_rows/norfolk_1_72hr_table-1.pkl')

        # Merge table rows
        calculated_merged_table = merge_table_rows(unmerged_table)

        self.assertEqual(merged_table, calculated_merged_table)

    def test_kadena_2_72hr_table_2(self):

        # Load merged table
        merged_table = Table.load_state('tests/unit-test-assets/merge_table_rows/kadena_2_72hr_table-2-merged.pkl')

        # Load unmerged table
        unmerged_table = Table.load_state('tests/unit-test-assets/merge_table_rows/kadena_2_72hr_table-2.pkl')

        # Merge table rows
        calculated_merged_table = merge_table_rows(unmerged_table)

        self.assertEqual(merged_table, calculated_merged_table)

    def test_kadena_1_72hr_table_3(self):

        # Load merged table
        merged_table = Table.load_state('tests/unit-test-assets/merge_table_rows/kadena_1_72hr_table-3-merged.pkl')

        # Load unmerged table
        unmerged_table = Table.load_state('tests/unit-test-assets/merge_table_rows/kadena_1_72hr_table-3.pkl')

        # Merge table rows
        calculated_merged_table = merge_table_rows(unmerged_table)

        self.assertEqual(merged_table, calculated_merged_table)

    def test_hickam_1_72hr_table_1(self):

        # Load merged table
        merged_table = Table.load_state('tests/unit-test-assets/merge_table_rows/hickam_1_72hr_table-1-merged.pkl')

        # Load unmerged table
        unmerged_table = Table.load_state('tests/unit-test-assets/merge_table_rows/hickam_1_72hr_table-1.pkl')

        # Merge table rows
        calculated_merged_table = merge_table_rows(unmerged_table)

        self.assertEqual(merged_table, calculated_merged_table)

class TestColumnInference(unittest.TestCase):

    def test_infer_roll_call_column_index(self):
        '''
        To see what the tables look like in the tests below check for the .txt file that has the same name as the .pkl file.

        Example:
        tests/unit-test-assets/infer_roll_call_column_index/sigonella_1_72hr_table-7.pkl -> tests/unit-test-assets/infer_roll_call_column_index/sigonella_1_72hr_table-7.txt
        '''

        # Load tables with no column headers
        no_headers_table_col0 = Table.load_state("tests/unit-test-assets/infer_roll_call_column_index/sigonella_1_72hr_table-7.pkl")
        no_headers_table_col1 = Table.load_state("tests/unit-test-assets/infer_roll_call_column_index/sigonella_1_72hr_table-7_rollcall_in_col_1.pkl")
        no_headers_table_col2 = Table.load_state("tests/unit-test-assets/infer_roll_call_column_index/sigonella_1_72hr_table-7_rollcall_in_col_2.pkl")

        # Load table with column headers
        headers_table = Table.load_state("tests/unit-test-assets/infer_roll_call_column_index/sigonella_1_72hr_table-8.pkl")
        headers_table_col1 = Table.load_state("tests/unit-test-assets/infer_roll_call_column_index/sigonella_1_72hr_table-8_rollcall_in_col_1.pkl")
        headers_table_col2 = Table.load_state("tests/unit-test-assets/infer_roll_call_column_index/sigonella_1_72hr_table-8_rollcall_in_col_2.pkl")

        # Create empty table
        empty_table = Table()

        # Create tables without roll call column
        no_roll_call_col_table_col0 = delete_column(no_headers_table_col0, 0)
        no_roll_call_col_table_col1 = delete_column(no_headers_table_col1, 1)
        no_roll_call_col_table_col2 = delete_column(no_headers_table_col2, 2)
        no_roll_call_col_headers_table = delete_column(headers_table, 0)

        # Infer roll call column index with no column headers
        self.assertEqual(infer_roll_call_column_index(no_headers_table_col0), 0)
        self.assertEqual(infer_roll_call_column_index(no_headers_table_col1), 1)
        self.assertEqual(infer_roll_call_column_index(no_headers_table_col2), 2)

        # Infer roll call column index with column headers
        self.assertEqual(infer_roll_call_column_index(headers_table), 0)
        self.assertEqual(infer_roll_call_column_index(headers_table_col1), 1)
        self.assertEqual(infer_roll_call_column_index(headers_table_col2), 2)

        # Infer roll call column index with empty table
        self.assertEqual(infer_roll_call_column_index(empty_table), -1)

        # Infer roll call column index with no roll call column
        self.assertEqual(infer_roll_call_column_index(no_roll_call_col_table_col0), -1)
        self.assertEqual(infer_roll_call_column_index(no_roll_call_col_table_col1), -1)
        self.assertEqual(infer_roll_call_column_index(no_roll_call_col_table_col2), -1)
        self.assertEqual(infer_roll_call_column_index(no_roll_call_col_headers_table), -1)

    def test_infer_seats_column_index(self):
        '''
        To see what the tables look like in the tests below check for the .txt file that has the same name as the .pkl file.

        Example:
        tests/unit-test-assets/infer_seats_column_index/sigonella_1_72hr_table-7_seats_in_col_0.pkl -> tests/unit-test-assets/infer_seats_column_index/sigonella_1_72hr_table-7_seats_in_col_0.txt
        '''

        # Load tables with no column headers
        no_headers_table_col0 = Table.load_state("tests/unit-test-assets/infer_seats_column_index/sigonella_1_72hr_table-7_seats_in_col_0.pkl")
        no_headers_table_col1 = Table.load_state("tests/unit-test-assets/infer_seats_column_index/sigonella_1_72hr_table-7_seats_in_col_1.pkl")
        no_headers_table_col2 = Table.load_state("tests/unit-test-assets/infer_seats_column_index/sigonella_1_72hr_table-7_seats_in_col_2.pkl")

        # Load table with column headers
        headers_table_col0 = Table.load_state("tests/unit-test-assets/infer_seats_column_index/sigonella_1_72hr_table-8_seats_in_col_0.pkl")
        headers_table_col1 = Table.load_state("tests/unit-test-assets/infer_seats_column_index/sigonella_1_72hr_table-8_seats_in_col_1.pkl")
        headers_table_col2 = Table.load_state("tests/unit-test-assets/infer_seats_column_index/sigonella_1_72hr_table-8_seats_in_col_2.pkl")

        # Create empty table
        empty_table = Table()

        # Create tables without seats column
        no_seats_col_table_col0 = delete_column(no_headers_table_col0, 0)
        no_seats_col_table_col1 = delete_column(no_headers_table_col1, 1)
        no_seats_col_table_col2 = delete_column(no_headers_table_col2, 2)
        no_seats_coll_headers_table = delete_column(headers_table_col2, 2)

        # Infer seats column index with no column headers
        self.assertEqual(infer_seats_column_index(no_headers_table_col0), 0)
        self.assertEqual(infer_seats_column_index(no_headers_table_col1), 1)
        self.assertEqual(infer_seats_column_index(no_headers_table_col2), 2)

        # Infer seats column index with column headers
        self.assertEqual(infer_seats_column_index(headers_table_col0), 0)
        self.assertEqual(infer_seats_column_index(headers_table_col1), 1)
        self.assertEqual(infer_seats_column_index(headers_table_col2), 2)

        # Infer seats column index with empty table
        self.assertEqual(infer_seats_column_index(empty_table), -1)

        # Infer seats column index with no seats column
        self.assertEqual(infer_seats_column_index(no_seats_col_table_col0), -1)
        self.assertEqual(infer_seats_column_index(no_seats_col_table_col1), -1)
        self.assertEqual(infer_seats_column_index(no_seats_col_table_col2), -1)
        self.assertEqual(infer_seats_column_index(no_seats_coll_headers_table), -1)

    def test_infer_destinations_column_index(self):
        '''
        To see what the tables look like in the tests below check for the .txt file that has the same name as the .pkl file.

        Example:
        tests/unit-test-assets/infer_destinations_column_index/sigonella_1_72hr_table-7_destinations_in_col_0.pkl -> tests/unit-test-assets/infer_destinations_column_index/sigonella_1_72hr_table-7_destinations_in_col_0.txt
        '''

        # Load tables with no column headers
        no_headers_table_col0 = Table.load_state("tests/unit-test-assets/infer_destinations_column_index/sigonella_1_72hr_table-7_destinations_in_col_0.pkl")
        no_headers_table_col1 = Table.load_state("tests/unit-test-assets/infer_destinations_column_index/sigonella_1_72hr_table-7_destinations_in_col_1.pkl")
        no_headers_table_col2 = Table.load_state("tests/unit-test-assets/infer_destinations_column_index/sigonella_1_72hr_table-7_destinations_in_col_2.pkl")

        # Load table with column headers
        headers_table_col0 = Table.load_state("tests/unit-test-assets/infer_destinations_column_index/sigonella_1_72hr_table-8_destinations_in_col_0.pkl")
        headers_table_col1 = Table.load_state("tests/unit-test-assets/infer_destinations_column_index/sigonella_1_72hr_table-8_destinations_in_col_1.pkl")
        headers_table_col2 = Table.load_state("tests/unit-test-assets/infer_destinations_column_index/sigonella_1_72hr_table-8_destinations_in_col_2.pkl")

        # Create empty table
        empty_table = Table()

        # Create tables without destinations column
        no_destinations_col_table_col0 = delete_column(no_headers_table_col0, 0)
        no_destinations_col_table_col1 = delete_column(no_headers_table_col1, 1)
        no_destinations_col_table_col2 = delete_column(no_headers_table_col2, 2)
        no_destinations_coll_headers_table = delete_column(headers_table_col2, 2)

        # Infer destinations column index with no column headers
        self.assertEqual(infer_destinations_column_index(no_headers_table_col0), 0)
        self.assertEqual(infer_destinations_column_index(no_headers_table_col1), 1)
        self.assertEqual(infer_destinations_column_index(no_headers_table_col2), 2)

        # Infer destinations column index with column headers
        self.assertEqual(infer_destinations_column_index(headers_table_col0), 0)
        self.assertEqual(infer_destinations_column_index(headers_table_col1), 1)
        self.assertEqual(infer_destinations_column_index(headers_table_col2), 2)

        # Infer destinations column index with empty table
        self.assertEqual(infer_destinations_column_index(empty_table), -1)

        # Infer destinations column index with no destinations column
        self.assertEqual(infer_destinations_column_index(no_destinations_col_table_col0), -1)
        self.assertEqual(infer_destinations_column_index(no_destinations_col_table_col1), -1)
        self.assertEqual(infer_destinations_column_index(no_destinations_col_table_col2), -1)
        self.assertEqual(infer_destinations_column_index(no_destinations_coll_headers_table), -1)
        