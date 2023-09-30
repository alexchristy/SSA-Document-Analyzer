import unittest
from table import Table
from table_utils import populate_merged_row_seat_columns, _get_merge_row_groups, _merge_grouped_rows, merge_table_rows

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