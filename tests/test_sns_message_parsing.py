import unittest
from recieve_pdf_data_textract import parse_sns_event
import sys

sys.path.append("./sns-event-messages")

from bwi_1_72hr_sns_messages import *

class TestSNSEventParsing(unittest.TestCase):

    def test_sns_successful_textract_job(self):
        """
        This test is to check that the function can properly parse a SNS message
        and determine if it is a successful Textract job.
        """

        # Arrange
        event = bwi_1_72hr_successful_job_sns_message

        # Act
        _, status, _, _ = parse_sns_event(event)

        # Assert
        self.assertEqual(status, 'SUCCEEDED')

    def test_sns_failed_textract_job(self):
        """
        This test is to check that the function can properly parse a SNS message
        and determine if it is a failed Textract job.
        """

        # Arrange
        event = bwi_1_72hr_failed_job_sns_message

        # Act
        _, status, _, _ = parse_sns_event(event)

        # Assert
        self.assertEqual(status, 'FAILED')

    def test_sns_error_textract_job(self):
        """
        This test is to check that the function can properly parse a SNS message
        and determine if it is a error Textract job.
        """

        # Arrange
        event = bwi_1_72hr_error_job_sns_message

        # Act
        _, status, _, _ = parse_sns_event(event)

        # Assert
        self.assertEqual(status, 'ERROR')

if __name__ == '__main__':
    unittest.main()