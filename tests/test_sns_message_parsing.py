import os
import sys
import unittest

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, "sns-event-messages"))

from recieve_pdf_data_textract import parse_sns_event  # noqa: E402

sys.path.append("./sns-event-messages")

from bwi_1_72hr_sns_messages import (  # noqa: E402
    bwi_1_72hr_error_job_sns_message,
    bwi_1_72hr_failed_job_sns_message,
    bwi_1_72hr_successful_job_sns_message,
)


class TestSNSEventParsing(unittest.TestCase):
    """Unit tests for the `parse_sns_event` function in the `recieve_pdf_data_textract` module."""

    def test_sns_successful_textract_job(self):  # noqa: ANN201, ANN101
        """Check that the function can properly parse a SNS message
        and determine if it is a successful Textract job.
        """  # noqa: D205
        # Arrange
        event = bwi_1_72hr_successful_job_sns_message

        # Act
        _, status, _, _ = parse_sns_event(event)

        # Assert
        self.assertEqual(status, "SUCCEEDED")

    def test_sns_failed_textract_job(self):  # noqa: ANN201, ANN101
        """Test to check that the function can properly parse a SNS message
        and determine if it is a failed Textract job.
        """  # noqa: D205
        # Arrange
        event = bwi_1_72hr_failed_job_sns_message

        # Act
        _, status, _, _ = parse_sns_event(event)

        # Assert
        self.assertEqual(status, "FAILED")

    def test_sns_error_textract_job(self):  # noqa: ANN201, ANN101
        """Test to check that the function can properly parse a SNS message
        and determine if it is a error Textract job.
        """  # noqa: D205
        # Arrange
        event = bwi_1_72hr_error_job_sns_message

        # Act
        _, status, _, _ = parse_sns_event(event)

        # Assert
        self.assertEqual(status, "ERROR")


if __name__ == "__main__":
    unittest.main()
