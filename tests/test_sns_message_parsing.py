import unittest
import tests.doc_analysis_responses as doc_analysis_responses
import tests.sns_event_message as sns_event_message
from recieve_pdf_data_textract import parse_sns_event

class TestSNSEventParsing(unittest.TestCase):

    def test_sns_successful_textract_job(self):
        """
        This test is to check that the function can properly parse a SNS message
        and determine if it is a successful Textract job.
        """

        # Arrange
        event = sns_event_message.sns_event_message_textract_successful_job

        # Act
        _, status = parse_sns_event(event)

        # Assert
        self.assertEqual(status, 'SUCCEEDED')

    def test_sns_failed_textract_job(self):
        """
        This test is to check that the function can properly parse a SNS message
        and determine if it is a failed Textract job.
        """

        # Arrange
        event = sns_event_message.sns_event_message_textract_failed_job

        # Act
        _, status = parse_sns_event(event)

        # Assert
        self.assertEqual(status, 'FAILED')

    def test_sns_error_textract_job(self):
        """
        This test is to check that the function can properly parse a SNS message
        and determine if it is a error Textract job.
        """

        # Arrange
        event = sns_event_message.sns_event_message_textract_error_job

        # Act
        _, status = parse_sns_event(event)

        # Assert
        self.assertEqual(status, 'ERROR')

if __name__ == '__main__':
    unittest.main()