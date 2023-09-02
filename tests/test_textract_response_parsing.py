import unittest
import sys
import hashlib
import recieve_pdf_data_textract

# Include test data directories
sys.path.append('tests/textract-responses')
sys.path.append('tests/sns-event-messages')

class TestTextractResponseParsing(unittest.TestCase):

    def test_bwi_1_72hr(self):


        # Import test data
        from bwi_1_72hr_sns_messages import bwi_1_72hr_successful_job_sns_message as sns_message
        from bwi_1_72hr_textract_response import bwi_1_72hr_textract_response as textract_response
        
        table_string = recieve_pdf_data_textract.lambda_test_handler(sns_message, textract_response)

        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        print(table_string)
        print(table_string_hash)

        self.assertEqual(table_string_hash, '12aa0730b16eb4385847e101818e8f61b5eab874a2340d0c09a1a41bd987b9c8')

    def test_norfolk_1_72hr(self):

        # Import test data
        from norfolk_1_72hr_sns_messages import norfolk_1_72hr_successful_job_sns_message as sns_message
        from norfolk_1_72hr_textract_response import norfolk_1_72hr_textract_response as textract_response
        
        table_string = recieve_pdf_data_textract.lambda_test_handler(sns_message, textract_response)

        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        print(table_string)
        print(table_string_hash)

        self.assertEqual(table_string_hash, 'ed62c9ee60dbaff72dd34f8423e7a12e8cd6853618451740f5998e6bac7fdce9')

    def test_iwakuni_1_72hr(self):

        # Import test data
        from iwakuni_1_72hr_sns_messages import iwakuni_1_72hr_successful_job_sns_message as sns_message
        from iwakuni_1_72hr_textract_response import iwakuni_1_72hr_textract_response as textract_response

        table_string = recieve_pdf_data_textract.lambda_test_handler(sns_message, textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, '219c443421e3b572e29b1d200051219e6d2d61366bbc722ba1105279cfa7e95b')

    def test_ramstein_1_72hr(self):

        # Import test data
        from ramstein_1_72hr_sns_messages import ramstein_1_72hr_successful_job_sns_message as sns_message
        from ramstein_1_72hr_textract_response import ramstein_1_72hr_textract_response as textract_response

        table_string = recieve_pdf_data_textract.lambda_test_handler(sns_message, textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, 'f977773d0b330c5a392aeafa95615d93e4384d6b5f248dcdbbe829945f7315ed')

    def test_andersen_1_72hr(self):

        # Import test data
        from andersen_1_72hr_sns_messages import andersen_1_72hr_successful_job_sns_message as sns_message
        from andersen_1_72hr_textract_response import andersen_1_72hr_textract_response as textract_response

        table_string = recieve_pdf_data_textract.lambda_test_handler(sns_message, textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, '9707c8e298e17fa427a27ba63a8c487a712fa304b517e8f921454a08c7337b6c')

if __name__ == '__main__':
    unittest.main()