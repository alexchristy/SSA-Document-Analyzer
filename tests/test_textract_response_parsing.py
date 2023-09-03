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

    def test_incirlik_1_72hr(self):

        # Import test data
        from incirlik_1_72hr_sns_messages import incirlik_1_72hr_successful_job_sns_message as sns_message
        from incirlik_1_72hr_textract_response import incirlik_1_72hr_textract_response as textract_response

        table_string = recieve_pdf_data_textract.lambda_test_handler(sns_message, textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, 'cb8e82330f5cbdf24f56b2f96bb85a2c8078a0e750430e7c7e9ee9046f958bf3')

    def test_misawa_1_72hr(self):

        # Import test data
        from misawa_1_72hr_sns_messages import misawa_1_72hr_successful_job_sns_message as sns_message
        from misawa_1_72hr_textract_response import misawa_1_72hr_textract_response as textract_response
        
        table_string = recieve_pdf_data_textract.lambda_test_handler(sns_message, textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, 'e89089e332e91a4673aede03cf1f9ab46950b236b12c88d3b652fc1ad9738865')

    def test_elmendorf_1_72hr(self):

        # Import test data
        from elmendorf_1_72hr_sns_messages import elmendorf_1_72hr_successful_job_sns_message as sns_message
        from elmendorf_1_72hr_textract_response import elmendorf_1_72hr_textract_response as textract_response

        table_string = recieve_pdf_data_textract.lambda_test_handler(sns_message, textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, '2c276921b48d6a526e2ba1ef5a4fa8f72d0b388bf609ee9c30e8343a51df453d')

    def test_guantanamo_1_72hr(self):

        # Import test data
        from guantanamo_1_72hr_sns_messages import guantanamo_1_72hr_successful_job_sns_message as sns_message
        from guantanamo_1_72hr_textract_response import guantanamo_1_72hr_textract_response as textract_response

        table_string = recieve_pdf_data_textract.lambda_test_handler(sns_message, textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, '4f94198c375d4e7ed68953f8f0d0b51894e816c01909879b3c4c30502ab064b5')

    def test_bahrain_1_72hr(self):

        # Import test data
        from bahrain_1_72hr_sns_messages import bahrain_1_72hr_successful_job_sns_message as sns_message
        from bahrain_1_72hr_textract_response import bahrain_1_72hr_textract_response as textract_response

        table_string = recieve_pdf_data_textract.lambda_test_handler(sns_message, textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, '4ee0f47eb822595597bdd51d3bd83d8df5ebd1730ecfdc8aa5de6cad1a86ee32')

    def test_al_udeid_1_72hr(self):

        # Import test data
        from al_udeid_1_72hr_sns_messages import al_udeid_1_72hr_successful_job_sns_message as sns_message
        from al_udeid_1_72hr_textract_response import al_udeid_1_72hr_textract_response as textract_response

        table_string = recieve_pdf_data_textract.lambda_test_handler(sns_message, textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, 'd36db2516c24464dcbb3f4c0058d97cc1a3efa875087bfd6d1739980fd974187')
        
if __name__ == '__main__':
    unittest.main()