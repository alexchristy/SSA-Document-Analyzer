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
        
    def test_charleston_1_72hr(self):

        # Import test data
        from charleston_1_72hr_sns_messages import charleston_1_72hr_successful_job_sns_message as sns_message
        from charleston_1_72hr_textract_response import charleston_1_72hr_textract_response as textract_response

        table_string = recieve_pdf_data_textract.lambda_test_handler(sns_message, textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, 'f7a1a7e60e442fffae46b3ac5ce0a98bef359c2b11c6075acdaac3dea35807b0')

    def test_dover_1_72hr(self):

        # Import test data
        from dover_1_72hr_sns_messages import dover_1_72hr_successful_job_sns_message as sns_message
        from dover_1_72hr_textract_response import dover_1_72hr_textract_response as textract_response

        table_string = recieve_pdf_data_textract.lambda_test_handler(sns_message, textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash,'832a4aaaf46bd39705c6b7fdb2beb88be02df5c68052e764eaea434abe3232eb')

    def test_fairchild_1_72hr(self):

        # Import test data
        from fairchild_1_72hr_sns_messages import fairchild_1_72hr_successful_job_sns_message as sns_message
        from fairchild_1_72hr_textract_response import fairchild_1_72hr_textract_response as textract_response

        table_string = recieve_pdf_data_textract.lambda_test_handler(sns_message,textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, '0215633345159add231d6cd8237a4b32fa0e75f45849f4278f3eba38237f5c58')

    def test_hickam_1_72hr(self):

        # Import test data
        from hickam_1_72hr_sns_messages import hickam_1_72hr_successful_job_sns_message as sns_message
        from hickam_1_72hr_textract_response import hickam_1_72hr_textract_response as textract_response

        table_string = recieve_pdf_data_textract.lambda_test_handler(sns_message,textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, '44e3d8ab37eafae8d42aa32328261b6fd6092212f952149a69d58c2daec297cf')

    def test_andrews_1_72hr(self):

        # Import test data
        from andrews_1_72hr_sns_messages import andrews_1_72hr_successful_job_sns_message as sns_message
        from andrews_1_72hr_textract_response import andrews_1_72hr_textract_response as textract_response

        table_string = recieve_pdf_data_textract.lambda_test_handler(sns_message,textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, '61bafc6a166da96013d19afabe2f5a660499e7bde01046ce21c57cdf57a308f0')

    def test_kadena_1_72hr(self):

        # Import test data
        from kadena_1_72hr_sns_messages import kadena_1_72hr_successful_job_sns_message as sns_message
        from kadena_1_72hr_textract_response import kadena_1_72hr_textract_response as textract_response

        table_string = recieve_pdf_data_textract.lambda_test_handler(sns_message,textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, 'ed8cea51777d198503a8e5eddaadf41f8b7b6d955ce1ede58f7b9843f600d23f')

    def test_little_rock_1_72hr(self):
        # Note that for this test little_rock_1_72hr_textract_response will return no tables. This is correct behavior.
        # This is because of the poorly formated tables in the PDF that Textract is unable to parse.

        # TODO: Update this test when fall back to camelot/text parser is implemented

        # Import test data
        from little_rock_1_72hr_sns_messages import little_rock_1_72hr_successful_job_sns_message as sns_message
        from little_rock_1_72hr_textract_response import little_rock_1_72hr_textract_response as textract_response

        table_string = recieve_pdf_data_textract.lambda_test_handler(sns_message,textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855')

    def test_macdill_1_72hr(self):

        # Import test data
        from macdill_1_72hr_sns_messages import macdill_1_72hr_successful_job_sns_message as sns_message
        from macdill_1_72hr_textract_response import macdill_1_72hr_textract_response as textract_response

        table_string = recieve_pdf_data_textract.lambda_test_handler(sns_message,textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, 'a5489385b655d5016e1bc5c0c50913aa0e700cd45525d257e5327ae91594c984')

    def test_mcconnell_1_72hr(self):

        # Import test data
        from mcconnell_1_72hr_sns_messages import mcconnell_1_72hr_successful_job_sns_message as sns_message
        from mcconnell_1_72hr_textract_response import mcconnell_1_72hr_textract_response as textract_response

        table_string = recieve_pdf_data_textract.lambda_test_handler(sns_message,textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, '2819b4fc92e6bddbf53e56bfaa6d7817bae425cbb381f809add0d5249c46fb72')

    def test_mcguire_1_72hr(self):

        # Import test data
        from mcguire_1_72hr_sns_messages import mcguire_1_72hr_successful_job_sns_message as sns_message
        from mcguire_1_72hr_textract_response import mcguire_1_72hr_textract_response as textract_response

        table_string = recieve_pdf_data_textract.lambda_test_handler(sns_message, textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, '3a97a6c137e0d8caef4321227810a8c4cc4dc5f84aa4769346300401c6cc5bcc')

    def test_naples_1_72hr(self):

        # Import test data
        from naples_1_72hr_sns_messages import naples_1_72hr_successful_job_sns_message as sns_message
        from naples_1_72hr_textract_response import naples_1_72hr_textract_response as textract_response

        table_string = recieve_pdf_data_textract.lambda_test_handler(sns_message, textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash,'f3dbfc25aab4cbec9278f94b2c9e0d90ffbe2f036056ee8fb2aa0c2ff598c383')

if __name__ == '__main__':
    unittest.main()