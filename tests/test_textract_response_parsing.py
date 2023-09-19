import json
import unittest
from unittest.mock import patch, MagicMock
import sys
import hashlib
from recieve_pdf_data_textract import get_document_analysis_results, parse_sns_event, get_lowest_confidence_row, reprocess_tables
from table_utils import gen_tables_from_textract_response
from s3_bucket import S3Bucket
import logging
from firestore_db import FirestoreClient

# Include test data directories
sys.path.append('tests/textract-responses')
sys.path.append('tests/sns-event-messages')

def lambda_test_handler(event, context):

    # Parse the SNS message
    job_id, status, s3_object_path, s3_bucket_name = parse_sns_event(event)

    # Initialize S3 client
    s3_client = S3Bucket(bucket_name=s3_bucket_name)

    if not job_id or not status:
        logging.error("JobId or Status missing in SNS message.")
        return

    # Update the job status in Firestore
    firestore_client = FirestoreClient()
    firestore_client.update_job_status(job_id, status)

    # If job failed exit program
    if status != 'SUCCEEDED':
        raise("Job did not succeed.")

    response = context # textract_client.get_document_analysis(JobId=job_id)

    tables = gen_tables_from_textract_response(response)

    # List to hold tables needing reprocessing
    tables_to_reprocess = []

    # Iterate through tables to find low confidence rows
    for table in tables:

        # Get the lowest confidence row
        _, lowest_confidence = get_lowest_confidence_row(table)

        # If the lowest confidence row is below the threshold
        # add the table to the list of tables to reprocess
        if lowest_confidence < 80:
            tables_to_reprocess.append(table)

    # Reprocess tables with low confidence rows
    reprocess_tables(tables=tables_to_reprocess, s3_client=s3_client, s3_object_path=s3_object_path, response=response)

    # Create a concated string of the markdown tables
    # to hash for testing
    table_str = ""
    for table in tables:

        table_str += table.to_markdown()
        
    return table_str

class TestTextractResponseParsing(unittest.TestCase):

    @patch('boto3.client')
    def test_single_page_get_document_analysis_results(self, mock_boto_client):

        # Import bwi_1_72hr known single page response
        from bwi_1_72hr_textract_response import bwi_1_72hr_textract_response as bwi_1_72hr_single_page_response
        from bwi_1_72hr_sns_messages import bwi_1_72hr_successful_job_sns_message as bwi_1_72hr_job_id
        
        # Setup mock
        mock_textract_client = MagicMock()
        mock_textract_client.get_document_analysis.side_effect = [bwi_1_72hr_single_page_response]
        
        mock_boto_client.return_value = mock_textract_client

        # Test your function
        result = get_document_analysis_results(mock_textract_client, 'bwi_1_72hr_job_id')
        
        self.assertEqual(result, bwi_1_72hr_single_page_response['Blocks'])

    @patch('boto3.client')
    def test_multi_page_get_document_analysis_results(self, mock_boto_client):

        # Load partial sigonella_1_72hr partial responses
        with open('tests/textract-responses/sigonella_1_72hr_page_1_response.json', 'r') as f:
            sigonella_1_72hr_partial_response_1 = json.load(f)

        with open('tests/textract-responses/sigonella_1_72hr_page_2_response.json', 'r') as f:
            sigonella_1_72hr_partial_response_2 = json.load(f)

        # Load complete sigonella_1_72hr response
        from sigonella_1_72hr_textract_response import sigonella_1_72hr_textract_response as sigonella_1_72hr_response

        # Setup mock
        mock_textract_client = MagicMock()
        mock_textract_client.get_document_analysis.side_effect = [sigonella_1_72hr_partial_response_1, sigonella_1_72hr_partial_response_2]

        mock_boto_client.return_value = mock_textract_client

        # Test your function
        result = get_document_analysis_results(mock_textract_client, 'sigonella_1_72hr_job_id')

        self.assertEqual(result, sigonella_1_72hr_response)

    def test_bwi_1_72hr(self):

        # Import test data
        from bwi_1_72hr_sns_messages import bwi_1_72hr_successful_job_sns_message as sns_message
        from bwi_1_72hr_textract_response import bwi_1_72hr_textract_response as textract_response
        
        table_string = lambda_test_handler(sns_message, textract_response)

        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, '12aa0730b16eb4385847e101818e8f61b5eab874a2340d0c09a1a41bd987b9c8')

    def test_norfolk_1_72hr(self):

        # Import test data
        from norfolk_1_72hr_sns_messages import norfolk_1_72hr_successful_job_sns_message as sns_message
        from norfolk_1_72hr_textract_response import norfolk_1_72hr_textract_response as textract_response
        
        table_string = lambda_test_handler(sns_message, textract_response)

        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, 'ed62c9ee60dbaff72dd34f8423e7a12e8cd6853618451740f5998e6bac7fdce9')

    def test_iwakuni_1_72hr(self):

        # Import test data
        from iwakuni_1_72hr_sns_messages import iwakuni_1_72hr_successful_job_sns_message as sns_message
        from iwakuni_1_72hr_textract_response import iwakuni_1_72hr_textract_response as textract_response

        table_string = lambda_test_handler(sns_message, textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, '219c443421e3b572e29b1d200051219e6d2d61366bbc722ba1105279cfa7e95b')

    def test_ramstein_1_72hr(self):

        # Import test data
        from ramstein_1_72hr_sns_messages import ramstein_1_72hr_successful_job_sns_message as sns_message
        from ramstein_1_72hr_textract_response import ramstein_1_72hr_textract_response as textract_response

        table_string = lambda_test_handler(sns_message, textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, 'f977773d0b330c5a392aeafa95615d93e4384d6b5f248dcdbbe829945f7315ed')

    def test_andersen_1_72hr(self):

        # Import test data
        from andersen_1_72hr_sns_messages import andersen_1_72hr_successful_job_sns_message as sns_message
        from andersen_1_72hr_textract_response import andersen_1_72hr_textract_response as textract_response

        table_string = lambda_test_handler(sns_message, textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, '9707c8e298e17fa427a27ba63a8c487a712fa304b517e8f921454a08c7337b6c')

    def test_incirlik_1_72hr(self):

        # Import test data
        from incirlik_1_72hr_sns_messages import incirlik_1_72hr_successful_job_sns_message as sns_message
        from incirlik_1_72hr_textract_response import incirlik_1_72hr_textract_response as textract_response

        table_string = lambda_test_handler(sns_message, textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, 'cb8e82330f5cbdf24f56b2f96bb85a2c8078a0e750430e7c7e9ee9046f958bf3')

    def test_misawa_1_72hr(self):

        # Import test data
        from misawa_1_72hr_sns_messages import misawa_1_72hr_successful_job_sns_message as sns_message
        from misawa_1_72hr_textract_response import misawa_1_72hr_textract_response as textract_response
        
        table_string = lambda_test_handler(sns_message, textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, 'e89089e332e91a4673aede03cf1f9ab46950b236b12c88d3b652fc1ad9738865')

    def test_elmendorf_1_72hr(self):

        # Import test data
        from elmendorf_1_72hr_sns_messages import elmendorf_1_72hr_successful_job_sns_message as sns_message
        from elmendorf_1_72hr_textract_response import elmendorf_1_72hr_textract_response as textract_response

        table_string = lambda_test_handler(sns_message, textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, '2c276921b48d6a526e2ba1ef5a4fa8f72d0b388bf609ee9c30e8343a51df453d')

    def test_guantanamo_1_72hr(self):

        # Import test data
        from guantanamo_1_72hr_sns_messages import guantanamo_1_72hr_successful_job_sns_message as sns_message
        from guantanamo_1_72hr_textract_response import guantanamo_1_72hr_textract_response as textract_response

        table_string = lambda_test_handler(sns_message, textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, '4f94198c375d4e7ed68953f8f0d0b51894e816c01909879b3c4c30502ab064b5')

    def test_bahrain_1_72hr(self):

        # Import test data
        from bahrain_1_72hr_sns_messages import bahrain_1_72hr_successful_job_sns_message as sns_message
        from bahrain_1_72hr_textract_response import bahrain_1_72hr_textract_response as textract_response

        table_string = lambda_test_handler(sns_message, textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, '4ee0f47eb822595597bdd51d3bd83d8df5ebd1730ecfdc8aa5de6cad1a86ee32')

    def test_al_udeid_1_72hr(self):

        # Import test data
        from al_udeid_1_72hr_sns_messages import al_udeid_1_72hr_successful_job_sns_message as sns_message
        from al_udeid_1_72hr_textract_response import al_udeid_1_72hr_textract_response as textract_response

        table_string = lambda_test_handler(sns_message, textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, 'd36db2516c24464dcbb3f4c0058d97cc1a3efa875087bfd6d1739980fd974187')
        
    def test_charleston_1_72hr(self):

        # Import test data
        from charleston_1_72hr_sns_messages import charleston_1_72hr_successful_job_sns_message as sns_message
        from charleston_1_72hr_textract_response import charleston_1_72hr_textract_response as textract_response

        table_string = lambda_test_handler(sns_message, textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, 'f7a1a7e60e442fffae46b3ac5ce0a98bef359c2b11c6075acdaac3dea35807b0')

    def test_dover_1_72hr(self):

        # Import test data
        from dover_1_72hr_sns_messages import dover_1_72hr_successful_job_sns_message as sns_message
        from dover_1_72hr_textract_response import dover_1_72hr_textract_response as textract_response

        table_string = lambda_test_handler(sns_message, textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash,'832a4aaaf46bd39705c6b7fdb2beb88be02df5c68052e764eaea434abe3232eb')

    def test_fairchild_1_72hr(self):

        # Import test data
        from fairchild_1_72hr_sns_messages import fairchild_1_72hr_successful_job_sns_message as sns_message
        from fairchild_1_72hr_textract_response import fairchild_1_72hr_textract_response as textract_response

        table_string = lambda_test_handler(sns_message,textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, '0215633345159add231d6cd8237a4b32fa0e75f45849f4278f3eba38237f5c58')

    def test_hickam_1_72hr(self):

        # Import test data
        from hickam_1_72hr_sns_messages import hickam_1_72hr_successful_job_sns_message as sns_message
        from hickam_1_72hr_textract_response import hickam_1_72hr_textract_response as textract_response

        table_string = lambda_test_handler(sns_message,textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, '44e3d8ab37eafae8d42aa32328261b6fd6092212f952149a69d58c2daec297cf')

    def test_andrews_1_72hr(self):

        # Import test data
        from andrews_1_72hr_sns_messages import andrews_1_72hr_successful_job_sns_message as sns_message
        from andrews_1_72hr_textract_response import andrews_1_72hr_textract_response as textract_response

        table_string = lambda_test_handler(sns_message,textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, '61bafc6a166da96013d19afabe2f5a660499e7bde01046ce21c57cdf57a308f0')

    def test_kadena_1_72hr(self):

        # Import test data
        from kadena_1_72hr_sns_messages import kadena_1_72hr_successful_job_sns_message as sns_message
        from kadena_1_72hr_textract_response import kadena_1_72hr_textract_response as textract_response

        table_string = lambda_test_handler(sns_message,textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, 'ed8cea51777d198503a8e5eddaadf41f8b7b6d955ce1ede58f7b9843f600d23f')

    def test_little_rock_1_72hr(self):
        # Note that for this test little_rock_1_72hr_textract_response will return no tables. This is correct behavior.
        # This is because of the poorly formated tables in the PDF that Textract is unable to parse.

        # TODO: Update this test when fall back to camelot/text parser is implemented

        # Import test data
        from little_rock_1_72hr_sns_messages import little_rock_1_72hr_successful_job_sns_message as sns_message
        from little_rock_1_72hr_textract_response import little_rock_1_72hr_textract_response as textract_response

        table_string = lambda_test_handler(sns_message,textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855')

    def test_macdill_1_72hr(self):

        # Import test data
        from macdill_1_72hr_sns_messages import macdill_1_72hr_successful_job_sns_message as sns_message
        from macdill_1_72hr_textract_response import macdill_1_72hr_textract_response as textract_response

        table_string = lambda_test_handler(sns_message,textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, 'a5489385b655d5016e1bc5c0c50913aa0e700cd45525d257e5327ae91594c984')

    def test_mcconnell_1_72hr(self):

        # Import test data
        from mcconnell_1_72hr_sns_messages import mcconnell_1_72hr_successful_job_sns_message as sns_message
        from mcconnell_1_72hr_textract_response import mcconnell_1_72hr_textract_response as textract_response

        table_string = lambda_test_handler(sns_message,textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, '2819b4fc92e6bddbf53e56bfaa6d7817bae425cbb381f809add0d5249c46fb72')

    def test_mcguire_1_72hr(self):

        # Import test data
        from mcguire_1_72hr_sns_messages import mcguire_1_72hr_successful_job_sns_message as sns_message
        from mcguire_1_72hr_textract_response import mcguire_1_72hr_textract_response as textract_response

        table_string = lambda_test_handler(sns_message, textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, '3a97a6c137e0d8caef4321227810a8c4cc4dc5f84aa4769346300401c6cc5bcc')

    def test_naples_1_72hr(self):

        # Import test data
        from naples_1_72hr_sns_messages import naples_1_72hr_successful_job_sns_message as sns_message
        from naples_1_72hr_textract_response import naples_1_72hr_textract_response as textract_response

        table_string = lambda_test_handler(sns_message, textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash,'f3dbfc25aab4cbec9278f94b2c9e0d90ffbe2f036056ee8fb2aa0c2ff598c383')

    def test_yokota_1_72hr(self):

        # Import test data
        from yokota_1_72hr_sns_messages import yokota_1_72hr_successful_job_sns_message as sns_message
        from yokota_1_72hr_textract_response import yokota_1_72hr_textract_response as textract_response

        table_string = lambda_test_handler(sns_message, textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, '0fb9fbe762e03f3a50573be715e9cca646e2413e3155ebfabf7e89a0b51fec3d')

    def test_scott_1_72hr(self):

        # Import test data
        from scott_1_72hr_sns_messages import scott_1_72hr_successful_job_sns_message as sns_message
        from scott_1_72hr_textract_response import scott_1_72hr_textract_response as textract_response

        table_string = lambda_test_handler(sns_message, textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, 'cd286eafd9a7adb19cdd83ce72bf5a6b8745348047b7f56ed4d6c07f9448c92a')

    def test_osan_1_72hr(self):

        # Import test data
        from osan_1_72hr_sns_messages import osan_1_72hr_successful_job_sns_message as sns_message
        from osan_1_72hr_textract_response import osan_1_72hr_textract_response as textract_response

        table_string = lambda_test_handler(sns_message,textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, 'befd947d0e89f03afaaff3bbce2d1686b1bf5071a104eddbcad2eb6661647bdb')

    def test_pope_1_72hr(self):

        # Import test data
        from pope_1_72hr_sns_messages import pope_1_72hr_successful_job_sns_message as sns_message
        from pope_1_72hr_textract_response import pope_1_72hr_textract_response as textract_response

        table_string = lambda_test_handler(sns_message,textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, 'c6989c199c1849be3f7a39d82467b5fdadfed87e0d2c0ea54955ea65fff223ff')

    def test_mildenhall_1_72hr(self):

        # Import test data
        from mildenhall_1_72hr_sns_messages import mildenhall_1_72hr_successful_job_sns_message as sns_message
        from mildenhall_1_72hr_textract_response import mildenhall_1_72hr_textract_response as textract_response

        table_string = lambda_test_handler(sns_message,textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash,'99255bc65b39eeb9e0ab50eb7eb0c653a17130ef8cc669b74a55d9159dc035ea')

    def test_rota_1_72hr(self):

        # Import test data
        from rota_1_72hr_sns_messages import rota_1_72hr_successful_job_sns_message as sns_message
        from rota_1_72hr_textract_response import rota_1_72hr_textract_response as textract_response

        table_string = lambda_test_handler(sns_message, textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash,'9b287e565cb0e92da208a738134303a4dd0980530b5bace00e08d2ba1dd6cd96')

    def test_seattle_1_72hr(self):

        # Import test data
        from seattle_1_72hr_sns_messages import seattle_1_72hr_successful_job_sns_message as sns_message
        from seattle_1_72hr_textract_response import seattle_1_72hr_textract_response as textract_response

        table_string = lambda_test_handler(sns_message, textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash,'21a69718a193aaec40bbef1c53fe28b0b9eb4b3a354b2dd6d7862ab84fa654cb')

    def test_sigonella_1_72hr(self):

        # Import test data
        from sigonella_1_72hr_sns_messages import sigonella_1_72hr_successful_job_sns_message as sns_message
        from sigonella_1_72hr_textract_response import sigonella_1_72hr_textract_response as textract_response

        table_string = lambda_test_handler(sns_message, textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, '5589916fe786404e6719e787b3dcbf1ff43d629c61109ee602283e8c0fe7b17f')

    def test_souda_bay_1_72hr(self):

        # Import test data
        from souda_bay_1_72hr_sns_messages import souda_bay_1_72hr_successful_job_sns_message as sns_message
        from souda_bay_1_72hr_textract_response import souda_bay_1_72hr_textract_response as textract_response

        table_string = lambda_test_handler(sns_message, textract_response)
        table_string_hash = hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, '96e5fb868040f3af899779fdf01bda88231954496606d525279cf68dd0003dc4')

    def test_mcchord_1_72hr(self):

        # Import test data
        from mcchord_1_72hr_sns_messages import mcchord_1_72hr_successful_job_sns_message as sns_message
        from mcchord_1_72hr_textract_response import mcchord_1_72hr_textract_response as textract_response

        table_string = lambda_test_handler(sns_message, textract_response)
        table_string_hash=hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, '1037dd26251af4bf9a7e1a480b290742444a501f35ce50029f41c1eef1d88fff') 

    def test_travis_1_72hr(self):

        # Import test data
        from travis_1_72hr_sns_messages import travis_1_72hr_successful_job_sns_message as sns_message
        from travis_1_72hr_textract_response import travis_1_72hr_textract_response as textract_response

        table_string = lambda_test_handler(sns_message, textract_response)
        table_string_hash=hashlib.sha256(table_string.encode()).hexdigest()

        self.assertEqual(table_string_hash, '64591fc525d5752163e7cc884a19975c9800ac0ec63d9aec23e4bfb7882cd090')

if __name__ == '__main__':
    unittest.main()