from firebase_admin import credentials, firestore, initialize_app, get_app
import os
import logging

class FirestoreClient:
    
    def __init__(self):
        # Initialize app only if it hasn't been initialized yet
        try:
            self.app = get_app()
        except ValueError as e:
            # Get the path to the Firebase Admin SDK service account key JSON file from an environment variable
            fs_creds_path = os.getenv('FS_CRED_PATH')
            
            # Initialize the credentials with the JSON file
            cred = credentials.Certificate(fs_creds_path)
            
            # Initialize the Firebase application with the credentials
            self.app = initialize_app(cred)
            
        # Create the Firestore client
        self.db = firestore.client(app=self.app)
        
    def add_textract_job(self, job_id, pdf_hash):
        # Add the job ID to the Firestore database in a single set operation
        self.db.collection('Textract_Jobs').document(job_id).set({
            'status': 'STARTED',
            'pdf_hash': pdf_hash
        })

    def get_pdf_hash_with_s3_path(self, s3_object_path):
        try:
            pdf_archive = os.getenv('PDF_ARCHIVE_COLLECTION')
            if not pdf_archive:
                logging.error("PDF_ARCHIVE_COLLECTION environment variable is not set.")
                return None

            # Fetch the document(s) from Firestore
            query_result = self.db.collection(pdf_archive).where('cloud_path', '==', str(s3_object_path)).get()

            # Check if the document exists
            if query_result:
                for doc in query_result:
                    hash_value = doc.to_dict().get('hash', None)
                    if hash_value:
                        logging.info(f"Successfully retrieved hash value: {hash_value}")
                        return hash_value
                    else:
                        logging.warning(f"Document found but 'hash' attribute is missing. S3 Path: {s3_object_path}")
                        return None

                logging.warning(f"No document found with matching S3 path: {s3_object_path}")
                return None

        except Exception as e:
            logging.error(f"An error occurred: {e}")
            return None