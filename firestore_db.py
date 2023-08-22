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
    
    def update_job_status(self, job_id, status):
        """
        Update the status of a job document in the Textract_Jobs collection.

        Parameters:
        job_id (str): The ID of the Textract job.
        status (str): The new status to set for the job.

        """
        try:
            job_ref = self.db.collection('Textract_Jobs').document(job_id)
            
            # Update the 'status' field in the job document
            job_ref.update({'status': status})
            
            logging.info(f"Successfully updated job {job_id} with status {status}")
        except Exception as e:
            logging.error(f"An error occurred while updating the job status: {e}")


    def add_flight_ids_to_pdf(self, pdf_hash, flight_ids):
        """
        Add a list of flight IDs to a PDF document in the PDF_ARCHIVE_COLLECTION.

        Parameters:
        pdf_hash (str): The hash of the PDF document.
        flight_ids (list): The list of flight IDs.

        """
        try:
            pdf_ref = self.db.collection(os.getenv('PDF_ARCHIVE_COLLECTION')).document(pdf_hash)

            # Add the list of flight IDs to the 'flight_ids' array in the PDF document
            pdf_ref.update({'flight_ids': firestore.ArrayUnion(flight_ids)})
            
            logging.info(f"Successfully added flight IDs to PDF {pdf_hash}")
        except Exception as e:
            logging.error(f"An error occurred while adding the flight IDs to the PDF: {e}")

    def insert_flight(self, flight):
        """
        Insert a flight object into the FLIGHT_ARCHIVE_COLLECTION.

        Parameters:
        flight (Flight): The Flight object to insert.
        
        """
        try:
            flight_collection = os.getenv('FLIGHT_ARCHIVE_COLLECTION')
            if not flight_collection:
                logging.error("FLIGHT_ARCHIVE_COLLECTION environment variable is not set.")
                return

            # Convert the Flight object to a dictionary
            flight_data = flight.to_dict()
            
            # Insert the flight object into the Firestore collection
            self.db.collection(flight_collection).document(flight.flight_id).set(flight_data)

            logging.info(f"Successfully inserted flight with ID {flight.flight_id} into {flight_collection}")
        except Exception as e:
            logging.error(f"An error occurred while inserting the flight object: {e}")
    
    def update_job_status(self, job_id, status):
        try:
            self.db.collection('Textract_Jobs').document(job_id).update({'status': status})
            logging.info(f"Successfully updated job {job_id} with status {status}")
        except Exception as e:
            logging.error(f"An error occurred while updating the job status: {e}")
