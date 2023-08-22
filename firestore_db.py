from firebase_admin import credentials, firestore, initialize_app, get_app
import os

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
        
    def add_textract_job(self, job_id, s3_object_path):
        # Add the job ID to the Firestore database in a single set operation
        self.db.collection('Textract_Jobs').document(job_id).set({
            'status': 'STARTED',
            'pdfS3Path': s3_object_path
        })
    
