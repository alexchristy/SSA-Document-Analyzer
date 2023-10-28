import datetime
import logging
import os
from typing import Any, Dict, List, Optional

from firebase_admin import (  # type: ignore
    credentials,
    firestore,
    get_app,
    initialize_app,
)

from flight import Flight


class FirestoreClient:
    """A client for interacting with Firestore."""

    def __init__(self: "FirestoreClient") -> None:
        """Initialize a FirestoreClient object."""
        # Initialize app only if it hasn't been initialized yet
        try:
            self.app = get_app()
        except ValueError:
            # Get the path to the Firebase Admin SDK service account key JSON file from an environment variable
            fs_creds_path = os.getenv("FS_CRED_PATH")

            # Initialize the credentials with the JSON file
            cred = credentials.Certificate(fs_creds_path)

            # Initialize the Firebase application with the credentials
            self.app = initialize_app(cred)

        # Create the Firestore client
        self.db = firestore.client(app=self.app)

    def add_textract_job(self: "FirestoreClient", job_id: str, pdf_hash: str) -> None:
        """Add a Textract job to the Firestore database.

        Args:
        ----
        job_id (str): The ID of the Textract job.
        pdf_hash (str): The hash value of the PDF document.

        """
        # Add the job ID to the Firestore database in a single set operation
        self.db.collection("Textract_Jobs").document(job_id).set(
            {"status": "STARTED", "pdf_hash": pdf_hash}
        )

    def get_textract_job(
        self: "FirestoreClient", job_id: str
    ) -> Optional[Dict[str, Any]]:
        """Get a Textract job from Firestore.

        Args:
        ----
        job_id (str): The ID of the Textract job.

        Returns:
        -------
        Optional[Dict[str, Any]]: A dictionary representing the Textract job if it exists, otherwise None.

        """
        # Get the Textract job from Firestore
        job = self.db.collection("Textract_Jobs").document(job_id).get()

        # Check if the job exists
        if job.exists:
            return job.to_dict()

        logging.error("Job %s does not exist.", job_id)
        return None

    def get_pdf_hash_with_s3_path(self: "FirestoreClient", s3_object_path: str) -> str:
        """Get the hash value of a PDF document from Firestore using its S3 object path.

        Args:
        ----
        s3_object_path (str): The S3 object path of the PDF document.

        Returns:
        -------
        Optional[str]: The hash value of the PDF document if it exists in Firestore, otherwise None.

        """
        logging.info("Retrieving hash value for S3 object path: %s", s3_object_path)
        try:
            pdf_archive = os.getenv("PDF_ARCHIVE_COLLECTION")
            if not pdf_archive:
                logging.error("PDF_ARCHIVE_COLLECTION environment variable is not set.")
                return ""

            # Fetch the document(s) from Firestore
            query_result = (
                self.db.collection(pdf_archive)
                .where("cloud_path", "==", str(s3_object_path))
                .get()
            )

            # Check if the document exists
            if query_result:
                for doc in query_result:
                    hash_value = doc.to_dict().get("hash", "")
                    if hash_value:
                        logging.info(
                            "Successfully retrieved hash value: %s", hash_value
                        )
                        return hash_value

                    logging.warning(
                        "Document found but 'hash' attribute is missing. S3 Path: %s",
                        s3_object_path,
                    )
                    return ""

                logging.warning(
                    "No document found with matching S3 path: %s", s3_object_path
                )
                return ""

        except Exception as e:
            logging.error(
                "An error occurred while retrieving pdf hash with s3 object path: %s", e
            )
            return ""

        return ""

    def update_job_status(self: "FirestoreClient", job_id: str, status: str) -> None:
        """Update the status of a job document in the Textract_Jobs collection.

        Args:
        ----
        job_id (str): The ID of the Textract job.
        status (str): The new status to set for the job.

        """
        try:
            job_ref = self.db.collection("Textract_Jobs").document(job_id)

            # Update the 'status' field in the job document
            job_ref.update({"status": status})

            logging.info("Successfully updated job %s with status %s", job_id, status)
        except Exception as e:
            logging.error("An error occurred while updating the job status: %s", e)

    def add_job_timestamp(self: "FirestoreClient", job_id: str, timestamp: str) -> None:
        """Add a started timestamp to the job document in the Textract_Jobs collection.

        Args:
        ----
        job_id (str): The ID of the Textract job.
        timestamp (str): The timestamp to add to the job document.

        """
        try:
            job_ref = self.db.collection("Textract_Jobs").document(job_id)

            # Update the 'finished' field in the job document
            job_ref.update(
                {
                    timestamp: int(
                        datetime.datetime.now(tz=datetime.UTC).strftime("%Y%m%d%H%M%S")
                    )
                }
            )

            logging.info(
                "Successfully added a %s timestamp to job %s", timestamp, job_id
            )
        except Exception as e:
            logging.error(
                "An error occurred while adding the %s timestamp to the job: %s",
                timestamp,
                e,
            )

    def add_flight_ids_to_job(
        self: "FirestoreClient", job_id: str, flights: List[Flight]
    ) -> None:
        """Add a list of flight IDs that were create from the textract job.

        Args:
        ----
        job_id (str): The ID of the Textract job.
        flights (list): The list of Flight objects.
        """
        try:
            job_ref = self.db.collection("Textract_Jobs").document(job_id)

            # Create a list of flight IDs from the list of Flight objects
            flight_ids = [flight.flight_id for flight in flights]

            # Update the 'flight_ids' field in the job document
            job_ref.update({"flight_ids": flight_ids})

            logging.info(
                "Successfully added flight IDs to job %s: %s", job_id, flight_ids
            )
        except Exception as e:
            logging.error(
                "An error occurred while adding the flight IDs to the job: %s", e
            )

    def add_flight_ids_to_pdf(
        self: "FirestoreClient", pdf_hash: str, flight_ids: List[str]
    ) -> None:
        """Add a list of flight IDs to a PDF document in the PDF_ARCHIVE_COLLECTION.

        Args:
        ----
        pdf_hash (str): The hash of the PDF document.
        flight_ids (list): The list of flight IDs.

        """
        try:
            pdf_ref = self.db.collection(os.getenv("PDF_ARCHIVE_COLLECTION")).document(
                pdf_hash
            )

            # Add the list of flight IDs to the 'flight_ids' array in the PDF document
            pdf_ref.update({"flight_ids": firestore.ArrayUnion(flight_ids)})

            logging.info("Successfully added flight IDs to PDF %s", pdf_hash)
        except Exception as e:
            logging.error(
                "An error occurred while adding the flight IDs to the PDF: %s", e
            )

    def store_flight(self: "FirestoreClient", flight: Flight) -> None:
        """Store a flight object into the FLIGHT_CURRENT_COLLECTION.

        Args:
        ----
        flight (Flight): The Flight object to insert.

        """
        try:
            flight_collection = os.getenv("FLIGHT_CURRENT_COLLECTION")
            if not flight_collection:
                logging.error(
                    "FLIGHT_CURRENT_COLLECTION environment variable is not set."
                )
                return

            # Convert the Flight object to a dictionary
            flight_data = flight.to_dict()

            # Insert the flight object into the Firestore collection
            self.db.collection(flight_collection).document(flight.flight_id).set(
                flight_data
            )

            logging.info(
                "Successfully inserted flight with ID %s into %s",
                flight.flight_id,
                flight_collection,
            )
        except Exception as e:
            logging.error("An error occurred while inserting the flight object: %s", e)

    def get_terminal_name_by_pdf_hash(self: "FirestoreClient", pdf_hash: str) -> str:
        """Get name of terminal that owns the PDF identified by the supplied hash.

        This function returns the name of the terminal that owns the PDF that is
        identified by the supplied hash.

        Args:
        ----
        pdf_hash: The SHA-256 hash of the PDF file

        Returns:
        -------
        (str) Terminal name or None if the hash is invalid or the PDF does not exist
        """
        logging.info("Retrieving terminal name by PDF hash.")

        # Get the name of the collections from environment variables
        pdf_archive_coll = os.getenv("PDF_ARCHIVE_COLLECTION")

        # Create a reference to the document using the SHA-256 hash as the document ID
        doc_ref = self.db.collection(pdf_archive_coll).document(pdf_hash)

        # Try to retrieve the document
        doc = doc_ref.get()

        # Check if the document exists
        if doc.exists:
            # The document exists, so we retrieve its data and create a Pdf object
            logging.info(
                "Found terminal name. PDF with hash %s found in the database.", pdf_hash
            )

            # Get the document's data
            pdf_data = doc.to_dict()

            # Return the terminal name
            terminal_name = pdf_data["terminal"]
            logging.info("Terminal name: %s", terminal_name)
            return terminal_name

        # The document does not exist
        logging.warning(
            "Unable to retrieve terminal name. PDF with hash %s does not exist in the database.",
            pdf_hash,
        )

        # Return None to indicate that no PDF was found
        return ""

    def get_pdf_type_by_hash(self: "FirestoreClient", pdf_hash: str) -> str:
        """Get name of terminal that owns the PDF identified by the supplied hash.

        This function returns the name of the terminal that owns the PDF that is
        identified by the supplied hash.

        Args:
        ----
        pdf_hash: The SHA-256 hash of the PDF file

        Returns:
        -------
        (str) Terminal name or None if the hash is invalid or the PDF does not exist
        """
        logging.info("Retrieving PDF type by hash.")

        # Get the name of the collections from environment variables
        pdf_archive_coll = os.getenv("PDF_ARCHIVE_COLLECTION", "PDF_Archive")

        # Create a reference to the document using the SHA-256 hash as the document ID
        doc_ref = self.db.collection(pdf_archive_coll).document(pdf_hash)

        # Try to retrieve the document
        doc = doc_ref.get()

        # Check if the document exists
        if doc.exists:
            # The document exists, so we retrieve its data and create a Pdf object
            logging.info(
                "Found PDF type. PDF with hash %s found in the database.", pdf_hash
            )

            # Get the document's data
            pdf_data = doc.to_dict()

            # Return the terminal name
            pdf_type = pdf_data["type"]
            logging.info("PDF type: %s", pdf_type)
            return pdf_type

        # The document does not exist
        logging.warning(
            "Unable to get PDF type. PDF with hash %s does not exist in the database.",
            pdf_hash,
        )

        # Return None to indicate that no PDF was found
        return ""
