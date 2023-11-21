import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pytz  # type: ignore
from firebase_admin import (  # type: ignore
    credentials,
    firestore,
    get_app,
    initialize_app,
)
from google.cloud.firestore import SERVER_TIMESTAMP  # type: ignore

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
        str: The hash value of the PDF document if it exists in Firestore, otherwise empty string.

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
            msg = (
                f"An error occurred while retrieving pdf hash with s3 object path: {e}"
            )
            raise RuntimeError(msg) from e

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
            msg = f"An error occurred while updating the job status: {e}"
            raise RuntimeError(msg) from e

    def add_job_timestamp(
        self: "FirestoreClient", job_id: str, field_name: str
    ) -> None:
        """Add a timestamp to the job document in the Textract_Jobs collection.

        The timestamp added is the server's current time.

        Args:
        ----
        job_id (str): The ID of the Textract job.
        field_name (str): The field name for the timestamp.

        """
        try:
            job_ref = self.db.collection("Textract_Jobs").document(job_id)

            # Update the specified field in the job document to the server timestamp
            job_ref.update({field_name: SERVER_TIMESTAMP})

            logging.info(
                "Successfully added a %s timestamp to job %s", field_name, job_id
            )
        except Exception as e:
            msg = f"An error occurred while adding the {field_name} timestamp to the job: {e}"
            logging.error(msg)
            raise Exception(msg) from e

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
            msg = f"An error occurred while adding the flight IDs to the job: {e}"
            logging.error(msg)
            raise Exception(msg) from e

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
            msg = f"An error occurred while adding the flight IDs to the PDF: {e}"
            logging.error(msg)
            raise Exception(msg) from e

    def store_flight(self: "FirestoreClient", flight: Flight) -> None:
        """Store a flight object into the FLIGHT_CURRENT_COLLECTION.

        Args:
        ----
        flight (Flight): The Flight object to insert.

        """
        try:
            flight_collection = os.getenv(
                "FLIGHT_CURRENT_COLLECTION", "Current_Flights"
            )

            if flight_collection == "Current_Flights":
                logging.warning(
                    "FLIGHT_CURRENT_COLLECTION environment variable not set. Using default value: Current_Flights"
                )

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
            msg = f"An error occurred while inserting the flight object: {e}"
            logging.critical(msg)
            raise Exception(msg) from e

    def archive_flight(self: "FirestoreClient", flight: Flight) -> None:
        """Archive a flight object into the FLIGHT_ARCHIVE_COLLECTION.

        Args:
        ----
        flight (Flight): The Flight object to insert.

        """
        try:
            flight_collection = os.getenv("FLIGHT_ARCHIVE_COLLECTION", "Flight_Archive")

            if flight_collection == "Flight_Archive":
                logging.warning(
                    "FLIGHT_ARCHIVE_COLLECTION environment variable not set. Using default value: Flight_Archive"
                )

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
            msg = f"An error occurred while inserting the flight object: {e}"
            logging.critical(msg)
            raise Exception(msg) from e

    def get_terminal_name_by_pdf_hash(self: "FirestoreClient", pdf_hash: str) -> str:
        """Get name of terminal that owns the PDF identified by the supplied hash.

        This function returns the name of the terminal that owns the PDF that is
        identified by the supplied hash. Search is performed in the PDF_ARCHIVE_COLLECTION.

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
        logging.error(
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

        msg = "Unable to get PDF type from the hash."
        raise Exception(msg)

    def get_flights_by_terminal(self: "FirestoreClient", terminal: str) -> List[Flight]:
        """Retrieve all flights with a specified origin terminal.

        Args:
        ----
        terminal (str): The name of the origin terminal.

        Returns:
        -------
        List[Flight]: A list of Flight objects.
        """
        flights_list = []  # Initialize an empty list to store Flight objects

        try:
            # Get the flight collection from environment variable or use default
            flight_current_collection = os.getenv(
                "FLIGHT_CURRENT_COLLECTION", "Current_Flights"
            )

            # Create a query to search for flights originating from the specified terminal
            query = self.db.collection(flight_current_collection).where(
                "origin_terminal", "==", terminal
            )

            # Execute the query and get the results
            query_results = query.get()

            # Convert query results to list of Flight objects
            for doc in query_results:
                doc_dict = doc.to_dict()
                flight = Flight.from_dict(doc_dict)
                if flight:
                    flights_list.append(flight)

            if not flights_list:
                logging.info("No flights found originating from terminal: %s", terminal)
                return []

            logging.info(
                "Successfully retrieved flights originating from terminal: %s", terminal
            )

            return flights_list

        except Exception as e:
            logging.critical(
                "An error occurred while retrieving flights by terminal: %s", e
            )
            raise e

    def delete_flight_by_id(self: "FirestoreClient", document_id: str) -> None:
        """Delete a flight document based on its Firestore document ID.

        Args:
        ----
        document_id (str): The Firestore document ID of the flight to be deleted.

        """
        try:
            # Determine the collection name from environment variable or use a default
            flight_current_collection = os.getenv(
                "FLIGHT_CURRENT_COLLECTION", "Current_Flights"
            )

            # Create a reference to the document
            self.db.collection(flight_current_collection).document(document_id).delete()

            logging.info(
                "Successfully deleted flight with document ID: %s", document_id
            )
        except Exception as e:
            msg = f"An error occurred while deleting the flight with document ID {document_id}: {e}"
            logging.error(msg)
            raise Exception(msg) from e

    def insert_document_with_id(
        self: "FirestoreClient", collection_name: str, doc_id: str, document_data: dict
    ) -> None:
        """Insert a document into a specified Firestore collection with a given document ID.

        Args:
        ----
            collection_name (str): The name of the Firestore collection.
            doc_id (str): The ID for the new document.
            document_data (dict): The data to insert into the document.
        """
        try:
            # Insert the document into the Firestore collection with the specified ID
            self.db.collection(collection_name).document(doc_id).set(document_data)
            logging.info(
                "Document with ID %s inserted into collection %s",
                doc_id,
                collection_name,
            )
        except Exception as e:
            logging.error(
                "Failed to insert document with ID %s into collection %s: %s",
                doc_id,
                collection_name,
                e,
            )
            raise e

    def delete_document_by_id(
        self: "FirestoreClient", collection_name: str, doc_id: str
    ) -> None:
        """Delete a document from a specified Firestore collection by document ID.

        Args:
        ----
            collection_name (str): The name of the Firestore collection.
            doc_id (str): The ID of the document to delete.
        """
        try:
            # Delete the document from the Firestore collection
            self.db.collection(collection_name).document(doc_id).delete()
            logging.info(
                "Document with ID %s deleted from collection %s",
                doc_id,
                collection_name,
            )
        except Exception as e:
            logging.error(
                "Failed to delete document with ID %s from collection %s: %s",
                doc_id,
                collection_name,
                e,
            )
            raise e

    def append_to_doc(
        self: "FirestoreClient",
        collection_name: str,
        document_id: str,
        values_to_append: Dict[str, Any],
    ) -> None:
        """Append attributes to a Firestore document.

        Args:
        ----
        collection_name (str): The name of the collection where the document resides.
        document_id (str): The ID of the document to append data to.
        values_to_append (dict): The dictionary of values to append to the document.
        """
        try:
            # Get a reference to the document
            doc_ref = self.db.collection(collection_name).document(document_id)

            # Append the provided attributes to the document
            doc_ref.set(values_to_append, merge=True)

            logging.info(
                "Successfully appended %s to document with ID %s in collection %s",
                values_to_append,
                document_id,
                collection_name,
            )
        except Exception as e:
            logging.error(
                "Failed to append values to document with ID %s in collection %s: %s",
                document_id,
                collection_name,
                e,
            )
            raise e

    def get_all_failed_proc_72_flights(
        self: "FirestoreClient",
        lookback_seconds: Optional[int] = None,
        buffer_seconds: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Get all textract jobs that failed to process 72hr tables to flights.

        Query for Textract_Jobs with 'finished_72hr_processing' as null and
        'started_72hr_processing' not null within the optional lookback time window
        up until the optional buffer time from now.

        Args:
        ----
        lookback_seconds (int, optional): The number of seconds to look back from now.
                                          If None, retrieves all matching documents.
        buffer_seconds (int, optional): The number of seconds to look back from the current time
                                        for the end of the lookback period. Defaults to None.

        Returns:
        -------
        list: A list of dictionaries representing the queried documents.
        """
        # Reference to the collection
        collection_ref = self.db.collection("Textract_Jobs")

        logging.info(
            "Querying for failed 72hr processing jobs with lookback: %s and buffer: %s",
            lookback_seconds,
            buffer_seconds,
        )

        # Begin constructing the query
        query = collection_ref.where("finished_72hr_processing", "==", None)

        # Determine the current time considering the buffer, if specified
        current_time = datetime.now(tz=pytz.UTC)
        if buffer_seconds is not None:
            current_time -= timedelta(seconds=buffer_seconds)

        # If a lookback time is specified, adjust the query
        if lookback_seconds is not None:
            lookback_time = current_time - timedelta(seconds=lookback_seconds)
            query = query.where("started_72hr_processing", ">", lookback_time)

        try:
            # Get the query results
            results = query.stream()

            # Create a list of dictionaries from the documents
            jobs = []
            for doc in results:
                doc_dict = doc.to_dict()
                started_processing_time = doc_dict.get("started_72hr_processing")
                # Ensure the started_processing_time is within the current time considering the buffer
                if started_processing_time and started_processing_time <= current_time:
                    jobs.append({**doc_dict, "job_id": doc.id})
            return jobs

        except Exception as e:
            logging.error("An error occurred while querying the documents: %s", e)
            return []

    def get_all_failed_textract_to_tables(
        self: "FirestoreClient",
        lookback_seconds: Optional[int] = None,
        buffer_seconds: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Get all textract jobs that failed to turn the textract response to tables.

        Query for Textract_Jobs with 'tables_parsed_finished' as null and
        'tables_parsed_started' not null within the optional lookback time window
        up until the optional buffer time from now.

        Args:
        ----
        lookback_seconds (int, optional): The number of seconds to look back from now.
                                          If None, retrieves all matching documents.
        buffer_seconds (int, optional): The number of seconds to look back from the current time
                                        for the end of the lookback period. Defaults to None.

        Returns:
        -------
        list: A list of dictionaries representing the queried documents.
        """
        # Reference to the collection
        collection_ref = self.db.collection("Textract_Jobs")

        logging.info(
            "Querying for failed 72hr processing jobs with lookback: %s and buffer: %s",
            lookback_seconds,
            buffer_seconds,
        )

        # Begin constructing the query
        query = collection_ref.where("tables_parsed_finished", "==", None)

        # Determine the current time considering the buffer, if specified
        current_time = datetime.now(tz=pytz.UTC)
        if buffer_seconds is not None:
            current_time -= timedelta(seconds=buffer_seconds)

        # If a lookback time is specified, adjust the query
        if lookback_seconds is not None:
            lookback_time = current_time - timedelta(seconds=lookback_seconds)
            query = query.where("tables_parsed_started", ">", lookback_time)

        try:
            # Get the query results
            results = query.stream()

            # Create a list of dictionaries from the documents
            jobs = []
            for doc in results:
                doc_dict = doc.to_dict()
                started_processing_time = doc_dict.get("tables_parsed_started")
                # Ensure the started_processing_time is within the current time considering the buffer
                if started_processing_time and started_processing_time <= current_time:
                    jobs.append({**doc_dict, "job_id": doc.id})
            return jobs

        except Exception as e:
            logging.error("An error occurred while querying the documents: %s", e)
            return []

    def get_terminal_dict_by_name(
        self: "FirestoreClient", terminal_name: str
    ) -> Dict[str, Any]:
        """Get a terminal document from Firestore by name.

        Args:
        ----
        terminal_name (str): The name of the terminal to retrieve.

        Returns:
        -------
        dict: A dictionary representing the terminal document.
        """
        terminal_collection = os.getenv("TERMINAL_COLLECTION", "Terminals")

        if terminal_collection == "Terminals":
            logging.warning(
                "TERMINAL_COLLECTION environment variable not set. Using default value: Terminals"
            )

        # Create a reference to the document
        doc_ref = self.db.collection(terminal_collection).document(terminal_name)

        # Try to retrieve the document
        doc = doc_ref.get()

        # Check if the document exists
        if doc.exists:
            return doc.to_dict()

        msg = f"Terminal {terminal_name} does not exist."
        logging.critical(msg)
        raise Exception(msg)
