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

    def __init__(  # noqa: PLR0913 (Args neede for testing)
        self: "FirestoreClient",
        pdf_archive_coll: Optional[str] = None,
        terminal_coll: Optional[str] = None,
        textract_jobs_coll: Optional[str] = None,
        flight_current_coll: Optional[str] = None,
        flight_archive_coll: Optional[str] = None,
    ) -> None:
        """Initialize a FirestoreClient object.

        Use environment variables to initialize the Firestore client if no arguments are provided.
        """
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

        # Get the name of the collections from environment variables
        if pdf_archive_coll:
            self.pdf_archive_coll = pdf_archive_coll
        else:
            self.pdf_archive_coll = os.getenv("PDF_ARCHIVE_COLLECTION", "PDF_Archive")
            logging.info(
                "Using ENV vars for firestore PDF_ARCHIVE_COLLECTION: %s",
                self.pdf_archive_coll,
            )

        if terminal_coll:
            self.terminal_coll = terminal_coll
        else:
            self.terminal_coll = os.getenv("TERMINAL_COLLECTION", "Terminals")
            logging.info(
                "Using ENV vars for firestore TERMINAL_COLLECTION: %s",
                self.terminal_coll,
            )

        if textract_jobs_coll:
            self.textract_jobs_coll = textract_jobs_coll
        else:
            self.textract_jobs_coll = os.getenv(
                "TEXTRACT_JOBS_COLLECTION", "Textract_Jobs"
            )
            logging.info(
                "Using ENV vars for firestore TEXTRACT_JOBS_COLLECTION: %s",
                self.textract_jobs_coll,
            )

        if flight_current_coll:
            self.flight_current_coll = flight_current_coll
        else:
            self.flight_current_coll = os.getenv(
                "FLIGHT_CURRENT_COLLECTION", "Current_Flights"
            )
            logging.info(
                "Using ENV vars for firestore FLIGHT_CURRENT_COLLECTION: %s",
                self.flight_current_coll,
            )

        if flight_archive_coll:
            self.flight_archive_coll = flight_archive_coll
        else:
            self.flight_archive_coll = os.getenv(
                "FLIGHT_ARCHIVE_COLLECTION", "Archived_Flights"
            )
            logging.info(
                "Using ENV vars for firestore FLIGHT_ARCHIVE_COLLECTION: %s",
                self.flight_archive_coll,
            )

    def set_pdf_archive_coll(self: "FirestoreClient", pdf_archive_coll: str) -> None:
        """Set the name of the PDF Archive collection.

        Args:
        ----
        pdf_archive_coll (str): The name of the PDF Archive collection.

        """
        self.pdf_archive_coll = pdf_archive_coll

    def set_terminal_coll(self: "FirestoreClient", terminal_coll: str) -> None:
        """Set the name of the Terminal collection.

        Args:
        ----
        terminal_coll (str): The name of the Terminal collection.

        """
        self.terminal_coll = terminal_coll

    def set_textract_jobs_coll(
        self: "FirestoreClient", textract_jobs_coll: str
    ) -> None:
        """Set the name of the Textract Jobs collection.

        Args:
        ----
        textract_jobs_coll (str): The name of the Textract Jobs collection.

        """
        self.textract_jobs_coll = textract_jobs_coll

    def set_flight_current_coll(
        self: "FirestoreClient", flight_current_coll: str
    ) -> None:
        """Set the name of the Flight Current collection.

        Args:
        ----
        flight_current_coll (str): The name of the Flight Current collection.

        """
        self.flight_current_coll = flight_current_coll

    def set_flight_archive_coll(
        self: "FirestoreClient", flight_archive_coll: str
    ) -> None:
        """Set the name of the Flight Archive collection.

        Args:
        ----
        flight_archive_coll (str): The name of the Flight Archive collection.

        """
        self.flight_archive_coll = flight_archive_coll

    def add_textract_job(self: "FirestoreClient", job_id: str, pdf_hash: str) -> None:
        """Add a Textract job to the Firestore database.

        Args:
        ----
        job_id (str): The ID of the Textract job.
        pdf_hash (str): The hash value of the PDF document.

        """
        # Add the job ID to the Firestore database in a single set operation
        self.db.collection(self.textract_jobs_coll).document(job_id).set(
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
        job = self.db.collection(self.textract_jobs_coll).document(job_id).get()

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
            # Fetch the document(s) from Firestore
            query_result = (
                self.db.collection(self.pdf_archive_coll)
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
            job_ref = self.db.collection(self.textract_jobs_coll).document(job_id)

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
            job_ref = self.db.collection(self.textract_jobs_coll).document(job_id)

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
            job_ref = self.db.collection(self.textract_jobs_coll).document(job_id)

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
            pdf_ref = self.db.collection(self.pdf_archive_coll).document(pdf_hash)

            # Add the list of flight IDs to the 'flight_ids' array in the PDF document
            pdf_ref.update({"flight_ids": firestore.ArrayUnion(flight_ids)})

            logging.info("Successfully added flight IDs to PDF %s", pdf_hash)
        except Exception as e:
            msg = f"An error occurred while adding the flight IDs to the PDF: {e}"
            logging.error(msg)
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

        # Create a reference to the document using the SHA-256 hash as the document ID
        doc_ref = self.db.collection(self.pdf_archive_coll).document(pdf_hash)

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

        # Create a reference to the document using the SHA-256 hash as the document ID
        doc_ref = self.db.collection(self.pdf_archive_coll).document(pdf_hash)

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
            # Create a query to search for flights originating from the specified terminal
            query = self.db.collection(self.flight_current_coll).where(
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
            # Create a reference to the document
            self.db.collection(self.flight_current_coll).document(document_id).delete()

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
        collection_ref = self.db.collection(self.textract_jobs_coll)

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
        collection_ref = self.db.collection(self.textract_jobs_coll)

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
        # Create a reference to the document
        doc_ref = self.db.collection(self.terminal_coll).document(terminal_name)

        # Try to retrieve the document
        doc = doc_ref.get()

        # Check if the document exists
        if doc.exists:
            return doc.to_dict()

        msg = f"Terminal {terminal_name} does not exist."
        logging.critical(msg)
        raise Exception(msg)

    def archive_flight(self: "FirestoreClient", flight: Flight) -> None:
        """Archive a flight document.

        Args:
        ----
        flight (Flight): The Flight object to archive.
        """
        try:
            # Convert the Flight object to a dictionary
            doc_dict = flight.to_dict()

            # Set the 'archived' and 'archived_timestamp' attributes
            doc_dict["archived"] = True
            doc_dict["archived_timestamp"] = SERVER_TIMESTAMP

            # Create a reference to the document
            self.db.collection(self.flight_archive_coll).document(flight.flight_id).set(
                doc_dict
            )

            logging.info("Successfully archived flight %s", flight.flight_id)
        except Exception as e:
            msg = f"An error occurred while archiving flight {flight.flight_id}: {e}"
            raise Exception(msg) from e

    def delete_current_flight(self: "FirestoreClient", flight: Flight) -> None:
        """Delete a flight document from the Current_Flights collection.

        Args:
        ----
        flight (Flight): The Flight object to delete.
        """
        try:
            # Create a reference to the document
            self.db.collection(self.flight_current_coll).document(
                flight.flight_id
            ).delete()

            logging.info("Successfully deleted flight %s", flight.flight_id)
        except Exception as e:
            msg = f"An error occurred while deleting flight {flight.flight_id}: {e}"
            raise Exception(msg) from e

    def store_flight_as_current(self: "FirestoreClient", flight: Flight) -> None:
        """Store a flight document in the Current_Flights collection.

        Args:
        ----
        flight (Flight): The Flight object to store.
        """
        try:
            # Convert the Flight object to a dictionary
            doc_dict = flight.to_dict()

            # Create a reference to the document
            self.db.collection(self.flight_current_coll).document(flight.flight_id).set(
                doc_dict
            )

            logging.info("Successfully stored flight %s", flight.flight_id)
        except Exception as e:
            msg = f"An error occurred while storing flight {flight.flight_id}: {e}"
            raise Exception(msg) from e

    def get_doc_by_id(
        self: "FirestoreClient", collection_name: str, doc_id: str
    ) -> Dict[str, Any]:
        """Get a document from a specified collection by its ID.

        Args:
        ----
        collection_name (str): The name of the collection where the document resides.
        doc_id (str): The ID of the document to retrieve.

        Returns:
        -------
        dict: A dictionary representing the retrieved document.
        """
        try:
            # Create a reference to the document
            doc_ref = self.db.collection(collection_name).document(doc_id)

            # Try to retrieve the document
            doc = doc_ref.get()

            # Check if the document exists
            if doc.exists:
                return doc.to_dict()

            msg = f"Document with ID {doc_id} does not exist."
            raise Exception(msg)

        except Exception as e:
            msg = f"An error occurred while retrieving document with ID {doc_id}: {e}"
            raise Exception(msg) from e

    def set_terminal_flights(
        self: "FirestoreClient",
        terminal_name: str,
        pdf_type: str,
        flight_ids: List[str],
    ) -> None:
        """Set the flights for a terminal in the Terminals collection.

        Args:
        ----
        terminal_name (str): The name of the terminal.
        pdf_type (str): The type of PDF document.
        flight_ids (list): A list of flight IDs.
        """
        try:
            # Create a reference to the document
            doc_ref = self.db.collection(self.terminal_coll).document(terminal_name)

            # Set the flights for the terminal
            if pdf_type == "72_HR":
                doc_ref.update({"flights72Hour": flight_ids})
            elif pdf_type == "30_DAY":
                doc_ref.update({"flights30Day": flight_ids})
            elif pdf_type == "ROLLCALL":
                doc_ref.update({"flightsRollcall": flight_ids})
            else:
                msg = f"Invalid PDF type: {pdf_type}"
                raise Exception(msg)

            logging.info(
                "Successfully set %s flights for terminal %s", pdf_type, terminal_name
            )
        except Exception as e:
            msg = f"An error occurred while setting {pdf_type} flights for terminal {terminal_name}: {e}"
            raise Exception(msg) from e

    def set_terminal_update_status(
        self: "FirestoreClient",
        terminal_name: str,
        pdf_type: str,
        status: bool,
    ) -> None:
        """Set the update status for a terminal in the Terminals collection.

        Args:
        ----
        terminal_name (str): The name of the terminal.
        pdf_type (str): The type of PDF document.
        status (bool): The status to set.
        """
        try:
            # Create a reference to the document
            doc_ref = self.db.collection(self.terminal_coll).document(terminal_name)

            # Set the update status for the terminal
            if pdf_type == "72_HR":
                doc_ref.update({"updating72Hour": status})
            elif pdf_type == "30_DAY":
                doc_ref.update({"updating30Day": status})
            elif pdf_type == "ROLLCALL":
                doc_ref.update({"updatingRollcall": status})
            else:
                msg = f"Invalid PDF type: {pdf_type}"
                raise Exception(msg)

            logging.info(
                "Successfully set %s update status for terminal %s",
                pdf_type,
                terminal_name,
            )
        except Exception as e:
            msg = f"An error occurred while setting {pdf_type} update status for terminal {terminal_name}: {e}"
            raise Exception(msg) from e

    def set_terminal_pdf(
        self: "FirestoreClient",
        terminal_name: str,
        pdf_type: str,
        pdf_cloud_path: str,
    ) -> None:
        """Set the PDF for a terminal in the Terminals collection.

        Args:
        ----
        terminal_name (str): The name of the terminal.
        pdf_type (str): The type of PDF document.
        pdf_cloud_path (str): The cloud path of the PDF document.
        """
        try:
            # Create a reference to the document
            doc_ref = self.db.collection(self.terminal_coll).document(terminal_name)

            # Set the PDF for the terminal
            if pdf_type == "72_HR":
                doc_ref.update({"pdf72Hour": pdf_cloud_path})
            elif pdf_type == "30_DAY":
                doc_ref.update({"pdf30Day": pdf_cloud_path})
            elif pdf_type == "ROLLCALL":
                doc_ref.update({"pdfRollcall": pdf_cloud_path})
            else:
                msg = f"Invalid PDF type: {pdf_type}"
                raise Exception(msg)

            logging.info(
                "Successfully set %s PDF for terminal %s", pdf_type, terminal_name
            )
        except Exception as e:
            msg = f"An error occurred while setting {pdf_type} PDF for terminal {terminal_name}: {e}"
            raise Exception(msg) from e

    def delete_collection(
        self: "FirestoreClient", collection_name: str, batch_size: int = 5
    ) -> None:
        """Delete all documents in a Firestore collection.

        Args:
        ----
            collection_name (str): The name of the collection to delete.
            batch_size (int): The size of the batch for each deletion round.
        """
        collection_ref = self.db.collection(collection_name)
        docs = collection_ref.limit(batch_size).stream()
        deleted = 0

        for doc in docs:
            doc.reference.delete()
            deleted += 1

        if deleted >= batch_size:
            return self.delete_collection(collection_name, batch_size)

        logging.info("Deleted all documents from collection '%s'", collection_name)
        return None

    def find_document_with_matching_array(
        self: "FirestoreClient",
        collection_name: str,
        array_field_name: str,
        target_array: List[Any],
    ) -> Optional[Dict[str, Any]]:
        """Find document in a collection with a matching array.

        Args:
        ----
            collection_name (str): The name of the collection to search.
            array_field_name (str): The name of the array field to search.
            target_array (list): The array to search for.

        Returns:
        -------
            Optional[Dict[str, Any]]: A dictionary representing the document if it exists, otherwise None.
        """
        try:
            # Query the Firestore collection
            collection_ref = self.db.collection(collection_name)
            docs = collection_ref.stream()

            for doc in docs:
                doc_data = doc.to_dict()
                # Check if the document has the field and if the field matches the target array
                if (
                    array_field_name in doc_data
                    and doc_data[array_field_name] == target_array
                ):
                    return doc_data

            return None

        except Exception as e:
            # Handle exceptions
            print(f"An error occurred retrieving a document by array: {e}")
            return None
