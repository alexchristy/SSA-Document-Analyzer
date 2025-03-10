import json
import logging
import os
import uuid
import shutil
from typing import Any, Dict, List, Tuple

import boto3  # type: ignore
import sentry_sdk
from aws_lambda_typing import context as lambda_context
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration

from aws_utils import initialize_client
from firestore_db import FirestoreClient
from parse_sns import parse_sns_event
from s3_bucket import S3Bucket
from screenshot_table import capture_screen_shot_of_table_from_pdf
from table import Table
from table_utils import gen_tables_from_textract_response

# Set up sentry
sentry_sdk.init(
    dsn="https://5cd0afbfc9ad23474f63e76f5dc199c0@o4506224652713984.ingest.sentry.io/4506224655597568",
    integrations=[AwsLambdaIntegration(timeout_warning=True)],
    # Set traces_sample_rate to 1.0 to capture 100%
    # of transactions for performance monitoring.
    traces_sample_rate=1.0,
    # Set profiles_sample_rate to 1.0 to profile 100%
    # of sampled transactions.
    # We recommend adjusting this value in production.
    profiles_sample_rate=1.0,
)

textract_client = initialize_client("textract")
lambda_client = initialize_client("lambda")


def get_lowest_confidence_row(table: Table) -> Tuple[int, float]:
    """Get the row index and confidence score of the row with the lowest confidence in a table.

    Args:
    ----
        table (custom Table class): The table to search.

    Returns:
    -------
        tuple: Tuple containing the row index and confidence score.
    """
    lowest_confidence = 100.0
    lowest_confidence_row_index = 0

    for index, _row in enumerate(table.rows):
        # Ignore first row (column headers)
        if index == 0:
            continue

        row_confidence = table.get_average_row_confidence(
            row_index=index, ignore_empty_cells=True
        )

        invalid_confidence = -1.0
        if row_confidence < invalid_confidence:
            logging.warning(
                "Failed to get average row confidence for row index %s.", index
            )
            continue

        if row_confidence < lowest_confidence:
            lowest_confidence = row_confidence
            lowest_confidence_row_index = index

    return lowest_confidence_row_index, lowest_confidence


def get_document_analysis_results(
    client: boto3.client, job_id: str
) -> List[Dict[str, Any]]:
    """Get the results of a Textract document analysis job.

    Args:
    ----
        client (boto3 Textract client): The Textract client.
        job_id (str): The ID of the Textract job.

    Returns:
    -------
        list: List of dictionaries containing the results of the Textract job.
    """
    # Initialize variables
    results = []
    next_token = None

    try:
        while True:
            # Handle paginated responses
            if next_token:
                response = client.get_document_analysis(
                    JobId=job_id, NextToken=next_token
                )
            else:
                response = client.get_document_analysis(JobId=job_id)

            # Append results to list
            results.extend(response["Blocks"])

            # Check if there are more pages of results
            next_token = response.get("NextToken", None)
            if not next_token:
                break

    except Exception as e:
        logging.error("Error getting document analysis results: %s", e)
        return []

    return results


def reprocess_tables(
    tables: List[Table],
    s3_client: S3Bucket,
    s3_object_path: str,
    response: List[Dict[str, Any]],
) -> List[Table]:
    """Reprocess tables with low confidence scores in a Textract document analysis job.

    Args:
    ----
        tables (List[Table]): The tables to reprocess.
        s3_client (str): The name of the S3 client.
        s3_object_path (str): The path to the S3 object.
        response (dict): The response from the Textract job.

    Returns:
    -------
        None
    """
    # Check if any tables need to be reprocessed
    if not tables:
        logging.info("No tables need to be reprocessed.")
        return []

    download_dir = os.getenv("DOWNLOAD_DIR")
    if not download_dir:
        msg = "Download directory is not set."
        raise EnvironmentError(msg)

    # Create local path to download PDF to
    unique_dir_name = str(uuid.uuid4())
    download_dir = os.path.join(download_dir, unique_dir_name)

    # Check if download directory exists
    if not os.path.isdir(download_dir):
        # Create download directory
        try:
            os.makedirs(download_dir, exist_ok=True)
        except Exception as e:
            msg = "Failed to create download directory."
            raise ValueError(msg) from e

    # Get PDF filename from S3 object path
    pdf_filename = os.path.basename(s3_object_path)

    # Create local path to PDF
    local_pdf_path = os.path.join(download_dir, pdf_filename)

    # Download PDF from S3
    try:
        s3_client.download_from_s3(s3_object_path, local_pdf_path)
    except Exception as e:
        msg = "Failed to download PDF from S3"
        raise ValueError(msg) from e

    # Store reporcessed tables
    reprocessed_tables_list = []

    # Reprocess tables with low confidence rows
    logging.info("Reprocessing %s tables.", len(tables))
    for i, table in enumerate(tables):
        logging.info("Reprocessing table with page number: %s", table.page_number)

        # Get table page number
        page_number = table.page_number

        table_screen_shot_with_title = capture_screen_shot_of_table_from_pdf(
            pdf_path=local_pdf_path,
            page_number=page_number,
            textract_response=response,
            output_folder=download_dir,
            padding=75,
            include_title=True,
        )

        if not table_screen_shot_with_title:
            logging.warning(
                "Failed to capture screen shot of table %d in s3 at: %s.",
                i,
                s3_object_path,
            )
            continue

        # Read the local PDF file
        with open(table_screen_shot_with_title, "rb") as file:
            file_bytes = bytearray(file.read())

        # Send to screen shot to Textract for reprocessing
        # Call AnalyzeDocument API
        reprocess_response = textract_client.analyze_document(
            Document={"Bytes": file_bytes}, FeatureTypes=["TABLES"]
        )

        # Parse the response
        new_tables = gen_tables_from_textract_response(reprocess_response)

        # Remove the screen shot of the table
        os.remove(table_screen_shot_with_title)

        # Check if more than one table was found
        if len(new_tables) > 1:
            msg = "More than one table found in reprocessed table."
            raise Exception(msg)

        # Get the only table in the list if there is a table
        # found. If not, set reprocessed_table to None and
        # continue to the next table.
        if new_tables:
            reprocessed_table = new_tables[0]
        else:
            logging.warning("No tables found in reprocessed table.")
            continue

        # Get the lowest confidence row of the reproccessed table
        _, lowest_confidence_row_reproccessed = get_lowest_confidence_row(
            reprocessed_table
        )

        # Compare the lowest confidence row to the original table
        # If the confidence is higher, replace the original table with the reprocessed table
        if lowest_confidence_row_reproccessed > get_lowest_confidence_row(table)[1]:
            logging.info(
                "Reprocessed table has higher confidence than original table. Replacing original table with reprocessed table."
            )
            reprocessed_tables_list.append(reprocessed_table)
        else:
            logging.info(
                "Reprocessed table has lower confidence than original table. Keeping original table."
            )
            reprocessed_tables_list.append(table)

    # Remove PDF from local directory
    os.remove(local_pdf_path)

    # Remove the local directory
    shutil.rmtree(download_dir)

    return reprocessed_tables_list


# Initialize logger and set log level
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event: dict, context: lambda_context.Context) -> Dict[str, Any]:
    """Convert textract response to tables.

    Args:
    ----
        event (dict): The event object passed by AWS Lambda.
        context (dict): The context object passed by AWS Lambda.

    Returns:
    -------
        dict: A dictionary containing the status code and response body.
    """
    # Initialize Firestore client
    firestore_client = FirestoreClient(textract_jobs_coll="Textract_Jobs")
    try:
        # Parse the SNS message
        job_id, status, s3_object_path, s3_bucket_name = parse_sns_event(event)

        if not job_id or not status:
            no_job_status_msg = "JobId or Status missing in SNS message."
            raise ValueError(no_job_status_msg)

        if not s3_bucket_name:
            response_msg = (
                "S3 object path missing in SNS message. JobId: {}, Status: {}".format(
                    job_id, status
                )
            )
            raise ValueError(response_msg)

        if not s3_object_path:
            response_msg = (
                "S3 object path missing in SNS message. JobId: {}, Status: {}".format(
                    job_id, status
                )
            )
            raise ValueError(response_msg)

        # If job failed exit program
        if status != "SUCCEEDED":
            msg = "Job did not succeed."
            raise Exception(msg)

        request_id = context.aws_request_id
        function_name = context.function_name

        func_72hr_info = {
            "func_textract_to_tables_request_id": request_id,
            "func_textract_to_tables_name": function_name,
        }

        # Append function info to Textract Job
        firestore_client.append_to_doc("Textract_Jobs", job_id, func_72hr_info)

        # Append null values to timestamp fields in Textract Job.
        # This allows us to query for jobs that failed to process completely.
        null_timestamps = {
            "tables_parsed_started": None,
            "tables_parsed_finished": None,
        }
        firestore_client.append_to_doc(
            "Textract_Jobs",
            job_id,
            null_timestamps,
        )

        # Set testing values
        textract_doc = firestore_client.get_textract_job(job_id)

        test = False
        if textract_doc:
            test = textract_doc.get("test", False)

            if test:
                logging.info("Using test values.")
                test_params: Dict[str, Any] = textract_doc.get("testParameters", {})

                if not test_params:
                    msg = "Test parameters not found"
                    raise Exception(msg)

                # Set testing terminal and pdf collections
                if "testPdfArchiveColl" in test_params:
                    pdf_archive_coll = test_params["testPdfArchiveColl"]

                    if isinstance(pdf_archive_coll, str):
                        firestore_client.set_pdf_archive_coll(pdf_archive_coll)
                else:
                    logging.warning(
                        "Failed to get test pdf archive collection. Using default collection."
                    )

                if "testTerminalColl" in test_params:
                    terminal_coll = test_params["testTerminalColl"]

                    if isinstance(terminal_coll, str):
                        firestore_client.set_terminal_coll(terminal_coll)
                else:
                    logging.warning(
                        "Failed to get test terminal collection. Using default collection."
                    )
        else:
            logging.error(
                "Failed to get textract job document when checking for testing values."
            )

        # Get extra environment variables
        min_confidence = int(os.getenv("MIN_CONFIDENCE", "80"))
        lambda_72hr_flight = os.getenv("LAMBDA_72HR_FLIGHT", "Process-72hr-Flights")
        lambda_30day_flight = os.getenv("LAMBDA_30DAY_FLIGHT", "Process-30day-Flights")
        lambda_rollcall = os.getenv("LAMBDA_ROLLCALL", "Process-Rollcall")

        if min_confidence == 80:  # noqa: PLR2004 (Already a constant)
            logging.info("Using default minimum confidence of 80.")

        if lambda_72hr_flight == "Process-72hr-Flights":
            logging.info("Using default lambda for 72hr flights: Process-72hr-Flights.")

        if lambda_30day_flight == "Process-30day-Flights":
            logging.info(
                "Using default lambda for 30day flights: Process-30day-Flights."
            )

        if lambda_rollcall == "Process-Rollcall":
            logging.info("Using default lambda for rollcall: Process-Rollcall.")

        # Initialize S3 client
        s3_client = S3Bucket(bucket_name=s3_bucket_name)

        # Update the job status in Firestore
        firestore_client.update_job_status(job_id, status)
        firestore_client.add_job_timestamp(job_id, "textract_finished")
        firestore_client.add_job_timestamp(job_id, "tables_parsed_started")

        # Get the PDF hash from Firestore
        pdf_hash = firestore_client.get_pdf_hash_with_s3_path(s3_object_path)

        if not pdf_hash:
            no_pdf_hash_msg = f"Failed to get PDF hash using s3 object path ({s3_object_path}) from Firestore."
            raise ValueError(no_pdf_hash_msg)

        # Get the origin terminal from Firestore
        origin_terminal = firestore_client.get_terminal_name_by_pdf_hash(pdf_hash)

        if not origin_terminal:
            no_origin_terminal_msg = f"Failed to get origin terminal using PDF hash ({pdf_hash}) from Firestore."
            raise ValueError(no_origin_terminal_msg)

        # Existing tables
        response = get_document_analysis_results(textract_client, job_id)
        tables = gen_tables_from_textract_response(response)

        if not tables:
            no_tables_msg = "No tables found in Textract response."
            raise ValueError(no_tables_msg)

        # Lists to hold tables
        tables_to_reprocess = []
        tables_to_keep = []

        # Iterate through tables to find low confidence rows
        for table in tables:
            _, lowest_confidence = get_lowest_confidence_row(table)
            if lowest_confidence < min_confidence:
                tables_to_reprocess.append(table)
            else:
                tables_to_keep.append(table)

        if tables_to_reprocess:
            # Reprocess tables with low confidence rows
            reprocessed_tables = reprocess_tables(
                tables=tables_to_reprocess,
                s3_client=s3_client,
                s3_object_path=s3_object_path,
                response=response,
            )
            # Remove the old versions of reprocessed tables and add the new versions
            tables = tables_to_keep + reprocessed_tables

        pdf_type = firestore_client.get_pdf_type_by_hash(pdf_hash)

        if not pdf_type:
            no_pdf_type_msg = "Failed to get PDF type from Firestore."
            raise ValueError(no_pdf_type_msg)

        if pdf_type == "72_HR":
            func_name = lambda_72hr_flight
        elif pdf_type == "30_DAY":
            func_name = lambda_30day_flight
        elif pdf_type == "ROLLCALL":
            func_name = lambda_rollcall
        else:
            invalid_pdf_type_msg = f"Invalid PDF type: {pdf_type}"
            raise ValueError(invalid_pdf_type_msg)

        # Serialize tables to dictionaries
        serialized_tables = [table.to_dict() for table in tables]

        payload = json.dumps(
            {
                "tables": serialized_tables,
                "pdf_hash": pdf_hash,
                "job_id": job_id,
            }
        )

        firestore_client.add_job_timestamp(job_id, "tables_parsed_finished")

        logging.info("Parsed %d tables.", len(tables))
        logging.info("Invoking lambda: %s", func_name)

        # Invoke lambda to process tables
        lambda_client.invoke(
            FunctionName=func_name,
            InvocationType="Event",
            Payload=payload,
        )

        return {
            "statusCode": 200,
            "body": "Successfully parsed textract to tables.",
            "payload": payload,
        }

    except Exception as e:
        logger.critical("Error occurred: %s", str(e))
        raise e
