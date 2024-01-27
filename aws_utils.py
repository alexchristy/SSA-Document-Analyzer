import logging
import os
from dotenv import load_dotenv

import boto3  # type: ignore
from botocore.exceptions import (  # type: ignore
    NoCredentialsError,
    PartialCredentialsError,
    UnknownServiceError,
)


def initialize_client(requested_client: str) -> boto3.client:
    """Initialize an AWS client.

    Parameters
    ----------
    requested_client : str
        The name of the AWS service client to be initialized.

    Returns
    -------
    boto3.client
        The initialized AWS service client.

    Raises
    ------
    ValueError
        If no client is requested or AWS credentials/region are missing in a local environment.
    UnknownServiceError
        If an invalid AWS service client name is provided.
    NoCredentialsError
        If no AWS credentials are found.
    PartialCredentialsError
        If incomplete AWS credentials are provided.
    """
    # Initialize logging
    logging.basicConfig(level=logging.INFO)

    if not requested_client:
        msg = "No client requested."
        raise ValueError(msg)

    try:
        if os.getenv("RUN_LOCAL"):
            logging.info("Running in a local environment.")
            load_dotenv()

            aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
            aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
            aws_region = os.getenv("AWS_REGION")

            if not all([aws_access_key, aws_secret_key, aws_region]):
                msg = "Missing AWS credentials or region for local environment."
                logging.error(msg)
                raise ValueError(msg)

            boto3.setup_default_session(
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                region_name=aws_region,
            )
        else:
            logging.info("Running in a cloud environment.")

        return boto3.client(requested_client)

    except UnknownServiceError as e:
        msg = f"Invalid AWS client '{requested_client}': {e}"
        logging.error(msg)
        raise UnknownServiceError({"error_code": "UnknownService"}, msg) from e
    except (NoCredentialsError, PartialCredentialsError) as e:
        msg = f"Incorrect AWS credentials: {e}"
        logging.error(msg)
        raise ValueError(msg) from e
    except Exception as e:
        msg = f"Failed to initialize AWS client {requested_client}: {e}"
        logging.error(msg)
        raise ValueError(msg) from e
