import logging
import os
from typing import Optional

import boto3  # type: ignore
from boto3.exceptions import S3UploadFailedError  # type: ignore
from botocore.exceptions import NoCredentialsError, ParamValidationError  # type: ignore

from package.botocore.exceptions import ClientError  # type: ignore


class S3Bucket:
    """A class representing an S3 bucket.

    Attributes
    ----------
        client (boto3.client): An S3 client object.
        bucket_name (str): The name of the S3 bucket.
    """

    def __init__(self: "S3Bucket", bucket_name: Optional[str] = None) -> None:
        """Initialize an S3Bucket object.

        Args:
        ----
        bucket_name : str, optional
            The name of the S3 bucket to use. If not provided, the value of the
            AWS_BUCKET_NAME environment variable will be used.

        Raises:
        ------
        EnvironmentError
            If the AWS configuration or bucket name is missing from the environment
            variables.
        """
        # Check if running in a local environment
        run_local = os.getenv("RUN_LOCAL")

        if run_local:
            logging.info("Running in a local environment.")

            # Get keys from environment variables
            env_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
            env_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

            # Check environment variables
            if not env_access_key_id or not env_secret_access_key:
                logging.error("AWS configuration missing in environment variables!")
                msg = "Missing AWS configuration in environment variables."
                raise EnvironmentError(msg)

            # Initialize S3 client with credentials
            self.client = boto3.client(
                "s3",
                aws_access_key_id=env_access_key_id,
                aws_secret_access_key=env_secret_access_key,
            )
        else:
            logging.info("Running in a cloud environment.")

            # Initialize S3 client using IAM role
            self.client = boto3.client("s3")

        # Use provided bucket name or fall back to environment variable
        self.bucket_name = (
            bucket_name if bucket_name else os.environ.get("AWS_BUCKET_NAME")
        )

        if not self.bucket_name:
            error_msg = "AWS bucket name missing!"
            logging.error(error_msg)
            raise EnvironmentError(error_msg)

    def download_from_s3(self: "S3Bucket", s3_path: str, local_path: str) -> None:
        """Download a file from S3 to a local path.

        Args:
        ----
        s3_path : str
            The S3 path of the file to download.
        local_path : str
            The local path to download the file to.

        Returns:
        -------
        None
        """
        try:
            self.client.download_file(self.bucket_name, s3_path, local_path)
            logging.info("Downloaded %s to %s", s3_path, local_path)
        except S3UploadFailedError as e:
            logging.error("Upload failed for %s to %s: %s", s3_path, local_path, e)
        except ParamValidationError as e:
            logging.error(
                "Parameter validation failed for %s to %s: %s", s3_path, local_path, e
            )
        except Exception as e:
            logging.error(
                "An unexpected error occurred while downloading %s to %s: %s",
                s3_path,
                local_path,
                e,
            )

    def upload_to_s3(self: "S3Bucket", local_path: str, s3_path: str) -> None:
        """Upload a file to S3 from a local path.

        Args:
        ----
        local_path : str
            The local path of the file to upload.
        s3_path : str
            The S3 path to upload the file to.

        Returns:
        -------
        None
        """
        try:
            self.client.upload_file(local_path, self.bucket_name, s3_path)
            logging.info("Uploaded %s to %s", local_path, s3_path)
        except S3UploadFailedError as e:
            logging.error("Upload failed for %s to %s: %s", local_path, s3_path, e)
        except NoCredentialsError as e:
            logging.error(
                "No credentials found for %s to %s: %s", local_path, s3_path, e
            )
        except ParamValidationError as e:
            logging.error(
                "Parameter validation failed for %s to %s: %s", local_path, s3_path, e
            )
        except Exception as e:
            logging.error(
                "An unexpected error occurred while uploading %s to %s: %s",
                local_path,
                s3_path,
                e,
            )

    def file_exists(self: "S3Bucket", s3_path: str) -> bool:
        """Check if a file exists in the S3 bucket.

        Args:
        ----
        s3_path : str
            The S3 path of the file to check.

        Returns:
        -------
        bool
            True if the file exists, False otherwise.
        """
        try:
            self.client.head_object(Bucket=self.bucket_name, Key=s3_path)
            return True
        except ClientError as e:
            # The error code will be a 404 if the object does not exist
            if e.response["Error"]["Code"] == "404":
                return False

            logging.error("Error checking if file exists in S3: %s", e)
            raise
