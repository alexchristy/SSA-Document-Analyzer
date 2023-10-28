import json
import logging
from typing import Any, Dict, Tuple


def parse_sns_event(event: Dict[str, Any]) -> Tuple[str, str, str, str]:
    """Parse the SNS event from Textract.

    Args:
    ----
        event (dict): The SNS event.

    Returns:
    -------
        tuple: Tuple containing JobId, Status, S3 Object Name, and S3 Bucket Name.
    """
    try:
        message_json_str = (
            event.get("Records", [{}])[0].get("Sns", {}).get("Message", "{}")
        )
    except IndexError:
        logging.error("Malformed SNS event: Records array is empty.")
        return "", "", "", ""

    try:
        message_dict = json.loads(message_json_str)
    except json.JSONDecodeError:
        logging.error("Failed to decode SNS message.")
        return "", "", "", ""

    job_id = message_dict.get("JobId", "")
    status = message_dict.get("Status", "")

    # Extract S3 Object Name and Bucket Name
    s3_object_name = message_dict.get("DocumentLocation", {}).get("S3ObjectName", "")
    s3_bucket_name = message_dict.get("DocumentLocation", {}).get("S3Bucket", "")

    return job_id, status, s3_object_name, s3_bucket_name
