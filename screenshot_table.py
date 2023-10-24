import logging
import os
from typing import Any, Dict

from pdf2image import convert_from_path

# Initialize logging
logging.basicConfig(level=logging.INFO)


def capture_screen_shot_of_table_from_pdf(
    pdf_path: str,
    textract_response: Dict[str, Any],
    page_number: int,
    **kwargs: Any,  # noqa: ANN401 (Ignored to allow using **kwargs)
) -> str:
    """Capture tables from a PDF based on AWS Textract response.

    Args:
    ----
        pdf_path (str): Path to the PDF file.
        textract_response (dict): Response from AWS Textract GetDocumentAnalysis.
        page_number (int): Page number to capture the table from.
        kwargs: Keyword arguments to pass to the function.

    Keyword Args:
    ------------
        output_folder (str, optional): Folder to save the screenshot. Default is current directory.
        padding (int, optional): Padding around the table in pixels. Default is 50.
        include_title (bool, optional): Whether to include table title in the screenshot. Default is True.

    Returns:
    -------
        str: Path to the last screenshot taken.
    """
    output_folder = kwargs.get("output_folder", ".")
    padding = kwargs.get("padding", 50)
    include_title = kwargs.get("include_title", True)
    try:
        # Create output folder if it doesn't exist
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
            logging.info("Created output folder: %s", output_folder)

        # Convert PDF to images
        images = convert_from_path(pdf_path)
    except Exception as e:
        logging.error("An error occurred while reading the PDF: %s", e)
        return ""

    # Validate page number
    if page_number > len(images) or page_number < 1:
        logging.error(
            "Invalid page number: %s. Total pages: %s", page_number, len(images)
        )
        return ""

    # Get the image corresponding to the specified page
    img = images[page_number - 1]

    # Initialize table counter and last screenshot path
    table_count = 0
    last_screenshot_path = ""

    # Check if the input is paginated (list of blocks) or not (single page JSON response)
    if isinstance(textract_response, list):
        blocks_to_process = textract_response
    else:
        blocks_to_process = textract_response.get("Blocks", [])

    for block in blocks_to_process:
        if block["BlockType"] == "TABLE" and block["Page"] == page_number:
            # Increment table counter
            table_count += 1

            # Initialize bounding box coordinates
            min_left = float("inf")
            min_top = float("inf")
            max_right = float("-inf")
            max_bottom = float("-inf")

            # Extract bounding box coordinates for the table
            bb = block["Geometry"]["BoundingBox"]
            min_left = min(min_left, bb["Left"])
            min_top = min(min_top, bb["Top"])
            max_right = max(max_right, bb["Left"] + bb["Width"])
            max_bottom = max(max_bottom, bb["Top"] + bb["Height"])

            # Update bounding box to include table title and footer if needed
            for related_block in blocks_to_process:
                if (
                    related_block["BlockType"] in ["TABLE_TITLE", "TABLE_FOOTER"]
                    and related_block["Page"] == page_number
                    and (include_title or related_block["BlockType"] != "TABLE_TITLE")
                ):
                    bb = related_block["Geometry"]["BoundingBox"]
                    min_left = min(min_left, bb["Left"])
                    min_top = min(min_top, bb["Top"])
                    max_right = max(max_right, bb["Left"] + bb["Width"])
                    max_bottom = max(max_bottom, bb["Top"] + bb["Height"])

            # Calculate coordinates for cropping, ensuring they are within bounds
            left = max(min_left * img.width - padding, 0)
            top = max(min_top * img.height - padding, 0)
            right = min(max_right * img.width + padding, img.width)
            bottom = min(max_bottom * img.height + padding, img.height)

            # Crop the image based on calculated coordinates
            cropped_img = img.crop((left, top, right, bottom))

            # Save the cropped image
            last_screenshot_path = os.path.join(
                output_folder, f"table_page_{page_number}_table_{table_count}.png"
            )
            cropped_img.save(last_screenshot_path)
            logging.info("Saved table screenshot: %s", last_screenshot_path)

    # Report if no tables were found on the page
    if table_count <= 0:
        logging.warning("No tables found on page %s", page_number)

    return last_screenshot_path
