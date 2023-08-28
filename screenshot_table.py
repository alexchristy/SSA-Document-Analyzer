import os
import logging
from pdf2image import convert_from_path
from PIL import Image
from tests.doc_analysis_responses import bwi_1_textract_response

# Initialize logging
logging.basicConfig(level=logging.INFO)

def capture_table_from_pdf(pdf_path, textract_response, page_number, output_folder='.', padding=50, include_title=True):
    """
    Capture tables from a PDF based on AWS Textract response.

    Parameters:
        pdf_path (str): Path to the PDF file.
        textract_response (dict): Response from AWS Textract GetDocumentAnalysis.
        page_number (int): Page number to capture the table from.
        output_folder (str): Folder to save the screenshot. Default is current directory.
        padding (int): Padding around the table in pixels. Default is 50.
        include_title (bool): Whether to include table title in the screenshot. Default is True.

    Returns:
        str: Path to the last screenshot taken.
    """
    try:
        # Create output folder if it doesn't exist
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
            logging.info(f"Created output folder: {output_folder}")

        # Convert PDF to images
        images = convert_from_path(pdf_path)
    except Exception as e:
        logging.error(f"An error occurred while reading the PDF: {e}")
        return None

    # Validate page number
    if page_number > len(images) or page_number < 1:
        logging.error(f"Invalid page number: {page_number}. Total pages: {len(images)}")
        return None

    # Get the image corresponding to the specified page
    img = images[page_number - 1]

    # Initialize table counter and last screenshot path
    table_count = 0
    last_screenshot_path = None

    # Loop through blocks in the Textract response to find tables
    for block in textract_response.get('Blocks', []):
        if block['BlockType'] == 'TABLE' and block['Page'] == page_number:
            # Increment table counter
            table_count += 1

            # Initialize bounding box coordinates
            min_left = float('inf')
            min_top = float('inf')
            max_right = float('-inf')
            max_bottom = float('-inf')

            # Extract bounding box coordinates for the table
            bb = block['Geometry']['BoundingBox']
            min_left = min(min_left, bb['Left'])
            min_top = min(min_top, bb['Top'])
            max_right = max(max_right, bb['Left'] + bb['Width'])
            max_bottom = max(max_bottom, bb['Top'] + bb['Height'])

            # Update bounding box to include table title and footer if needed
            for related_block in textract_response.get('Blocks', []):
                if related_block['BlockType'] in ['TABLE_TITLE', 'TABLE_FOOTER'] and related_block['Page'] == page_number:
                    if include_title or related_block['BlockType'] != 'TABLE_TITLE':
                        bb = related_block['Geometry']['BoundingBox']
                        min_left = min(min_left, bb['Left'])
                        min_top = min(min_top, bb['Top'])
                        max_right = max(max_right, bb['Left'] + bb['Width'])
                        max_bottom = max(max_bottom, bb['Top'] + bb['Height'])

            # Calculate coordinates for cropping, ensuring they are within bounds
            left = max(min_left * img.width - padding, 0)
            top = max(min_top * img.height - padding, 0)
            right = min(max_right * img.width + padding, img.width)
            bottom = min(max_bottom * img.height + padding, img.height)

            # Crop the image based on calculated coordinates
            cropped_img = img.crop((left, top, right, bottom))

            # Save the cropped image
            last_screenshot_path = os.path.join(output_folder, f"table_page_{page_number}_table_{table_count}.png")
            cropped_img.save(last_screenshot_path)
            logging.info(f"Saved table screenshot: {last_screenshot_path}")

    # Report if no tables were found on the page
    if table_count <= 0:
        logging.warning(f"No tables found on page {page_number}")

    return last_screenshot_path