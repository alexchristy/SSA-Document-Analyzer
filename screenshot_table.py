from pdf2image import convert_from_path
from PIL import Image
from tests.doc_analysis_responses import norfolk_1_textract_response
import os


def capture_table_from_pdf(pdf_path, textract_response, page_number, output_folder='.', padding=75, include_title=True):
    # Create output folder if it doesn't exist
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    # Convert PDF to images
    images = convert_from_path(pdf_path)
    
    # Get the image corresponding to the page where the table is located
    img = images[page_number - 1]
    
    # Initialize table counter
    table_count = 0
    last_screenshot_path = None
    
    # Loop through blocks in the Textract response
    for block in textract_response['Blocks']:
        if block['BlockType'] == 'TABLE' and block['Page'] == page_number:
            # Increment table counter
            table_count += 1
            
            # Initialize bounding box coordinates
            min_left = float('inf')
            min_top = float('inf')
            max_right = float('-inf')
            max_bottom = float('-inf')
            
            # Extract the bounding box coordinates for the table
            bb = block['Geometry']['BoundingBox']
            min_left = min(min_left, bb['Left'])
            min_top = min(min_top, bb['Top'])
            max_right = max(max_right, bb['Left'] + bb['Width'])
            max_bottom = max(max_bottom, bb['Top'] + bb['Height'])
            
            # Look for table title and footer blocks and update bounding box
            for related_block in textract_response['Blocks']:
                if related_block['BlockType'] in ['TABLE_TITLE', 'TABLE_FOOTER'] and related_block['Page'] == page_number:
                    if include_title or related_block['BlockType'] != 'TABLE_TITLE':
                        bb = related_block['Geometry']['BoundingBox']
                        min_left = min(min_left, bb['Left'])
                        min_top = min(min_top, bb['Top'])
                        max_right = max(max_right, bb['Left'] + bb['Width'])
                        max_bottom = max(max_bottom, bb['Top'] + bb['Height'])
            
            # Calculate the coordinates for cropping, ensuring they are within bounds
            left = max(min_left * img.width - padding, 0)
            top = max(min_top * img.height - padding, 0)
            right = min(max_right * img.width + padding, img.width)
            bottom = min(max_bottom * img.height + padding, img.height)
            
            # Crop the image
            cropped_img = img.crop((left, top, right, bottom))
            
            # Save the cropped image
            last_screenshot_path = os.path.join(output_folder, f"table_page_{page_number}_table_{table_count}.png")
            cropped_img.save(last_screenshot_path)
    
    return last_screenshot_path

if __name__ == "__main__":
    path = capture_table_from_pdf(pdf_path="tests/test-pdfs/bwi_1.pdf", textract_response=norfolk_1_textract_response, page_number=2, include_title=False)
    print(path)