import json
import boto3
import os
import re
from firestore_db import FirestoreClient
from flight import Flight
import logging
from dotenv import load_dotenv
import doc_analysis_responses
import sns_event_message
from datetime import datetime as dt  # Importing datetime class as dt to avoid naming conflicts

def initialize_clients():
    # Initialize logging
    logging.basicConfig(level=logging.INFO)

    # Check if running in a local environment
    if os.getenv('RUN_LOCAL'):
        logging.info("Running in a local environment.")

        # Set environment variables
        load_dotenv()
        
        # Setup AWS session
        boto3.setup_default_session(
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION')
        )

        # Initialize Textract client
        textract_client = boto3.client('textract')
        
    else:
        logging.info("Running in a cloud environment.")
        
        # Assume the role and environment is already set up in Lambda or EC2 instance, etc.
        textract_client = boto3.client('textract')

    return textract_client

# Initialize Textract client
textract_client = initialize_clients()

# Initialize Firestore client
firestore_client = FirestoreClient()

# Initialize logger and set log level
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Function to get text from related blocks
def get_text_from_related_blocks(related_ids, blocks_map):
    """Extracts and concatenates text from related blocks.
    
    Args:
        related_ids (list): List of IDs that are related.
        blocks_map (dict): Dictionary containing block data.
        
    Returns:
        str: Concatenated text.
    """
    text = ''
    for block_id in related_ids:
        if block_id in blocks_map:
            text += get_text(blocks_map[block_id], blocks_map) + ' '
    return text.strip()

# Function to get rows and columns map from table block
def get_rows_columns_map(table_block, blocks_map):
    """Maps rows, titles, and footers in a table block.
    
    Args:
        table_block (dict): The table block data.
        blocks_map (dict): Dictionary containing block data.
        
    Returns:
        tuple: Rows, titles, and footers.
    """
    rows = []
    titles = []
    footers = []
    # Loop through relationships to identify Child, Table Title, and Table Footer
    for relationship in table_block['Relationships']:
        if relationship['Type'] == 'CHILD':
            for cell_id in relationship['Ids']:
                cell_block = blocks_map[cell_id]
                row_index = cell_block['RowIndex']
                col_index = cell_block['ColumnIndex']
                # Extend rows list to fit this cell
                while len(rows) < row_index:
                    rows.append([])
                # Extend this row to fit this cell
                while len(rows[row_index - 1]) < col_index:
                    rows[row_index - 1].append(None)
                # Insert this cell into the row
                rows[row_index - 1][col_index - 1] = cell_block
        elif relationship['Type'] == 'TABLE_TITLE':
            titles.append(get_text_from_related_blocks(relationship['Ids'], blocks_map))
        elif relationship['Type'] == 'TABLE_FOOTER':
            footers.append(get_text_from_related_blocks(relationship['Ids'], blocks_map))
    return rows, titles, footers

# Function to get text from a cell block
def get_text(cell_block, blocks_map, min_confidence=90):
    """Extracts text from a cell block if it meets the minimum confidence level.
    
    Args:
        cell_block (dict): The cell block data.
        blocks_map (dict): Dictionary containing block data.
        min_confidence (int, optional): Minimum confidence level. Defaults to 90.
        
    Returns:
        str: Extracted text.
    """
    cell_text = ''
    if 'Relationships' in cell_block:
        for relationship in cell_block['Relationships']:
            if relationship['Type'] == 'CHILD':
                for word_id in relationship['Ids']:
                    word_block = blocks_map[word_id]
                    if word_block['BlockType'] == 'WORD':
                        if word_block['Confidence'] >= min_confidence:
                            cell_text += word_block['Text'] + ' '
    return cell_text.strip()

# Function to get date from table title
def get_date_from_title(title):
    """Extracts date from the table title using regex.
    
    Args:
        title (str): The table title.
        
    Returns:
        str: Date in YYYYMMDD format or None if not found.
    """
    date_patterns = [
        r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b \d{1,2} \d{4}"
    ]
    for pattern in date_patterns:
        match = re.search(pattern, title, re.IGNORECASE)
        if match:
            date_str = match.group(0)
            # Convert to YYYYMMDD format
            date_obj = dt.strptime(date_str, '%b %d %Y')
            return date_obj.strftime('%Y%m%d')
    logging.warning("No date found in table title.")
    return None

# Main function to parse Textract response
def parse_textract_response_to_flights(response, min_confidence=90):
    """Parses the AWS Textract response to extract flight information.
    
    Args:
        response (dict): The Textract response.
        min_confidence (int, optional): Minimum confidence level. Defaults to 90.
        
    Returns:
        list: List of Flight objects.
    """
    logging.info("Parsing Textract response.")
    
    # Define regex filters for columns
    destination_filters = [r"destination"]
    rollcall_filters = [r"roll call"]
    seats_filters = [r"seats"]
    notes_filters = [r"notes"]
    
    flights = []
    blocks_map = {}
    
    # Create a map of block IDs to blocks
    for block in response['Blocks']:
        blocks_map[block['Id']] = block
    
    # Filter out table blocks from the response
    table_blocks = [block for block in response['Blocks'] if block['BlockType'] == 'TABLE']
    
    # Loop through each table to extract flight information
    for index, table in enumerate(table_blocks):
        if table['Confidence'] < min_confidence:
            logging.warning(f"Skipping table {index} due to low confidence ({table['Confidence']}).")
            continue
        
        rows, titles, footers = get_rows_columns_map(table, blocks_map)
        
        # Extract date from table title
        table_title = ' '.join(titles) if titles else 'N/A'
        logging.debug(f"Table Title: {table_title}")
        date = get_date_from_title(table_title)
        
        # Validate the first row to see if it contains headers
        first_row_texts = [get_text(cell, blocks_map).lower() for cell in rows[0]]
        if not any(re.search(pattern, text, re.IGNORECASE) for text in first_row_texts for pattern in destination_filters):
            logging.warning("Invalid headers in the first row. Skipping.")
            logging.debug(f"Headers: {first_row_texts}")
            rows = rows[1:]
        
        # Identify columns using regex filters
        header = rows[0]
        destination_col = rollcall_col = seats_col = notes_col = -1
        for i, cell in enumerate(header):
            cell_text = get_text(cell, blocks_map).lower()
            if any(re.search(pattern, cell_text, re.IGNORECASE) for pattern in destination_filters):
                destination_col = i
            elif any(re.search(pattern, cell_text, re.IGNORECASE) for pattern in rollcall_filters):
                rollcall_col = i
            elif any(re.search(pattern, cell_text, re.IGNORECASE) for pattern in seats_filters):
                seats_col = i
            elif any(re.search(pattern, cell_text, re.IGNORECASE) for pattern in notes_filters):
                notes_col = i
        
        # Create Flight objects and append to the list
        for row in rows[1:]:
            logging.debug(f"Row Data: {[get_text(cell, blocks_map) for cell in row]}")
            
            # Extract individual cell data
            destination = get_text(row[destination_col], blocks_map) if destination_col != -1 else ""
            rollcall_time = get_text(row[rollcall_col], blocks_map) if rollcall_col != -1 else ""
            seats_text = get_text(row[seats_col], blocks_map) if seats_col != -1 else "TBD"
            if seats_text == "TBD":
                num_of_seats = 0
                seat_status = "TBD"
            else:
                num_of_seats, seat_status = re.match(r"(\d+)([TF])", seats_text).groups()
            notes = get_text(row[notes_col], blocks_map) if notes_col != -1 else ""
            table_footer = ' '.join(footers) if footers else 'N/A'
            
            # Create and append Flight object
            flight = Flight("", destination, rollcall_time, int(num_of_seats), seat_status, notes, date, table_footer)
            flights.append(flight)
        
    logging.info("Parsing complete.")
    return flights

def lambda_handler(event, context):
    message_json_str = event['Records'][0]['Sns']['Message']
    message_dict = json.loads(message_json_str)
    
    job_id = message_dict.get('JobId', '')
    status = message_dict.get('Status', '')
    
    if not job_id or not status:
        logging.error("JobId or Status missing in SNS message.")
        return

    # Update the job status in Firestore
    firestore_client.update_job_status(job_id, status)

    # Get flight origin from Firestore
    pdf_hash = firestore_client.get_textract_job(job_id).get('pdf_hash', None)

    # If pdf_hash is not None we can retrieve
    # origin information from Firestore
    if pdf_hash:
        flight_origin = firestore_client.get_flight_origin_by_pdf_hash(pdf_hash)

        if not flight_origin:
            logging.error(f"Flight origin not found for PDF hash: {pdf_hash}")
            flight_origin = "N/A"
    else:
        logging.error(f"PDF hash not found for job ID: {job_id}")
        flight_origin = "N/A"

    # If the job succeeded, parse the Textract response
    if status == 'SUCCEEDED':
        response = doc_analysis_responses.bwi_1_textract_response # textract_client.get_document_analysis(JobId=job_id)
        flights = parse_textract_response_to_flights(response)

        # Insert each flight into Firestore
        for flight in flights:
            # Set the origin for each flight
            if flight.origin == "N/A":
                flight.origin = flight_origin
            
            firestore_client.insert_flight(flight)

    return {
        'statusCode': 200,
        'body': json.dumps('Lambda function executed successfully!')
    }

if __name__ == "__main__":
    lambda_handler(sns_event_message.sns_event_message, None)
