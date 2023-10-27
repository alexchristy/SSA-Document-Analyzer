# SmartSpaceA Document Analyzer

This project contains the code and tests for the four backend AWS Lambda functions that process PDF flight schedules.

## Functions

### Start_PDF_Textract_Job

This Lambda function is the one that is triggered by adding new PDFs into the current folder of the S3 bucket that stores PDFs scraped by the Update Checker.

**Details:**
- Lambda Handler: start_pdf_textract_tables_job.lambda_handler
- Deployment: [Zip Archive Method](#deploy-to-aws-lambda)
- Dependencies: `/dependencies/start_job_requirements.txt`
- Enviroment Variables: (To be added)
- Note: N/A

### Textract_to_Tables (To be split from /recieve_pdf_data_textract.py)

This Lambda function is responsible for recieving the data from textract about the PDF processed. It takes in a SNS message and then processes the Textract response table objects. These table objects are defined in `/table.py`. This function will then trigger one of the three Tables_to_X functions below for further processing.

**Details:**
- Lambda Handler: textract_to_tables.lambda_handler
- Deployment: [Zip Archive Method](#deploy-to-aws-lambda)
- Dependencies: `/dependecies/textract_parsing_requirements.txt`
- Enviorment Variables: (To be added)
- Notes:
  - This function will reprocess tables with synchronous requests to Textract if a any row has an average cell confidence of below 80.

### Tables_to_72HR_Flights

This Lambda function is responsible for recieving table objects from [Textract_to_Tables](#textract_to_tables) function and converting the tables into flight objects. These flight objects are defined in `/flight.py`.

**Details:**
- Lambda Hanlder: tables_to_72hr_flights.lambda_handler
- Deployment: [Zip Archive Method](#deploy-to-aws-lambda)
- Dependencies: `/dependencies/convert_72hr_flights_requirements.txt`
- Eviroment Variables: (To be added)
- Notes: N/A

## Deploy to AWS Lambda

1) Make deployment directory:

```bash
mkdir deployment_package
```

2) Bundle dependencies:

```bash
pip install -r /path/to/function/requirements.txt --target ./deployment_package
```

> **Note:** If you are developing the functions in a virtual enviroment their package versions might not match the system python packages. To fix this, enter the virtual enviroment and then run the command above.

3) Copy custom libraries

```bash
cp /path/to/project/root/*.py ./deployment_package
```

4) (Optional) Copy firestore credentials

```bash
cp /path/to/project/root/creds.json ./deployment_package
```

5) Create zip archive

```bash
cd ./deployment_package
zip -r9 ../deployment_package.zip .
```
