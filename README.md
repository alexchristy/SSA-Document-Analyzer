# SmartSpaceA Document Analyzer

## Overview

This project contains the code and tests for AWS Lambda functions that process PDF flight schedules. 

## Table of Contents

- [Functions](#functions)
  - [Start-PDF-Textract-Job](#start-pdf-textract-job)
  - [Textract_to_Tables](#textract_to_tables)
  - [Tables_to_72HR_Flights](#tables_to_72hr_flights)
- [Deployment](#deployment)

## Functions

### Start-PDF-Textract-Job

> **Trigger**: Uploading new PDFs to an S3 bucket.

- **Lambda Handler**: `start_pdf_textract_tables_job.lambda_handler`
- **Deployment**: [Zip Archive Method](#deployment)
- **Dependencies**: See `dependencies/start_job_requirements.txt`
- **Environment Variables**: 
  - `FS_CRED_PATH`: Firebase creds.json path for Firestore.
  - `PDF_ARCHIVE_COLLECTION`: Firestore collection for PDF hash retrieval.
  - `SNS_ROLE_ARN`: IAM Role for SNS pipeline.
  - `SNS_TOPIC_ARN`: SNS Topic ARN.

---

### Textract_to_Tables

> **Responsibility**: Processes table objects from Textract.

- **Lambda Handler**: `textract_to_tables.lambda_handler`
- **Deployment**: [Zip Archive Method](#deployment)
- **Dependencies**: See `dependencies/textract_parsing_requirements.txt`
- **Eviroment Variables:**
  - `DOWNLOAD_DIR`: Path to directory where pdfs can be download to reprocesses them with screenshots.
  - `FS_CRED_PATH`: Path to Firestore credentials.
  - `PDF_ARCHIVE_COLLECTION`: Name of firestore collection where pdf info is stored.
- **Notes**: 
  - Reprocesses tables if avergae cell confidence for a row is below 80%.

---

### Process-72HR-Flights

> **Responsibility**: Converts table objects from 72hr schedules into flight objects.

- **Lambda Handler**: `process_72hr_flights.lambda_handler`
- **Deployment**: [Zip Archive Method](#deployment)
- **Dependencies**: See `dependencies/process_72hr_requirements.txt`

## Deployment

### Steps

1. **Create Deployment Directory**
    ```bash
    mkdir deployment_package
    ```
   
2. **Install Dependencies**
    ```bash
    pip install -r /path/to/function/requirements.txt --target ./deployment_package
    ```
    > **Note**: For virtual environments, activate it before running the above command.
   
3. **Copy Custom Libraries**
    ```bash
    cp /path/to/project/root/*.py ./deployment_package
    ```
   
4. **(Optional) Copy Firestore Credentials**
    ```bash
    cp /path/to/project/root/creds.json ./deployment_package
    ```

5. **Create Zip Archive**
    ```bash
    cd ./deployment_package
    zip -r9 ../deployment_package.zip .
    ```
