# SmartSpaceA Document Analyzer

## Overview

This project contains the code and tests for AWS Lambda functions that process PDF flight schedules. 

## Lambda Functions

1. [Start-PDF-Textract-Job](https://github.com/alexchristy/SSA-Document-Analyzer/wiki/Start%E2%80%90PDF%E2%80%90Textract%E2%80%90Job)
2. [Textract-to-Tables](https://github.com/alexchristy/SSA-Document-Analyzer/wiki/Textract%E2%80%90to%E2%80%90Tables)
3. [Process-72HR-Flights](https://github.com/alexchristy/SSA-Document-Analyzer/wiki/Process%E2%80%9072HR%E2%80%90Flights)
4. [Store-Flights](https://github.com/alexchristy/SSA-Document-Analyzer/wiki/Store%E2%80%90Flights)

All information related to the lambda functions in the processing chain can be found in their respective pages. The list above is ordered based on execution order.

## Building

1. [ZIP Archive Method](https://github.com/alexchristy/SSA-Document-Analyzer/wiki/Lambda-Deployment#scripted-zip-deployment)
2. [Docker Container Image](https://github.com/alexchristy/SSA-Document-Analyzer/wiki/Deploying-Textract%E2%80%90to%E2%80%90Tables-Container#steps)

Only the [Textract-to-Tables](https://github.com/alexchristy/SSA-Document-Analyzer/wiki/Textract%E2%80%90to%E2%80%90Tables) function uses the Docker image deployment method.

## Testing

Information on function specific testing can be found in each of the [lambda function](https://github.com/alexchristy/SSA-Document-Analyzer/wiki#lambda-functions) wiki pages. Information on running the whole lambda chain in testing mode for things like end to end testing can be found [here](https://github.com/alexchristy/SSA-Document-Analyzer/wiki/Testing-Mode).
