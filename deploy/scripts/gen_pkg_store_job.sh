#!/bin/bash

# Define the root directory for the lambda function
LAMBDA_ROOT_DIR="store_flights"

# Clear and recreate the lambda root directory
rm -rf $LAMBDA_ROOT_DIR
mkdir $LAMBDA_ROOT_DIR

# Install dependencies in the lambda root directory
# Note: The virtual environment activation is not necessary
pip install -r ../../dependencies/store_flights_requirements.txt --target $LAMBDA_ROOT_DIR

# Copy Python files and credentials to the lambda root directory
cp ../../*.py $LAMBDA_ROOT_DIR/
cp ../../creds.json $LAMBDA_ROOT_DIR/

# Create the deployment package
cd $LAMBDA_ROOT_DIR
zip -r9 ../store_flights.zip .

# Clean up
cd ..
rm -rf $LAMBDA_ROOT_DIR