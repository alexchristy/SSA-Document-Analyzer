#!/bin/bash
rm -rf proc_72_flights*
mkdir proc_72_flights
cd proc_72_flights
source ../../../convert_72hr_env/bin/activate
pip install -r ../../../dependencies/process_72hr_requirements.txt --target .
cp ../../../*.py ./
cp ../../../creds.json ./
zip -r9 ../proc_72_flights.zip .
cd ..
rm -rf proc_72_flights