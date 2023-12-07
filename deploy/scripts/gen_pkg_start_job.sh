#!/bin/bash
rm -rf start_job*
mkdir start_job
cd start_job
source ../../../start_job_env/bin/activate
pip install -r ../../../dependencies/start_job_requirements.txt --target .
cp ../../../*.py ./
cp ../../../creds.json ./
zip -r9 ../start_job.zip .
cd ..
rm -rf start_job