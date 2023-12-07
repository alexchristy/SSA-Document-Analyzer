#!/bin/bash
rm -rf store_flights*
mkdir store_flights
cd store_flights
source ../../../store_flights_env/bin/activate
pip install -r ../../../dependencies/store_flights_requirements.txt --target .
cp ../../../*.py ./
cp ../../../creds.json ./
zip -r9 ../store_flights.zip .
cd ..
rm -rf store_flights