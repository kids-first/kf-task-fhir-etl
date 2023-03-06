#!/bin/bash

# Setup python virtual env with project dependencies
# Setup docker compose stack

# --- CLI Options ---

# --delete-volumes will delete any volumes in the docker compose stack
# This means any data in Data Service or FHIR service will be lost

# --ingest will ingest the SD_ME0WME0W test study into Data Service 

set -e

START_TIME=$SECONDS
HAPI_DATA_DIR="./hapi/.data"
INGEST_DATA_DIR="./.data"
DELETE_VOLUMES=0
INGEST_STUDY=0

while [ -n "$1" ]; do 
	case "$1" in
	--delete-volumes) DELETE_VOLUMES=1 ;; 
	--ingest) INGEST_STUDY=1 ;; 
	*) echo "Option $1 not recognized" ;; 
	esac
	shift
done

echo "➡️  Begin development environment setup for HAPI 😃 ..."

if [ ! -d venv ]; then
echo "\n🐍 Setup Python virtual env and install deps ..."
    virtualenv venv
    source venv/bin/activate
    pip install -e .
else
    source venv/bin/activate
fi

echo "\n🐳 Start docker compose stack ..."

# Set env file
if [[ ! -f './hapi/.env' ]]; then
    cp './hapi/env.sample' './hapi/.env'
fi
if [[ ! -f './.env' ]]; then
    cp './hapi/.env' './.env'
fi
source ./hapi/.env

if [ $DELETE_VOLUMES -eq 1 ]; then
    echo "\n🗑️ Remove old volumes ..."
    rm -rf $HAPI_DATA_DIR/fhir_postgres
    rm -rf $HAPI_DATA_DIR/hapi
    docker compose -f ./hapi/docker-compose.yml down -v  
else
    docker compose -f ./hapi/docker-compose.yml down
fi

sleep 10
docker compose -f ./hapi/docker-compose.yml up  -d 

echo "🔥 Waiting for fhir server to finish deploying (may take a few minutes) ..."
until docker compose -f ./hapi/docker-compose.yml logs | grep "Server startup in"
do
    echo -n "."
    sleep 2
done

if [ $INGEST_STUDY -eq 1 ]; then
    ELAPSED=$(( SECONDS - START_TIME ))
    echo "\nElapsed Time: $ELAPSED seconds"
    rm -rf "$INGEST_DATA_DIR/LoadStage"
    ./bin/setup_study.sh
fi

ELAPSED=$(( SECONDS - START_TIME ))
echo "\nElapsed Time: $ELAPSED seconds"

echo "✅ --- Development environment setup complete! ---"

