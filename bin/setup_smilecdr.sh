#!/bin/bash

# Setup python virtual env with project dependencies
# Setup docker compose stack

# --- CLI Options ---

# --delete-volumes will delete any volumes in the docker compose stack
# This means any data in Data Service or FHIR service will be lost

# --ingest will ingest the SD_ME0WME0W test study into Data Service 

set -e

START_TIME=$SECONDS
DATA_DIR="./.data"
DELETE_VOLUMES=0
INGEST_STUDY=0
DOCKER_HUB_USERNAME=${DOCKER_HUB_USERNAME}
DOCKER_HUB_PW=${DOCKER_HUB_PW}

while [ -n "$1" ]; do 
	case "$1" in
	--delete-volumes) DELETE_VOLUMES=1 ;; 
	--ingest) INGEST_STUDY=1 ;; 
	*) echo "Option $1 not recognized" ;; 
	esac
	shift
done

echo "➡️  Begin development environment setup for Smile CDR 🤓 ..."

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
if [[ ! -f './smilecdr/.env' ]]; then
    cp './smilecdr/env.sample' './smilecdr/.env'
fi
if [[ ! -f './.env' ]]; then
    cp './smilecdr/.env' './.env'
fi
source ./smilecdr/.env

echo "\n🐳 Clean up docker stack from before ..."

# Delete docker volumes
if [ $DELETE_VOLUMES -eq 1 ]; then
    echo "\n🗑️ Remove old volumes ..."
    rm -rf $DATA_DIR/fhir_postgres
    rm -rf $DATA_DIR/smilecdr
    docker compose -f ./smilecdr/docker-compose.yml down -v  
else
    docker compose -f ./smilecdr/docker-compose.yml down
fi

# Check docker hub creds 
if [[ -z $DOCKER_HUB_USERNAME ]] || [[ -z $DOCKER_HUB_PW ]]
then
    echo "🔐 You need the Kids First DRC docker hub credentials to continue" 
    echo "Please contact the Github repo admins: natasha@d3b.center or meenchulkim@d3b.center" 
    exit 1
fi

echo "Logging into Docker Hub ..."
echo "$DOCKER_HUB_PW" | docker login -u "$DOCKER_HUB_USERNAME" --password-stdin

sleep 10

echo "\n🐳 Start docker compose stack ..."
docker-compose -f ./smilecdr/docker-compose.yml pull --ignore-pull-failures
docker compose -f ./smilecdr/docker-compose.yml up  -d --build

echo "🔥 Waiting for fhir server to finish deploying (may take up to 10 minutes) ..."
until docker compose -f ./smilecdr/docker-compose.yml logs | grep "up and running"
do
    echo -n "."
    sleep 2
done

if [ $INGEST_STUDY -eq 1 ]; then
    ELAPSED=$(( SECONDS - start_time ))
    echo "\nElapsed Time: $ELAPSED seconds"
    rm -rf $DATA_DIR/LoadStage
    ./bin/setup_study.sh
fi

ELAPSED=$(( SECONDS - start_time ))
echo "\nElapsed Time: $ELAPSED seconds"

echo "✅ --- Development environment setup complete! ---"

