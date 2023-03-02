
#!/bin/bash

# Setup python virtual env with project dependencies
# Setup docker compose stack

# --- CLI Options ---

# --delete-volumes will delete any volumes in the docker-compose stack
# This means any data in Data Service or FHIR service will be lost

# --ingest will ingest the SD_ME0WME0W test study into Data Service 

set -e

DATA_DIR="./.data"
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

echo "➡️  Begin development environment setup ..."

if [ ! -d venv ]; then
echo "\n🐍 Setup Python virtual env and install deps ..."
    virtualenv venv
    source venv/bin/activate
    pip install -e .
else
    source venv/bin/activate
fi

# Delete docker-compose volumes
if [ $DELETE_VOLUMES -eq 1 ]; then
    echo "\n🗑️ Remove old volumes ..."
    rm -rf $DATA_DIR/fhir_postgres
    rm -rf $DATA_DIR/hapi
fi

echo "\n🐳 Start docker compose stack ..."
source .env
docker compose down
sleep 10
docker compose up -d 

if [ $DELETE_VOLUMES -eq 1 ]; then
    echo "\n 👷‍♀️ Waiting for FHIR service to deploy (may take a minute) ..."
    docker-compose logs -f fhir | grep -cm1 "Started Application in"
fi

if [ $INGEST_STUDY -eq 1 ]; then
    rm -rf $DATA_DIR/LoadStage
    ./bin/setup_study.sh
fi

echo "✅ --- Development environment setup complete! ---"

