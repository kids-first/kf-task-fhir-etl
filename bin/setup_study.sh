
#!/bin/bash

# Create SD_ME0WME0W test study in Data Service
# Ingest SD_ME0WME0W test study into Data Service

set -e

echo "\n ✨ Creating test study and sequencing center in Data Service ..."

curl -f -X POST \
-H "Content-Type: application/json" \
-d '{"kf_id": "SD_ME0WME0W", "external_id": "cat-study"}' \
$KF_API_DATASERVICE_URL/studies | jq '.results.kf_id'

curl -f  -X POST \
-H "Content-Type: application/json" \
-d '{"kf_id": "SC_A1JNZAZH", "name": "Baylor"}' \
$KF_API_DATASERVICE_URL/sequencing-centers | jq '.results.kf_id'

# echo "\n 🏭 Ingest test study data into Data Service ..."
kidsfirst ingest tests/data/SD_ME0WME0W

