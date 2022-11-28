#!/bin/bash

# Usage ./scripts/entrypoint.sh SD_W00FW00F SD_ME0WME0W ...
set -e

# Read in KF study IDs from standard input
KF_STUDY_IDS=$@

# Run FHIR-ETL
kidsfirst fhir-etl $KF_STUDY_IDS
