#!/bin/bash

# Check if exactly one argument is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <study>"
    exit 1
fi

study="$1"
#endpoints=("Patient" "ResearchStudy" "ResearchSubject" "Condition" "Observation" "Specimen" "DocumentReference")
endpoints=("DocumentReference" "Specimen" "Observation" "Condition" "ResearchSubject" "ResearchStudy" "Patient")

for endpoint in "${endpoints[@]}"; do
    python fhir-delete.py "$endpoint" "$study"
done
