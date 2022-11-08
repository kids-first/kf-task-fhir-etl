# Kids First FHIR ETL Task Service

The Kids First FHIR ETL Task Service, built off of the [Kids First Data Ingest Library](https://github.com/kids-first/kf-lib-data-ingest), is a Python wrapper application, which

- Extracts tables from the KF Dataservice DB;
- Transforms the extracted tabular data to FHIR resources in JSON; and
- Loads the transformed records into a Kids First FHIR Service.

## Quickstart

### Running ETL from Command Line Interface

1. Make sure Python (>= 3.7) is installed.

2. Obtain three sets of credentials as follows:

   - Kids First Dataservice DB URL: Contact Kids First DRC DevOps Team.
   - FHIR USERNAME and PASSWORD: The Kids First FHIR ETL uses basic authentication for POST, PUT, PATCH, and DELETE . Contact Kids First DRC DevOps Team.
   - FHIR Cookie: Follow the instruction described [here](https://github.com/kids-first/kf-api-fhir-service).

3. Clone this repository:

```
$ git clone https://github.com/kids-first/kf-task-fhir-etl.git
$ cd kf-task-fhir-etl
```

4. Create and activate a virtual environment:

```
$ python3 -m venv venv
$ source venv/bin/activate
```

5. Install dependencies:

```
(venv) $ pip install --upgrade pip && pip install -e .
```

6. Create a `.env` with the following environment variable names:

```bash
KF_DATASERVICE_DB_URL=<PUT-KF-DATASERVICE-DB-URL>
KF_API_DATASERVICE_URL=<PUT-KF-API-DATASERVICE-URL> # e.g., https://kf-api-dataservice.kidsfirstdrc.org/
KF_API_FHIR_SERVICE_URL=<PUT-KF-API-FHIR-SERVICE-URL> # e.g., https://kf-api-fhir-service.kidsfirstdrc.org

FHIR_USERNAME=<PUT-FHIR-USERNAME>
FHIR_PASSWORD=<PUT-FHIR-USERNAME>
FHIR_COOKIE=<PUT-FHIR-COOKIE>
```

7. Get familiar with required arguments:

```
(venv) kidsfirst fhir-etl -h
Usage: kidsfirst fhir-etl [OPTIONS] KF_STUDY_IDS...

  Ingest a Kids First study(ies) into a FHIR server.

  Arguments:

      KF_STUDY_IDS - a KF study ID(s) concatenated by whitespace, e.g., SD_BHJXBDQK SD_M3DBXD12

Options:
  -h, --help  Show this message and exit.
```

8. Tunnel to the KF Dataservice DB (See also [here](https://github.com/d3b-center/d3b-cli-igor) or contact Kids First DRC DevOps Team):

```
(venv) igor awslogin
(venv) export AWS_PROFILE=Mgmt-Console-Dev-D3bCenter@232196027141
(venv) igor dev-env-tunnel --environment prd --cidr_block 0.0.0.0/0
```

9. Run the following command (the KF study IDs below are exemplars):

```
(venv) kidsfirst fhir-etl SD_ZXJFFMEF SD_46SK55A3
```

### Running ETL from Docker (TBD)
