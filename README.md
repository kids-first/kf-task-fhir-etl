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
$ git clone https://github.com/kids-first/kf-task-ingest.git
$ cd kf-task-ingest
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
(venv) fhiretl ingest -h
Usage: fhiretl ingest [OPTIONS] KF_STUDY_IDS...

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
(venv) fhiretl ingest SD_ZXJFFMEF SD_46SK55A3
```

## Contributing 

If you are a developer on the project you will need to get your development 
environment setup in order to modify code and run tests. You can do this easily
in a few steps.

There are setup scripts to run either the open source HAPI FHIR server or the 
commercial version of HAPI, Smile CDR. 

### Setup - Smile CDR Docker-Compose Stack
Since we use Smile CDR in production, we will use it in our development
environment and develop against it. HAPI is just included for experimentation
and convenience.

```shell
# Get the source code
git clone git@github.com:kids-first/kf-task-fhir-etl.git
cd kf-task-fhir-etl

# Run the development environment setup which does the following:
# - Setup your virtualenv if you don't already have one
# - Create the necessary .env file if you don't already have one
# - Delete any old docker volumes
# - Bring up Data Service in the docker compose stack
# - Bring up Smile CDR in the docker compose stack
# - Ingest a test study `SD_ME0WME0W` into Data Service

./bin/setup_smilecdr.sh --delete-volumes --ingest
```
Note that all of the Smile CDR files are in kf-task-fhir-etl/smilecdr. This 
includes necessary config files and the docker-compose.yml file.

If all goes well, you can test the CLI out by ETLing the test study from 
Data Service into the FHIR server:

```shell
fhiretl ingest SD_ME0WME0W
```

Now check the FHIR server to ensure data was created. You can find the 
username/password and FHIR endpoint in the kf-task-fhir-etl/.env file that was
created during setup.

```shell
# You should see a total of 9 Patients
curl -v -X GET -H 'Content-Type: application/json' \
-u admin:password http://127.0.0.1:8000/Patient\?_summary\=count
```

### Setup - (Optional) HAPI Docker-Compose Stack
**Only do this if you want to work with HAPI instead of Smile CDR**

```shell
# Get the source code
git clone git@github.com:kids-first/kf-task-fhir-etl.git
cd kf-task-fhir-etl

# Run the development environment setup which does the same thing as 
the smilecdr setup script
./bin/setup_hapi.sh --delete-volumes --ingest
```
Note that all of the HAPI files are in kf-task-fhir-etl/hapi. This 
includes necessary config files and the docker-compose.yml file.

If all goes well, you can test the CLI out by ETLing the test study from 
Data Service into the FHIR server:

```shell
fhiretl ingest SD_ME0WME0W
```

Now check the FHIR server to ensure data was created. You can find the 
username/password and FHIR endpoint in the kf-task-fhir-etl/.env file that was
created during setup.

```shell
# You should see a total of 9 Patients
curl -v -X GET -H 'Content-Type: application/json' \
-u admin:password http://127.0.0.1:8080/fhir/Patient\?_summary\=count
```

