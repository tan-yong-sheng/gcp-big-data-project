# Part 3 - Orchestration Layer : Setup Google Cloud Composer
Setting Up Google Cloud Composer Environment to orchestrate ETL Pipeline
------------------------------------------------------------------------

This guide outlines the steps to create a Cloud Composer environment for orchestrating ETL workflows using Airflow.

Prerequisites
-------------

1.  Google Cloud Platform account with billing enabled
2.  Google Cloud Shell
3.  Existing Cloud Function setup shown in Part 1
4.  Existing Dataproc cluster setup shown in Part 2
5.  Required APIs enabled in your GCP project, in Part 1 and Part 2

Step 1: Environment Setup
-------------------------

The commands are executed under the cloud shell terminal

![](../images/Part%203%20-%20Orchestration%20Layer.jpg)

*   Set environment variables in Cloud Shell:

```text-plain
> export PROJECT_ID=steam-link-443214-f2
> export REGION=us-central1
> export ZONE=us-central1-f
> export PHS_CLUSTER_NAME=phs-my
> export BUCKET=dataproc-composer
> export STAGING_BUCKET=dataproc-composer-staging
> export DAGS_BUCKET=us-central1-airflow-0d80b3b5-bucket
> export COMPOSER_ENV_NAME=airflow
```

Step 2: Enable Required APIs
----------------------------

```bash
> gcloud services enable composer.googleapis.com
```

Step 3: Create Cloud Composer Environment
-----------------------------------------

*   Deploy the Composer environment with configurations:

```bash
> gcloud composer environments create ${COMPOSER_ENV_NAME} \
  --project ${PROJECT_ID} \
  --location ${REGION} \
  --environment-size small \
  --image-version composer-3-airflow-2.9.3 \
  --env-variables=MY_PROJECT_ID=${PROJECT_ID},REGION=${REGION},ZONE=${ZONE},PHS_CLUSTER_NAME=${PHS_CLUSTER_NAME},BUCKET=${BUCKET},STAGING_BUCKET=${STAGING_BUCKET} \
  --storage-bucket=${DAGS_BUCKET}
```

Step 4: Configure Dataproc Integration
--------------------------------------

Move to Dataproc's script we've written earlier in Part 2 at `**scripts/dataproc_gcs_to_gbq_job.py**` to the the Composer's GCS bucket:

```bash
> gsutil cp dpr_scripts/dataproc_gcs_to_gbq_job.py gs://${DAGS_BUCKET}/scripts/dataproc_gcs_to_gbq_job.py
```

![](../images/3_Part%203%20-%20Orchestration%20Layer.jpg)

Step 5: Set Up Airflow Connection
---------------------------------

Configure HTTP connection for Cloud Function integration:

```bash
> gcloud composer environments run ${COMPOSER_ENV_NAME} \
    --location ${REGION} connections -- \
    add 'http_default' \
    --conn-type 'https' \
    --conn-host https://${REGION}-${PROJECT_ID}.cloudfunctions.net
```

This command will create a new connection called ‘http\_default’ in Google Cloud Composer

![](../images/5_Part%203%20-%20Orchestration%20Layer.jpg)

Step 6: Deploy DAG
------------------

*   Create the DAG file:

```bash
nano dags/co2_emissions_dag.py
```

Add the following Python Code, as it is a DAG script for Google Cloud Composer

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import Variable
import os
import json

# Project Configuration
PROJECT_ID = os.environ.get('MY_PROJECT_ID')
GOOGLE_CONN_ID = os.environ.get('GOOGLE_CONN_ID')
REGION = os.environ.get('REGION')
ZONE = os.environ.get('ZONE')

BUCKET_NAME = os.environ.get('BUCKET')
TEMP_BUCKET_NAME = os.environ.get('STAGING_BUCKET')
DAGS_BUCKET_NAME = os.environ.get('DAGS_BUCKET')

## Variables for Dataproc jobs
PHS_CLUSTER_NAME = os.environ.get('PHS_CLUSTER_NAME')

## Variables for BigQuery jobs
BIGQUERY_TABLE = f"{PROJECT_ID}.staging.co2_emissions"

## Variables for Python file & Dataset
PYTHON_SCRIPT_FILE = f"gs://{DAGS_BUCKET_NAME}/scripts/dataproc_gcs_to_gbq_job.py"
DATASET_FILE = f"gs://{BUCKET_NAME}/dataset/co2_emissions_canada.csv"

## Variables for Cloud Function trigger to download data from Kaggle
FUNCTION_NAME = "download_and_upload"

# Setup configuration for pyspark job in Dataproc
PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": PHS_CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": PYTHON_SCRIPT_FILE,
        "args": [
            f"--gcs_path={DATASET_FILE}",
            f"--bigquery_table={BIGQUERY_TABLE}",
            f"--bucket_name={TEMP_BUCKET_NAME}",
        ],
    },
}

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 1),  # Must be in the past
    'retries': 1,
    'retry_delay': timedelta(seconds=50),
}

# DAG definition
with DAG("SparkETL", schedule_interval="@weekly", default_args=default_args) as dag:

    # Run cloud function with SimpleHttpOperator
    t1 = SimpleHttpOperator(
        task_id='invoke_cloud_function',
        method='POST',
        http_conn_id='http_default',
        endpoint=FUNCTION_NAME,
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            "bucket-name": BUCKET_NAME,  # passing the bucket name directly
        }),
    )

    # Submit PySpark job to Dataproc
    t2 = DataprocSubmitJobOperator(
        task_id="pyspark_task",
        job=PYSPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID,
    )

    # Task to write data to BigQuery
    t3 = BigQueryInsertJobOperator(
        task_id="upsert_co2_emissions_to_bigquery",
        configuration={
            "query": {
                "query": """
                    MERGE INTO fact.co2_emissions AS target
                    USING staging.co2_emissions AS source
                    ON target.make = source.make 
                    AND target.model = source.model 
                    AND target.vehicle_class = source.vehicle_class
                    AND target.engine_size = source.engine_size
                    AND target.cylinders = source.cylinders
                    AND target.transmission = source.transmission
                    AND target.fuel_type = source.fuel_type
                    AND target.fuel_consumption_city = source.fuel_consumption_city
                    AND target.fuel_consumption_hwy = source.fuel_consumption_hwy
                    AND target.fuel_consumption_comb_lkm = source.fuel_consumption_comb_lkm
                    AND target.fuel_consumption_comb_mpg = source.fuel_consumption_comb_mpg
                    AND target.co2_emissions = source.co2_emissions
                    WHEN MATCHED THEN
                    UPDATE SET
                        engine_size = source.engine_size,
                        cylinders = source.cylinders,
                        transmission = source.transmission,
                        fuel_type = source.fuel_type,
                        fuel_consumption_city = source.fuel_consumption_city,
                        fuel_consumption_hwy = source.fuel_consumption_hwy,
                        fuel_consumption_comb_lkm = source.fuel_consumption_comb_lkm,
                        fuel_consumption_comb_mpg = source.fuel_consumption_comb_mpg,
                        co2_emissions = source.co2_emissions
                    WHEN NOT MATCHED THEN
                    INSERT (make, model, vehicle_class, engine_size, cylinders, 
                        transmission, fuel_type, fuel_consumption_city, 
                        fuel_consumption_hwy, fuel_consumption_comb_lkm, 
                        fuel_consumption_comb_mpg, co2_emissions)
                    VALUES (source.make, source.model, source.vehicle_class,
                        source.engine_size, source.cylinders, source.transmission, 
                        source.fuel_type, source.fuel_consumption_city, 
                        source.fuel_consumption_hwy, source.fuel_consumption_comb_lkm, 
                        source.fuel_consumption_comb_mpg, source.co2_emissions);
                """,
                "useLegacySql": False,
            }
        },
        location="US",
        gcp_conn_id="google_cloud_default",  # Ensure this connection exists in Airflow
    )

    # Define task dependencies
    t1 >> t2 >> t3
```

So, here is the flow of the tasks being executed and automated:

*   Scraping data from Kaggle via Python code, executed via Cloud Function + Store the scraped data into Google Cloud Storage
*   Perform ETL processing via Pyspark in DataProc, and then load the clean data into staging table in BigQuery
*   Upsert the data from staging table in BigQuery to fact table in BigQuery

![](../images/6_Part%203%20-%20Orchestration%20Layer.jpg)

The DAG code is displayed in Airflow UI as well:

![](../images/7_Part%203%20-%20Orchestration%20Layer.jpg)

*   Upload DAG to Composer bucket:

```bash
> gsutil cp dags/co2_emissions_dag.py gs://${DAGS_BUCKET}/dags/co2_emissions_dag.py
```

Step 7: Verify Deployment
-------------------------

*   Check if DAG file is properly uploaded to Composer's GCS bucket:

```bash
> gsutil cat gs://${DAGS_BUCKET}/dags/co2_emissions_dag.py
```

![](../images/4_Part%203%20-%20Orchestration%20Layer.jpg)

DAG Details
-----------

The DAG performs the following operations:

1.  Triggers Cloud Function to download data from Kaggle
2.  Submits PySpark job to Dataproc for data processing
3.  Loads processed data into BigQuery using MERGE operation for upsert operation which avoid duplicated entries being appended multiple times in BigQuery


References
----------

*   Automating Cloud Function with Google Composer (Airflow 2.9.3) – Daily Runs Made Easy [https://www.linkedin.com/pulse/automating-cloud-function-google-composer-airflow-293-jader-lima-2suwf](https://www.linkedin.com/pulse/automating-cloud-function-google-composer-airflow-293-jader-lima-2suwf)
*   The Complete Hands-On Introduction to Apache Airflow [https://www.udemy.com/course/the-complete-hands-on-course-to-master-apache-airflow](https://www.udemy.com/course/the-complete-hands-on-course-to-master-apache-airflow)
