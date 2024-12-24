# Part 2a - Data Processing Layer : Set up Dataproc for ET: task
Setting Up Dataproc for Data Processing Pipeline
------------------------------------------------

This guide outlines the steps to set up a Dataproc cluster and run PySpark jobs to process data from Google Cloud Storage to BigQuery.

Prerequisites
-------------

*   Google Cloud Platform account with billing enabled
*   Google Cloud Shell or local environment with gcloud CLI installed
*   Required IAM permissions

Step 1: Configure Environment Variables
---------------------------------------

The commands are executed under the cloud shell terminal…

![](../images/2_Part%202%20-%20Data%20Processing%20Layer.jpg)

Set up the necessary environment variables in Cloud Shell:

```bash
> export PROJECT_ID=steam-link-443214-f2
> export REGION=us-central1
> export BUCKET=dataproc-composer
> export STAGING_BUCKET=dataproc-composer-staging
> export PHS_CLUSTER_NAME=phs-my
> export SERVICE_ACCOUNT_EMAIL=552356513533-compute@developer.gserviceaccount.com
```

Step 2: Enable Required APIs
----------------------------

Enable the necessary Google Cloud APIs:

```bash
> gcloud services enable \
  storage.googleapis.com \
  bigquery.googleapis.com \
  dataproc.googleapis.com
```

Step 3: Configure Network Settings
----------------------------------

*   Enable private Google Access for the default subnet:

```bash
# Enable private IP Google access
> gcloud compute networks subnets update default \
 --region=${REGION} \
 --enable-private-ip-google-access
```

```bash
# Verify the configuration
> gcloud compute networks subnets describe default \
 --region=${REGION} \
 --format="get(privateIpGoogleAccess)"
```

Step 4: Set Up Storage
----------------------

*   Create GCS buckets:

```bash
# Create main data bucket
> gsutil mb -l ${REGION} gs://${BUCKET}

# Create staging bucket
> gsutil mb -l ${REGION} gs://${STAGING_BUCKET}
```

Create necessary folder structure:

*   Create a `dataset` folder for your data
*   Create a `staging` folder for temporary files

![](images/Part 2 - Data Processing Layer.jpg)

Step 5: Configure IAM Permissions
---------------------------------

Grant storage admin permissions to the service account:

```bash
> gcloud projects add-iam-policy-binding ${PROJECT_ID} \
    --member=serviceAccount:${SERVICE_ACCOUNT_EMAIL} \
    --role="roles/storage.admin"
```

  
Step 6: Create Dataproc Cluster
----------------------------------

Create a single-node Dataproc cluster with component gateway enabled:

```bash
> gcloud dataproc clusters create ${PHS_CLUSTER_NAME} \
  --region=${REGION} \
  --single-node \
  --bucket=${BUCKET} \
  --enable-component-gateway
```

Output here:

![](/images/1_Part%202%20-%20Data%20Processing%20Layer.jpg)

Step 7: Test PySpark Setup
--------------------------

*   Create a directory for the Dataproc's script

```bash
mkdir -p dpr_scripts
```

*   Create a test PySpark script:

```bash
nano dpr_scripts/test.py
```

Add the following to the test.py file

```python
from pyspark.sql import SparkSession

def main():
   # Create a SparkSession
   spark = SparkSession.builder \
       .appName("HelloWorldPySpark") \
       .getOrCreate()

   # Sample data
   data = [("Alice", 28), ("Bob", 24), ("Cathy", 22)]

   # Convert the data to a DataFrame
   df = spark.createDataFrame(data, ["Name", "Age"])

   # Show the DataFrame
   df.show()

   # Stop the SparkSession
   spark.stop()

if __name__ == "__main__":
   main()
```

*   Submit the test job in Cloud Shell:

```bash
> gcloud dataproc jobs submit pyspark dpr_scripts/test.py \
   --cluster=${PHS_CLUSTER_NAME} \
   --region=${REGION} \
   --jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.26.0.jar
```

Step 8: GCS to BigQuery Data Pipeline
-------------------------------------

*   Create the data processing script (dataproc\_gcs\_to\_gbq\_job.py):
    *   Implements data loading from GCS
    *   Includes schema definition
    *   Handles data deduplication
    *   Overwrite the data to BigQuery's staging table

*   Create `**dataproc_gcs_to_gbq_job.py**`

```bash
nano dpr_scripts/dataproc_gcs_to_gbq_job.py
```

Add the following Python Code for the Dataproc's script:

```python
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType


def create_spark_session():
    # Create Spark session with the necessary configurations
    spark = SparkSession.builder \
        .appName("GCS_to_BigQuery") \
        .getOrCreate()
    return spark


def load_data_to_bigquery(spark, gcs_path, bigquery_table, bucket_name):
    """
    Function to load data from a GCS bucket to BigQuery.
    Parameters:
        spark: Spark session object.
        gcs_path: GCS path to the input dataset (e.g., gs://bucket/dataset.csv).
        bigquery_table: Destination BigQuery table (e.g., `project.dataset.table_name`).
        bucket_name: Temporary GCS bucket for BigQuery load jobs.
    """
    # Define the schema explicitly to ensure correct data types
    schema = StructType([
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("vehicle_class", StringType(), True),
        StructField("engine_size", FloatType(), True),
        StructField("cylinders", IntegerType(), True),
        StructField("transmission", StringType(), True),
        StructField("fuel_type", StringType(), True),
        StructField("fuel_consumption_city", FloatType(), True),
        StructField("fuel_consumption_hwy", FloatType(), True),
        StructField("fuel_consumption_comb_Lkm", FloatType(), True),
        StructField("fuel_consumption_comb_mpg", IntegerType(), True),
        StructField("co2_emissions", IntegerType(), True),
    ])

    # Read the dataset from GCS with the predefined schema
    df = spark.read.option("header", "true").schema(schema).csv(gcs_path)
    print(f"Read data from GCS: {gcs_path}")

    # De-duplicate the data based on all rows
    df = df.distinct()
    print("De-duplicated the dataset based on all columns.")

    # Remove rows with missing values
    df = df.dropna()
    print("Removed rows with missing values.")

    # Show the schema and a few records (for debugging purposes)
    df.printSchema()
    df.show(5)

    # Write the DataFrame to BigQuery
    df.write \
        .format("bigquery") \
        .option("table", bigquery_table) \
        .option("temporaryGcsBucket", bucket_name) \
        .mode("overwrite") \
        .save()

    print(f"Data written to BigQuery table: {bigquery_table}")


def main():
    # Set up argument parsing
    parser = argparse.ArgumentParser(description="Load data from GCS to BigQuery using PySpark.")
    parser.add_argument("--gcs_path", required=True, help="GCS path to the input dataset (e.g., gs://bucket/dataset.csv).")
    parser.add_argument("--bigquery_table", required=True, help="Destination BigQuery table (e.g., project.dataset.table).")
    parser.add_argument("--bucket_name", required=True, help="Temporary GCS bucket for BigQuery load jobs.")

    args = parser.parse_args()

    # Create Spark session
    spark = create_spark_session()

    # Load data from GCS to BigQuery
    load_data_to_bigquery(spark, args.gcs_path, args.bigquery_table, args.bucket_name)


if __name__ == "__main__":
    main()
```

*   Submit the data processing job in Cloud Shell:

```bash
> gcloud dataproc jobs submit pyspark dpr_scripts/dataproc_gcs_to_gbq_job.py \
   --cluster=${PHS_CLUSTER_NAME} \
   --region=${REGION} \
   -- \
   --gcs_path=gs://${BUCKET}/dataset/co2_emissions_canada.csv \
   --bigquery_table=${PROJECT_ID}.fact.co2_emissions \
   --bucket_name=${STAGING_BUCKET}
```

![](../images/4_Part%202%20-%20Data%20Processing%20Layer.jpg)

And here is the UI where you could monitor the Dataproc's Job:

![](../images/3_Part%202%20-%20Data%20Processing%20Layer.jpg)

Script Features
---------------

The GCS to BigQuery script includes:

*   Data deduplication
*   Handling missing values

Monitoring
----------

You can monitor job progress through:

*   Dataproc Jobs UI
*   Cloud Logging

References
----------

*   [https://ilhamaulanap.medium.com/data-lake-with-pyspark-through-dataproc-gcp-using-airflow-d3d6517f8168](https://ilhamaulanap.medium.com/data-lake-with-pyspark-through-dataproc-gcp-using-airflow-d3d6517f8168) 
*   [https://medium.com/google-cloud/apache-spark-and-jupyter-notebooks-made-easy-with-dataproc-component-gateway-fa91d48d6a5a](https://medium.com/google-cloud/apache-spark-and-jupyter-notebooks-made-easy-with-dataproc-component-gateway-fa91d48d6a5a) 
*   [https://freedium.cfd/medium.com/@shaloomathew/all-you-need-to-know-about-google-cloud-dataproc-part-1-fb70b0af4c60](https://freedium.cfd/medium.com/@shaloomathew/all-you-need-to-know-about-google-cloud-dataproc-part-1-fb70b0af4c60) 
*   [https://codelabs.developers.google.com/dataproc-serverless#2](https://codelabs.developers.google.com/dataproc-serverless#2)