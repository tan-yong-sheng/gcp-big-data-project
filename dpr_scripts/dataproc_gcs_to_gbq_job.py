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