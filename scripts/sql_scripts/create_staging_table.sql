-- Create staging table for co2 emissions data, where it stores the data from the source file.
-- The staging layer in BigQuery stores clean data loaded from GCS, ensuring data quality and efficient incremental updates to the fact table without data duplication. 

CREATE TABLE IF NOT EXISTS staging.co2_emissions (
  make STRING,
  model STRING,
  vehicle_class STRING,
  engine_size FLOAT64,
  cylinders INT64,
  transmission STRING,
  fuel_type STRING,
  fuel_consumption_city FLOAT64,
  fuel_consumption_hwy FLOAT64,
  fuel_consumption_comb_lkm FLOAT64,
  fuel_consumption_comb_mpg INT64,
  co2_emissions INT64
);
