-- Create fact table for CO2 emissions data

CREATE TABLE IF NOT EXISTS fact.co2_emissions (
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
