-- train a multivariate linear regression model to predict CO2 emissions based on engine size and fuel consumption

CREATE OR REPLACE MODEL model.co2_emissions_model
OPTIONS(
  model_type = 'LINEAR_REG',
  input_label_cols = ['output']
) AS
SELECT
  engine_size,
  fuel_consumption_city,
  fuel_consumption_hwy,
  fuel_consumption_comb_lkm,
  ce.co2_emissions AS output
FROM
  fact.co2_emissions AS ce