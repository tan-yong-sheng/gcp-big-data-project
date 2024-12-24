-- upsert data from staging table to fact table, to avoid duplicates during insertion task

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
INSERT (make, model, vehicle_class, engine_size, cylinders, transmission, fuel_type, fuel_consumption_city, fuel_consumption_hwy, fuel_consumption_comb_lkm, fuel_consumption_comb_mpg, co2_emissions)
VALUES (source.make, source.model, source.vehicle_class, source.engine_size, source.cylinders, source.transmission, source.fuel_type, source.fuel_consumption_city, source.fuel_consumption_hwy, source.fuel_consumption_comb_lkm, source.fuel_consumption_comb_mpg, source.co2_emissions);
