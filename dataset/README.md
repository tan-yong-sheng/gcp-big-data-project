## Data Description

In the recent years, the canker of global warming has been worsened by the continual increase in the emission of Carbon dioxide into the atmosphere from various sources of which automobile vehicles are a part and this threaten the life of humans on earth.

This dataset contains the emission rate of CO2 by vehicles in Canada and is useful in monitoring and predicting carbon dioxide emission rate by automobile vehicles.
'Data Description.csv' contains the description for the various features in the dataset

'co2_emissions_canada.csv' contains the CO2 emission data of vehicles in Canada.

The data comes from Kaggle: https://www.kaggle.com/datasets/isaacfemiogunniyi/co2-emission-of-vehicles-in-canada/data

## Data Dictionary

| **Column header**                     | **Label**                            | **Description**                                                                                       |
|--------------------------------|--------------------------------------|-------------------------------------------------------------------------------------------------------|
| make                           | **Make**                             | The company that manufactures the vehicle.                                                            |
| model                          | **Model**                            | The vehicle's model.                                                                                  |
| vehicle_class                  | **Vehicle Class**                    | Vehicle class by utility, capacity, and weight.                                                       |
| engine_size                    | **Engine Size (L)**                  | The engine's displacement in liters.                                                                  |
| cylinders                      | **Cylinders**                  | The number of cylinders.                                                                                    |
| transmission                   | **Transmission**                     | The transmission type: A = Automatic, AM = Automatic Manual, AS = Automatic with select shift, AV = Continuously variable, M = Manual, 3-10 = the number of gears. |
| fuel_type                      | **Fuel Type**                        | The fuel type: X = Regular gasoline, Z = Premium gasoline, D = Diesel, E = Ethanol (E85), N = Natural gas. |
| fuel_comsumption_comb_lkm      | **Fuel Consumption Comb (L/100 km)** | Combined city/highway (55%/45%) fuel consumption in liters per 100 km (L/100 km).                      |
| fuel_comsumption_comb_mpg     | **Fuel Consumption Comb (mpg)**       | 	Fuel Consumption Combined (mpg): Combined city/highway (55%/45%) fuel consumption in miles per gallon.  |
| co2_emissions                  | **CO2 Emissions (g/km)**             | The tailpipe carbon dioxide emissions in grams per kilometer for combined city and highway driving.   |


## Data Transformation Plan

We will perform a few data processing via pyspark in Dataproc as follows:
- Remove duplicated rows, if any
- Remove rows with missing values, if any

