# Data LakeHouse Architecture Pipeline

This project demonstrates the design and implementation of a basic Data LakeHouse Architecture Pipeline using Google Cloud Platform (GCP) services. The pipeline is structured into six key layers: Orchestration, Ingestion, Storage, Process, Analytics, and Visualization. Each layer plays a critical role in transforming raw data into actionable insights, ensuring scalability, reliability, and automation at every stage of the data workflow.

![](/images/data-pipeline-architecture.png)


We would like to invite you to explore our documentation guide, which outlines the setup steps for creating a simple Data Lakehouse in Google Cloud. Our team utilized tools like Cloud Functions, Google Cloud Storage, Dataproc, BigQuery, and Looker Studio to build this streamlined solution.
- [Part 1 - Data Ingestion Layer with Cloud Function](/setup_docs/Part%201%20-%20Data%20Ingestion%20Layer.md)
- [Part 2a - Data Processing Layer with Dataproc](/setup_docs/Part%202a%20-%20Data%20Processing%20Layer.md)
- [Part 2b - Data Processing Layer with Cloud SQL in BigQuery](/setup_docs/Part%202b%20-%20Data%20Processing%20Layer.md)
- [Part 3 - Analytics Layer with BigQuery](/setup_docs/Part%203%20-%20Analytics%20Layer.md)
- [Part 4 - Orchestration Layer with Composer](/setup_docs/Part%204%20-%20Orchestration%20Layer.md)

And here is the a simple dataset about CO2 emissions in Canada we're performing data processing, analytics modeling and visualization with, using tools in Google Cloud!
- [Data Description](/dataset/)

## Overview

Data is collected from various sources, including Kaggle, through automated Python scripts and cloud functions. This data is then stored securely in Google Cloud Storage, which serves as a scalable and durable data lake. The data undergoes ETL (Extract, Transform, Load) processing using Dataproc (for distributed data processing) and Cloud SQL. Once the data is prepared, it is loaded into BigQuery for fast querying and analytics. Finally, insights are visualized through Looker Studio, providing stakeholders with interactive dashboards and reports to drive data-driven decisions.

## Orchestration

The entire data pipeline is orchestrated using **Google Cloud Composer**, which is based on Apache Airflow. Composer automates and coordinates the key tasks of the pipeline:
- **Data Ingestion Layer**: Automates the ingestion of CO2 emissions data from Kaggle using the Kaggle API.
- **Data Processing Layer**: Manages the coordination between Dataproc (PySpark) and Cloud SQL for data transformation and loading into BigQuery.

![](/images/airflow-task-orchestration.png)

## Data Ingestion

The ingestion layer collects raw data from various sources, including APIs and pre-existing datasets. For this project, the dataset on **Carbon Dioxide (CO2) emissions by vehicles** is fetched from Kaggle using the Kaggle API, and the process is automated using **Google Cloud Functions**. This ensures seamless, repeatable data collection and ingestion into Google Cloud Storage.

![](/images/cloud-function-for-data-ingest.png)

## Data Storage

In the **Data Storage Layer**, raw data is securely stored in **Google Cloud Storage**, a cloud-based data lake solution. This provides a scalable and cost-effective way to store large datasets, ensuring they are easily accessible for further processing.

![](/images/dataset-in-gcs.png)

## Data Processing

In the **Data Processing Layer**, the raw data undergoes cleansing, transformation, and preparation. Using **Dataproc (PySpark)**, missing values are handled, duplicates are removed, and the data is transformed into a usable format. Afterward, the processed data is loaded into **BigQuery** for advanced analytics. The entire ETL workflow is automated and orchestrated by Google Cloud Composer.

![](/images/dataproc-job-info.png)

## Data Analytics

The **Data Analytics Layer** leverages **BigQuery** for fast querying and data analysis. BigQuery supports large-scale analytics, allowing for in-depth analysis of CO2 emissions data. The analytics layer also supports **BigQuery ML** for machine learning model development, enabling the prediction of future trends and uncovering insights related to the data, such as the impact of fuel type on CO2 emissions.


- Analytics with BigQuery
    ![](/images/bigquery-schema-preview.png)

- Modeling with BigQuery ML
    ![](/images/bigquery-ml-training.png)

## Data Visualization

Finally, the **Data Visualization Layer** uses **Looker Studio** to create interactive dashboards and reports, helping stakeholders visualize trends and insights derived from the analytics layer. For example, visualizations could display the vehicle models contributing to emissions trends or the impact of fuel consumption on CO2 emissions.

![](/images/visualization-with-looker-studio.png)


## Tools Used

| **Layer**         | **Tools**                 | **Uses**                                               |
|-------------------|---------------------------|-----------------------------------------------------------------|
| Orchestration     | Google Cloud Composer     | Automates task scheduling and coordinates data flow.            |
| Ingestion         | Cloud Functions           | Automates data collection from APIs, including Kaggle.           |
| Storage           | Google Cloud Storage      | Provides scalable, reliable, and cost-efficient data storage.   |
| Processing        | Dataproc, Cloud SQL       | Distributed data processing (PySpark) and relational data management. |
| Analytics         | BigQuery, BigQuery ML     | Fast querying, analytics, and machine learning capabilities.    |
| Visualization     | Looker Studio             | Creates interactive dashboards and reports for data insights.   |

## Conclusion

This Data LakeHouse Architecture Pipeline is a modern, cloud-native solution that seamlessly integrates data ingestion, processing, analytics, and visualization. By leveraging the power of Google Cloud services, this pipeline ensures a robust, scalable, and efficient data workflow, capable of handling large datasets and providing valuable insights through advanced analytics and interactive visualizations.


## Contribution

- Bong Hui Xin
- Legawatthi A/P Thiyagarajan
- Nitya A/P Ponnusamy
- Nurfatin Syahirah Binti Sukaimi
- Tan Yong Sheng

