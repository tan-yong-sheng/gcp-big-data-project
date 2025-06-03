# Part 1 - Data Ingestion Layer : Set up Cloud Function to scrape Kaggle data
Setting Up a Cloud Function for Kaggle Data Pipeline
----------------------------------------------------

This guide outlines the steps to create a Cloud Function that downloads a dataset from Kaggle, processes it, and uploads it to Google Cloud Storage.

Prerequisites
-------------

1.  Google Cloud Platform account with billing enabled
2.  Kaggle account with API access
3.  Google Cloud Shell

Step 1: Environment Setup
-------------------------

The commands are executed under the cloud shell terminal…

![](../images/Part%201%20-%20Data%20Ingestion%20Layer.jpg)

First, set up the necessary environment variables in Cloud Shell:

```bash
> export PROJECT_ID=steam-link-443214-f2
> export REGION=us-central1
> export BUCKET=dataproc-composer
> export STAGING_BUCKET=dataproc-composer-staging
> export SERVICE_ACCOUNT_EMAIL=552356513533-compute@developer.gserviceaccount.com
```

Step 2: Configure Kaggle Authentication
---------------------------------------

*   Go to [Kaggle Settings](https://www.kaggle.com/settings)
*   Click on "Create New API Token" to download kaggle.json

![](../images/1_Part%201%20-%20Data%20Ingestion%20Layer.jpg)

*   Create a new kaggle.json file in Cloud Shell

```bash
> nano kaggle.json
```

And paste all the contents in `kaggle.json` file downloaded from Kaggle website into the `kaggle.json` file in the Cloud Shell

![](../images/2_Part%201%20-%20Data%20Ingestion%20Layer.jpg)

Step 3: Enable Required Google Cloud APIs
-----------------------------------------

```bash
> gcloud services enable \
  storage.googleapis.com \
  secretmanager.googleapis.com \
  cloudfunctions.googleapis.com \
  cloudbuild.googleapis.com \
  run.googleapis.com \
  logging.googleapis.com
```

Step 4: Set Up Secret Manager
-----------------------------

*   Create a secret for Kaggle credentials

```bash
> gcloud secrets create kaggle-json --data-file=kaggle.json
```

*   Grant access to the service account:

```bash
> gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member=serviceAccount:${SERVICE_ACCOUNT_EMAIL} \
  --role=roles/secretmanager.secretAccessor
```

Step 5: Create Cloud Function Files
-----------------------------------

*   Create a directory for the Cloud Function

```bash
> mkdir -p cf_scripts
```

*   Create requirements.txt

```bash
> nano cf_scripts/requirements.txt
```

*   Add the following dependencies:

```
functions-framework==3.*
google-cloud-secret-manager
google-cloud-storage
kaggle
flask
pandas
```

*   Create main.py:

```bash
> nano cf_scripts/main.py
```

*   Add the Python code for the Cloud Function (as below)

```python
import os
import zipfile
import pandas as pd
from google.cloud import secretmanager
from google.cloud import storage
import json
import functions_framework
from flask import jsonify

def get_secret(secret_name):
    client = secretmanager.SecretManagerServiceClient()
    project_id = os.getenv('PROJECT_ID')
    secret_name_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(name=secret_name_path)
    return response.payload.data.decode("UTF-8")

@functions_framework.http
def download_and_upload(request):
    try:
        # Validate environment variables
        bucket_name = os.getenv('BUCKET')
        if not bucket_name:
            return jsonify({'error': 'BUCKET environment variable not set'}), 500

        dataset_name = 'karomatovdovudkhon/co2-emissions-canada'
        local_zip_path = '/tmp/co2_emissions_canada.zip'
        csv_filename = 'CO2 Emissions_Canada.csv'
        local_csv_path = os.path.join('/tmp', csv_filename)
        gcs_path = 'dataset/co2_emissions_canada.csv'

        # Define new header names
        new_headers = [
            "make", "model", "vehicle_class", "engine_size", "cylinders", "transmission",
            "fuel_type", "fuel_consumption_city", "fuel_consumption_hwy", "fuel_consumption_comb_Lkm", 
            "fuel_consumption_comb_mpg", "co2_emissions"
        ]

        # Get Kaggle credentials
        try:
            kaggle_creds = json.loads(get_secret("kaggle-json"))
            os.environ['KAGGLE_USERNAME'] = kaggle_creds['username']
            os.environ['KAGGLE_KEY'] = kaggle_creds['key']
            
            # Import kaggle after setting credentials
            import kaggle
            
        except Exception as e:
            return jsonify({'error': f'Failed to get Kaggle credentials: {str(e)}'}), 500

        # Download from Kaggle
        try:
            print("Starting dataset download...")
            kaggle.api.dataset_download_files(dataset_name, path='/tmp', unzip=True)
            print("Dataset downloaded successfully")

            # Check if the CSV file exists after extraction
            if not os.path.exists(local_csv_path):
                return jsonify({'error': f'{csv_filename} not found in {local_zip_path} after extraction'}), 500

            print("Files in /tmp:", os.listdir('/tmp'))  # List files in /tmp for debugging

            # Read the CSV and update headers
            df = pd.read_csv(local_csv_path)

            # Check if the CSV columns exist
            if len(df.columns) != len(new_headers):
                return jsonify({'error': f"CSV file has {len(df.columns)} columns, expected {len(new_headers)}"}), 500

            # Rename the columns
            df.columns = new_headers

            # Save the modified CSV
            df.to_csv(local_csv_path, index=False)
            print(f"Headers updated in {local_csv_path}")

        except Exception as e:
            return jsonify({'error': f'Failed to download or modify the CSV: {str(e)}'}), 500

        # Upload to GCS
        try:
            client = storage.Client()
            bucket = client.get_bucket(bucket_name)
            blob = bucket.blob(gcs_path)

            # Upload the modified CSV file
            blob.upload_from_filename(local_csv_path)
            print(f"Dataset uploaded to gs://{bucket_name}/{gcs_path}")
        except Exception as e:
            return jsonify({'error': f'Failed to upload to GCS: {str(e)}'}), 500

        return jsonify({
            'status': 'success',
            'message': 'Download, modification, and upload completed successfully',
            'destination': f'gs://{bucket_name}/{gcs_path}'
        }), 200

    except Exception as e:
        return jsonify({'error': f'Unexpected error: {str(e)}'}), 500

```

Step 6: Deploy the Cloud Function
---------------------------------

*   Deploy the function with the following configuration:

```bash
gcloud functions deploy download_and_upload \
  --runtime python310 \
  --trigger-http \
  --allow-unauthenticated \
  --region ${REGION} \
  --source ./cf_scripts \
  --set-env-vars PROJECT_ID=${PROJECT_ID},BUCKET=${BUCKET} \
  --timeout=540s \
  --memory=1024MB
```

After waiting for around 30s - 1 min, our script will be deployed to the Cloud Function, as follows:

![](/images/Cloud-function-setup.png)

![](/images/code-in-cloud-function.png)


Step 7: Test the Cloud Function
-------------------------------

```bash
> curl "https://${REGION}-${PROJECT_ID}.cloudfunctions.net/download_and_upload?bucket-name=${BUCKET}"
```

If you open the link directly in your browser, you will see this output:

![](../images/4_Part%201%20-%20Data%20Ingestion%20Layer.jpg)

And when you check your Google Cloud Storage, you will see the data will be downloaded:

![](../images/5_Part%201%20-%20Data%20Ingestion%20Layer.jpg)

Function Details
----------------

The Cloud Function performs the following operations:

1.  Authenticates with Kaggle using stored credentials
2.  Downloads the specified dataset
3.  Updates the CSV headers to standardized names
4.  Uploads the processed file to Google Cloud Storage


Monitoring
----------

You can monitor the function's execution using Cloud Logging:

```bash
> gcloud functions logs read download_and_upload
```

Reference
---------

*   Getting started with Cloud Functions (2nd gen) [https://codelabs.developers.google.com/codelabs/cloud-starting-cloudfunctions-v2#1](https://codelabs.developers.google.com/codelabs/cloud-starting-cloudfunctions-v2#1)
