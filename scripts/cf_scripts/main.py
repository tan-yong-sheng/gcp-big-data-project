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