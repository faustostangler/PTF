import assets.helper as b3

import os

import pandas as pd

from google.cloud import storage
import json
import gzip
import io
import base64

# SYSTEM LOAD
def load_system(value):
    df_nsd = load_parquet('nsd')
    df_nsd = save_parquet(df_nsd, 'nsd', upload=True)

    df = load_database()


    return value


# SYSTEM WIDE
def check_or_create_folder(folder):
    """
    Check if the given folder exists, and create it if it doesn't.

    Args:
    folder (str): the path of the folder to be checked/created.

    Returns:
    str: the path of the folder.
    """
    try:
        if not os.path.exists(folder): 
            os.makedirs(folder)  # create folder if it doesn't exist
    except Exception as e:
        print('Error occurred while creating folder:', e)
    return folder

## File Management
def load_parquet(df_name):
    """
    Load a pandas DataFrame from a Parquet file, or create an empty DataFrame if the file doesn't exist.

    Args:
    df_name (str): The name of the file (without the extension) to load/create.

    Returns:
    df (DataFrame): The DataFrame loaded from the file, or an empty DataFrame if the file doesn't exist.
    """
    # Construct the full path to the file using the 'dataset_path'.
    filepath = os.path.join(b3.dataset_path, f'{df_name}.parquet')

    # Attempt to download from Google Cloud Storage and save locally.
    try:
        df = download_from_gcs(df_name)
    except Exception as e:
        try:
            df = pd.read_parquet(filepath)  # Try to read the file as a Parquet file.
            df = upload_to_gcs(df, df_name)  # Upload to Google Cloud Storage if successful.
        except Exception as e:
            df = pd.DataFrame(columns=b3.columns['nsd'])  # Create an empty DataFrame as a fallback.

    return df

def save_parquet(df, df_name, upload=True):
    """
    Save a pandas DataFrame as a Parquet file and optionally upload it to Google Cloud Storage.

    Args:
    df (DataFrame): The DataFrame to be saved.
    df_name (str): The name of the file (without the extension) to save.
    upload (bool): Whether to upload the file to Google Cloud Storage (default is True).

    Returns:
    df (DataFrame): The original DataFrame.
    """
    try:
        df.to_parquet(f'{b3.dataset_path}/{df_name}.parquet')  # Save as a Parquet file locally.
        if upload:
            df = upload_to_gcs(df, df_name)  # Upload to Google Cloud Storage if specified.
    except Exception as e:
        pass

    return df

### Storage Google Cloud GCS
def upload_to_gcs(df, df_name):
    """
    Uploads a pandas DataFrame to Google Cloud Storage (GCS) as a Parquet file.

    Args:
        df (pandas.DataFrame): The DataFrame to be uploaded.
        df_name (str): The name to be used for the uploaded file (excluding the '.parquet' extension).

    Returns:
        None: The function doesn't return anything but uploads the file to GCS.

    Raises:
        google.auth.exceptions.DefaultCredentialsError: If no valid credentials are found.
        google.api_core.exceptions.NotFound: If the specified bucket does not exist.
        google.cloud.exceptions.GoogleCloudError: If there was an error during the upload operation.
    """
    try:
        # GCS configuration
        destination_blob_name = f'{df_name}.parquet'

        # Initialize GCS client
        client = storage.Client.from_service_account_json(b3.json_key_file)
        bucket = client.get_bucket(b3.bucket_name)

        # Save DataFrame to a bytes buffer as a Parquet file
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        # Upload the buffer to GCS
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_file(buffer, content_type='application/octet-stream')
        
    except Exception as e:
        pass

def download_from_gcs(df_name):
    """
    Downloads a Parquet file from Google Cloud Storage (GCS) and returns its contents as a pandas DataFrame.

    Args:
        df_name (str): The name of the file to download (excluding the '.parquet' extension).

    Returns:
        pandas.DataFrame: The contents of the downloaded file as a DataFrame.

    Raises:
        google.auth.exceptions.DefaultCredentialsError: If no valid credentials are found.
        google.api_core.exceptions.NotFound: If the specified bucket does not exist.
        google.cloud.exceptions.GoogleCloudError: If there was an error during the download operation.
        ValueError: If the downloaded file cannot be read as a pandas DataFrame.
    """
    # GCS configuration
    source_blob_name = f'{df_name}.parquet'

    # Initialize GCS client
    client = storage.Client.from_service_account_json(b3.json_key_file)
    bucket = client.get_bucket(b3.bucket_name)

    # Download the DataFrame from GCS to a bytes buffer
    buffer = io.BytesIO()
    blob = bucket.blob(source_blob_name)
    blob.download_to_file(buffer)
    buffer.seek(0)

    # Load the Parquet DataFrame into a Pandas DataFrame
    df = pd.read_parquet(buffer)

    df = save_parquet(df, df_name, upload=False)

    return df

# B3
def load_nsd(df_nsd=''):


    return df_nsd


def load_database(df=''):
    # 1. NSD # Número Sequencial de Documento

    return df