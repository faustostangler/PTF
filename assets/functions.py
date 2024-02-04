import assets.helper as b3

import os
import sys
import datetime
import time
import winsound
import random

import pandas as pd

from google.cloud import storage
import json
import gzip
import io
import base64

import requests
from bs4 import BeautifulSoup

import unidecode
import string
import re

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# SYSTEM LOAD
def load_system(value):
	# load df_nsd
	df_nsd = load_nsd()

	# load df_acoes
	df_acoes = load_acoes(df_nsd)

	# database full
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

def beep(frequency=5000, duration=50):
    """
    Generate a system beep sound with the specified frequency and duration.

    Args:
        frequency (int): The frequency of the beep sound in Hertz (default is 5000 Hz).
        duration (int): The duration of the beep sound in milliseconds (default is 50 ms).

    Returns:
        bool: True if the beep was successful, False otherwise.
    """
    winsound.Beep(frequency, duration)
    return True

def remaining_time(start_time, size, i):
    """
    Calculate the remaining time for a process based on its progress.

    Args:
        start_time (float): The start time of the process in seconds.
        size (int): The total number of items in the process.
        i (int): The current index or progress of the process.

    Returns:
        str: A formatted string indicating the progress and remaining time.

    """
    # Calculate the number of remaining items
    counter = i + 1
    remaining_items = size - counter
    
    # Calculate the percentage of completion
    percentage = counter / size
    
    # Calculate the elapsed time
    running_time = time.time() - start_time
    
    # Calculate the average time taken per item
    avg_time_per_item = running_time / counter
    
    # Calculate the remaining time based on the average time per item
    remaining_time = remaining_items * avg_time_per_item
    
    # Convert remaining time to hours, minutes, and seconds
    hours, remainder = divmod(int(remaining_time), 3600)
    minutes, seconds = divmod(remainder, 60)
    
    # Format remaining time as a string
    remaining_time_formatted = f'{int(hours)}h {int(minutes):02}m {int(seconds):02}s'
    
    # Create a progress string with all the calculated values
    progress = (
        f'{percentage:.2%} '
        f'{counter}+{remaining_items}, '
        f'{avg_time_per_item:.6f}s per item, '
        f'Remaining: {remaining_time_formatted}'
    )

    beep()  # Function to generate a system beep

    return progress

def header_random():
    """
    Generate random HTTP headers for simulating different user agents, referers, and languages.

    Returns:
        dict: A dictionary containing randomly chosen 'User-Agent', 'Referer', and 'Accept-Language' headers.
    """
    # Randomly select a user agent, referer, and language from predefined lists
    user_agent = random.choice(b3.USER_AGENTS)
    referer = random.choice(b3.REFERERS)
    language = random.choice(b3.LANGUAGES)

    # Create a dictionary with the selected headers
    headers = {
        'User-Agent': user_agent,
        'Referer': referer,
        'Accept-Language': language
    }

    return headers


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

	return df

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
	source_blob_name = f'{df_name}.parquet' + 'google_error'

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

## Text Manipulation
def clean_text(text):
    """
    Cleans text by removing any leading/trailing white space, converting it to lowercase, removing
    accents, punctuation, and converting to uppercase.
    
    Args:
    text (str): The input text to clean.
    
    Returns:
    str: The cleaned text.
    """
    if not isinstance(text, str):
        try:
            text = str(text)
        except Exception as e:
            print(f"{text} is not convertible to string: {e}")
            return text

    # Remove accents, punctuation, and convert to uppercase
    text = unidecode.unidecode(text).translate(str.maketrans('', '', string.punctuation)).upper().strip()

    # Replace multiple spaces with a single space
    text = re.sub(r'\s+', ' ', text)

    return text

def word_to_remove(text):
    """
    Removes specified words from a text content.

    This function takes a text content (string) and removes specified words from it.
    The words to remove are defined in the 'words_to_remove' list.

    Args:
        text (str): The content of the text to be cleaned.

    Returns:
        str: The cleaned text content without the specified words.
    """
    pattern = '|'.join(map(re.escape, b3.nsd_words_to_remove))
    text = re.sub(pattern, '', text)
    return text


# B3

## NSD
def load_nsd():
	# nsd_local
	df_nsd = load_parquet('nsd')
	
	# update df_nsd
	gap = 0

	## get start and end points
	if not df_nsd.empty:
		df_nsd['envio'] = pd.to_datetime(df_nsd['envio'], dayfirst=True)
		start, end = nsd_range(df_nsd)
	else:
		start, end = 1, 100
		
	rows = []
	start_time = time.time()
	for i, n in enumerate(range(start, end)):
		# interrupt conditions
		last_date, limit_date, max_gap = nsd_dates(df_nsd)
		if last_date > limit_date:
			if gap == max_gap:
				break
		progress = remaining_time(start_time, end-start, i)
		try:
			# add nsd row to dataframe
			row = get_nsd(n)
			rows.append(row)
			print(n, progress, row[10], row[4], row[3], row[0])
			# reset gap
			gap = 0
		except Exception as e:
			# increase gap count
			gap += 1
			print(n, progress)

		# partial save
		if (end-start - i - 1) % b3.bin_size == 0:
			df_web = pd.DataFrame(rows, columns=b3.columns['nsd'])
			rows = []
			if not df_nsd.empty:
				df_nsd = pd.concat([df_nsd.dropna(), df_web.dropna()], ignore_index=True)
			else:
				df_nsd = df_web

			if end-start - i - 1 != 0:
				df_nsd = save_parquet(df_nsd, 'nsd', upload=False)
				print('partial save')
			else:
				df_nsd = save_parquet(df_nsd, 'nsd', upload=True)
				print('final save')

	return df_nsd

def nsd_range(df_nsd):
	"""
	Calculate the range for 'nsd' values based on data in the DataFrame.

	Args:
		df_nsd (pandas.DataFrame): DataFrame containing 'nsd' and 'envio' columns.

	Returns:
		tuple: A tuple containing the start and end values for the 'nsd' range.
	"""
	# Start
	try:
		start = int(max(df_nsd['nsd'].astype(int))) + 1
	except:
		start = 1

	# Calculate the gap in days from today to the max 'envio' date
	last_date = df_nsd['envio'].max().date()
	today = datetime.datetime.now().date()
	days_gap = (today - last_date).days

	# Group 'nsd' by day
	nsd_per_day = df_nsd.groupby(df_nsd['envio'].dt.date)['nsd'].count()

	# Find the average 'nsd' items per day group, and other statistics
	avg_nsd_per_day = nsd_per_day.mean()
	max_nsd_per_day = nsd_per_day.max()
	max_date_nsd_per_day = nsd_per_day.idxmax()

	# Calculate the expected items up to today
	expected_nsd = int(avg_nsd_per_day * (days_gap + 1) * b3.nsd_safety_factor)

	# End
	end = start + expected_nsd

	# Range
	start = start
	end = start + expected_nsd 

	print(f'NSD from {start} to {end}')

	return start, end

def nsd_dates(df_nsd):
	"""
	Calculate important dates and parameters related to 'nsd' values based on the DataFrame.

	Args:
		df_nsd (pandas.DataFrame): DataFrame containing 'nsd' and 'envio' columns.

	Returns:
		tuple: A tuple containing last_date, limit_date, and max_gap values.
	"""
	try:
		# Calculate the gap in days from today to max 'envio' date
		last_date = df_nsd['envio'].max().date()
		today = datetime.datetime.now().date()
		days_gap = (today - last_date).days

		# Find the maximum 'nsd' gap
		try:
			gap = df_nsd['nsd'].astype(int).diff().max()
		except Exception as e:
			gap = 1

		max_gap = int((gap + b3.safety_factor) * 0.1)

		# Group 'nsd' by day
		nsd_per_day = df_nsd.groupby(df_nsd['envio'].dt.date)['nsd'].count()

		# Find the average 'nsd' items per day group, and other statistics
		avg_nsd_per_day = nsd_per_day.mean()
		max_nsd_per_day = nsd_per_day.max()
		max_date_nsd_per_day = nsd_per_day.idxmax()

		# Calculate the last_date and previous safe date
		back_days = round(max_gap / avg_nsd_per_day)
		limit_date = datetime.datetime.now().date() - datetime.timedelta(days=back_days)

	except Exception as e:
		last_date, limit_date, max_gap = pd.to_datetime('1970-01-02').date(), datetime.datetime.now().date(), b3.max_gap

	return last_date, limit_date, max_gap

def get_nsd(nsd):
    """
    Extract information from a URL related to NSD (Número Sequencial do Documento), and RAD means Recebimento Automatizado de Documentos 

    Args:
        nsd (str): The NSD number to fetch information for.

    Returns:
        list: A list containing extracted information including company, dri, dri2, dre, data, versao, auditor,
              auditor_rt, cancelamento, protocolo, envio, url, and nsd.
    """
    # URL
    url = b3.url['nsd_pre'] + str(nsd) + b3.url['nsd_pos']

    # Getting the HTML content from the URL
    response = requests.get(url, headers=header_random())
    html_content = response.text

    # Parsing the HTML content with BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')

    # Extracting company
    nomeCompanhia_tag = soup.find('span', {'id': 'lblNomeCompanhia'})
    company = nomeCompanhia_tag.text.strip()
    company = unidecode.unidecode(company).upper()
    company = clean_text(company)
    company = word_to_remove(company)

    # Extracting dri and dri2
    nomeDRI_tag = soup.find('span', {'id': 'lblNomeDRI'})
    dri_info = nomeDRI_tag.text.strip().split(' - ')
    dri = dri_info[0]
    dri = unidecode.unidecode(dri).upper()
    dri2 = dri_info[-1].replace('(', '').replace(')', '')
    dri2 = unidecode.unidecode(dri2).upper()

    # Extracting 'FCA', data, and versao
    descricaoCategoria_tag = soup.find('span', {'id': 'lblDescricaoCategoria'})
    descricaoCategoria = descricaoCategoria_tag.text.strip()
    versao = descricaoCategoria.split(' - ')[-1]
    data = descricaoCategoria.split(' - ')[1]
    dre = descricaoCategoria.split(' - ')[0]
    dre = unidecode.unidecode(dre).upper()

    # Extracting auditor
    lblAuditor_tag = soup.find('span', {'id': 'lblAuditor'})
    auditor = lblAuditor_tag.text.strip().split(' - ')[0]
    auditor = unidecode.unidecode(auditor).upper()

    # Extracting auditor_rt
    lblResponsavelTecnico_tag = soup.find('span', {'id': 'lblResponsavelTecnico'})
    auditor_rt = lblResponsavelTecnico_tag.text.strip()
    auditor_rt = unidecode.unidecode(auditor_rt).upper()

    # Extracting protocolo
    lblProtocolo_tag = soup.find('span', {'id': 'lblProtocolo'})
    protocolo = lblProtocolo_tag.text.strip()

    # Extracting '2010' and envio
    lblDataDocumento_tag = soup.find('span', {'id': 'lblDataDocumento'})
    lblDataDocumento = lblDataDocumento_tag.text.strip()

    lblDataEnvio_tag = soup.find('span', {'id': 'lblDataEnvio'})
    envio = lblDataEnvio_tag.text.strip()
    envio = datetime.datetime.strptime(envio, "%d/%m/%Y %H:%M:%S")

    # Extracting cancelamento
    cancelamento_tag = soup.find('span', {'id': 'lblMotivoCancelamentoReapresentacao'})
    cancelamento = cancelamento_tag.text.strip()
    cancelamento = unidecode.unidecode(cancelamento).upper()

    data = [company, dri, dri2, dre, data, versao, auditor, auditor_rt, cancelamento, protocolo, envio, url, nsd]
    data = [clean_text(item) if item not in [company, envio, url] else item for item in data]

    return data

## ACOES
def load_acoes(df_nsd):
	df_acoes = load_parquet('acoes')

	df_nsd_filtered, df_acoes = clean_acoes(df_nsd, df_acoes)

	# Ordenar filtered_nsd



	return df_acoes

def clean_acoes(df_nsd, df_acoes):
    """
    Cleans and filters DataFrames containing NSD information and stock actions, retaining only the most recent documents per company and quarter.
    
    Args:
        df_nsd (pandas.DataFrame): Contains NSD information, including company, quarter (Trimestre), and document type (dre).
        df_acoes (pandas.DataFrame): Contains stock actions with document URLs and quarters.
        
    Returns:
        tuple of pandas.DataFrame: The first DataFrame contains cleaned and updated NSD information, and the second contains stock actions with only the most recent documents.
    """
    
    # Filter for relevant document types
    df_nsd_filtered = df_nsd[df_nsd['dre'].isin(b3.dfp)].copy()

    # Convert data types and remove duplicates
    df_nsd_filtered['nsd'] = df_nsd_filtered['nsd'].astype(int)
    df_nsd_filtered['Trimestre'] = pd.to_datetime(df_nsd_filtered['Trimestre'], format='%d%m%Y', errors='coerce')
    df_nsd_filtered.sort_values('nsd', ascending=True, inplace=True)
    df_nsd_filtered.drop_duplicates(['Companhia', 'Trimestre'], keep='last', inplace=True)

    # Identify latest document per company and quarter
    nsd_max = df_nsd_filtered.groupby(['Companhia', 'Trimestre'])['nsd'].max().reset_index()
    nsd_max['Trimestre'] = pd.to_datetime(nsd_max['Trimestre'], format='%d%m%Y', errors='coerce')
    nsd_max['nsd'] = nsd_max['nsd'].astype(int)

    df_nsd_filtered.sort_values(by=['Companhia', 'Trimestre'], ascending=[True, True], inplace=True)
	
    if not df_acoes.empty:
        # Extract NSD from URLs and convert 'Trimestre' to datetime
        df_acoes['nsd'] = df_acoes['URL'].apply(lambda x: int(x.split('Documento=')[-1].split('&')[0]))
        df_acoes['Trimestre'] = pd.to_datetime(df_acoes['Trimestre'], format='%d%m%Y', errors='coerce')

        # Combine rows based on Company and Quarter, retaining recent entries
        df_acoes_merged = pd.merge(df_acoes, nsd_max, on=['Companhia', 'Trimestre'], how='left', suffixes=('', '_max'))
        acoes_most_recent = df_acoes_merged[df_acoes_merged['nsd'] >= df_acoes_merged['nsd_max']].drop(columns=['nsd_max'])

        # List URLs from recent entries to remove from df_nsd_filtered
        urls_to_remove = acoes_most_recent['URL'].unique()
        df_nsd_filtered = df_nsd_filtered[~df_nsd_filtered['url'].isin(urls_to_remove)]

        # Drop temporary columns
        df_acoes.drop(columns=['nsd', 'nsd_max'], inplace=True, errors='ignore')

    return df_nsd_filtered, df_acoes


# OTHER TO ORGANIZE

def load_database(df=''):
	# 1. NSD # Número Sequencial de Documento

	return df