import assets.helper as b3

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import os
import sys
import datetime
import time
import winsound
import random

import numpy as np
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

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.common.alert import Alert
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

from lxml import html
import zipfile

import multiprocessing
from multiprocessing import Pool

from urllib.parse import urljoin
session = requests.Session() # Inicializar uma sessão
visited_subfolders = set() # Conjunto para armazenar subpastas já visitadas
filelist = []

# SYSTEM LOAD
def load_system(value):
     
	# load df_companies
	df_companies = load_companies()

	# # load df_nsd
	# df_nsd = load_nsd()
	df_nsd = load_parquet('nsd')
	print('fast debug df_nsd')

	# # load df_rad
	df_rad = load_rad(df_nsd)
	# df_rad = load_parquet('rad')
	# print('fast debug df_rad')

	# Re-download any incomplete sheets in the newly grabbed data
	df_rad = regrab_web(df_rad)  # Assumes 'regrab_web' checks and re-downloads incomplete data

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

def load_browser(chromedriver_path='', download_directory=None, driver_wait_time=5):
	"""
	Launches chromedriver and creates a wait object.
	
	Parameters:
	- chromedriver_path (str): The path to the chromedriver executable.
	- driver_wait_time (int): The time to wait for elements to appear.
	
	Returns:
	tuple: A tuple containing a WebDriver instance and a WebDriverWait instance.
	"""
	chromedriver_path = b3.chromedriver_path

	try:
		# Define the options for the ChromeDriver.
		options = webdriver.ChromeOptions()
		if download_directory:
			options.add_experimental_option('prefs', {
				"download.default_directory": download_directory,
				"download.prompt_for_download": False,
				"download.directory_upgrade": True,
				"safebrowsing.enabled": True
			})
		# options.add_argument('--headless')  # Run in headless mode.
		options.add_argument('--no-sandbox')  # Avoid sandboxing.
		options.add_argument('--disable-dev-shm-usage')  # Disable shared memory usage.
		options.add_argument('--disable-blink-features=AutomationControlled')  # Disable automated control.
		options.add_argument('start-maximized')  # Maximize the window on startup.

		# Initialize the ChromeDriver.
		# driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
		# driver = webdriver.Chrome(executable_path=chromedriver_path, options=options)
		service = Service(executable_path=chromedriver_path)
		driver = webdriver.Chrome(service=service, options=options)

		# Define the exceptions to ignore during WebDriverWait.
		exceptions_ignore = (NoSuchElementException, StaleElementReferenceException)
		
		# Create a WebDriverWait instance for the driver, using the specified wait time and exceptions to ignore.
		wait = WebDriverWait(driver, driver_wait_time, ignored_exceptions=exceptions_ignore)
	except Exception as e:
		print(f"Error initializing browser: {str(e)}")
		return None, None
	
	# Return a tuple containing the driver and the wait object.
	return driver, wait

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
			df = pd.DataFrame(columns=b3.columns[df_name])  # Create an empty DataFrame as a fallback.

	print(f'load {df_name}')

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

	print(f'save {df_name}')

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
def wText(xpath: str, wait: WebDriverWait) -> str:
    """
    Finds and retrieves text from a web element using the provided xpath and wait object.
    
    Args:
    xpath (str): The xpath of the element to retrieve text from.
    wait (WebDriverWait): The wait object to use for finding the element.
    
    Returns:
    str: The text of the element, or an empty string if an exception occurs.
    """
    try:
        # Wait until the element is clickable, then retrieve its text.
        element = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
        text = element.text
        
        return text
    except Exception as e:
        # If an exception occurs, print the error message (if needed) and return an empty string.
        # print('wText', e)
        return ''

def wClick(xpath: str, wait: WebDriverWait) -> bool:
    """
    Finds and clicks on a web element using the provided xpath and wait object.
    
    Args:
    xpath (str): The xpath of the element to click.
    wait (WebDriverWait): The wait object to use for finding the element.
    
    Returns:
    bool: True if the element was found and clicked, False otherwise.
    """
    try:
        # Wait until the element is clickable, then click it.
        element = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
        element.click()
        return True
    except Exception as e:
        # If an exception occurs, print the error message (if needed) and return False.
        # print('wClick', e)
        return False

def wSelect(xpath: str, driver: webdriver.Chrome, wait: WebDriverWait) -> int:
    """
    Finds and selects a web element using the provided xpath and wait object.
    
    Args:
    xpath (str): The xpath of the element to select.
    driver (webdriver.Chrome): The Chrome driver object to use for selecting the element.
    wait (WebDriverWait): The wait object to use for finding the element.
    
    Returns:
    int: The value of the selected option, or an empty string if an exception occurs.
    """
    try:
        # Wait until the element is clickable, then click it.
        element = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
        element.click()
        
        # Get the Select object for the element, find the maximum option value, and select it.
        select = Select(driver.find_element(By.XPATH, xpath))
        options = [int(x.text) for x in select.options]
        batch = str(max(options))
        select.select_by_value(batch)
        
        return int(batch)
    except Exception as e:
        # If an exception occurs, print the error message (if needed) and return an empty string.
        # print('wSelect', e)
        return ''
   
def wSendKeys(xpath: str, keyword: str, wait: WebDriverWait) -> str:
    """
    Finds and sends keys to a web element using the provided xpath and wait object.
    
    Args:
    xpath (str): The xpath of the element to send keys to.
    keyword (str): The keyword to send to the element.
    wait (WebDriverWait): The wait object to use for finding the element.
    
    Returns:
    str: The keyword that was sent to the element, or an empty string if an exception occurs.
    """
    try:
        # Wait until the element is clickable, then send the keyword to it.
        input_element = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
        input_element.send_keys(keyword)
        
        return keyword
    except Exception as e:
        # If an exception occurs, print the error message (if needed) and return an empty string.
        # print('wSendKeys', e)
        return ''

def wRaw(xpath, wait):
  try:
    # Wait until the element is clickable, then retrieve its text.
    element = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
    raw_code = element.get_attribute("innerHTML")
    return raw_code
  except Exception as e:
    # If an exception occurs, print the error message (if needed) and return an empty string.
    # print('wText', e)
    return ''

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

## COMPANIES
def load_companies():
	try:
		# browser
		driver, wait = load_browser()
		driver.get(b3.url['b3_search'])

		# companies_local
		df_companies = load_parquet('companies')

		companies_tickers = grab_tickers(driver, wait)

		new_items = filter_new_companies(df_companies, companies_tickers)



		df_companies_web = grab_companies(driver, wait, companies_tickers)

		# Define columns and constants


	except Exception as e:
		pass

	return df_companies

def filter_new_companies(df_companies, companies_tickers):
    """
    Filters out companies from df_companies that are not listed in companies_tickers.

    Args:
    - df_companies (pd.DataFrame): DataFrame containing information about companies, expected to have a 'ticker' column.
    - companies_tickers (list): List of company tickers for comparison.

    Returns:
    - pd.DataFrame: A DataFrame containing companies from df_companies not found in companies_tickers.
    """
    try:
        # Ensure the 'ticker' column is in df_companies for comparison
        if 'ticker' not in df_companies.columns:
            raise ValueError("df_companies must contain a 'ticker' column.")

        # Filter df_companies to include only those not in companies_tickers
        new_companies_df = df_companies[~df_companies['ticker'].isin(companies_tickers)]

        # Optionally, reset index and select specific columns if needed
        new_companies_df.reset_index(drop=True, inplace=True)

    except Exception as e:
        # Handle any exceptions, such as missing 'ticker' column
        print(f"Error encountered: {e}")
        new_companies_df = pd.DataFrame(columns=df_companies.columns)

    return new_companies_df

def grab_companies(driver, wait, companies_tickers):
	try:
		pass

	except Exception as e:
		pass

	return df_companies_web

def grab_tickers(driver, wait):

	try:
		batch = wSelect(f'//*[@id="selectPage"]', driver, wait)
		companies = wText(f'//*[@id="divContainerIframeB3"]/form/div[1]/div/div/div[1]/p/span[1]', wait)
		companies = int(companies.replace('.',''))
		pages = int(companies/batch)
		wSelect(f'//*[@id="selectPage"]', driver, wait)

		value = f'found {companies} companies in {pages+1} pages'
		print(value)

		raw_code = []
		start_time = time.time()
		for i, page in enumerate(range(0, pages+1)):
			xpath = '//*[@id="nav-bloco"]/div'
			xpath = '//*[@id="nav-bloco"]/div'
			inner_html = wRaw(xpath, wait)
			raw_code.append(inner_html)
			wClick(f'//*[@id="listing_pagination"]/pagination-template/ul/li[10]/a', wait)
			time.sleep(0.5)
			value = f'page {page+1}'
			print(remaining_time(start_time, pages+1, i), value)

		companies_tickers = grab_ticker_keywords(raw_code)


	except Exception as e:
		pass

	return companies_tickers

def grab_ticker_keywords(raw_code):
  # Initialize a list to hold the keyword information
  keywords = []

  for inner_html in raw_code:
     # Parse the raw HTML source code
    soup = BeautifulSoup(inner_html, 'html.parser')

    # Find all the card elements
    cards = soup.find_all('div', class_='card-body')

    # Loop through each card element and extract the ticker and company name
    for card in cards:
      try:
        # Extract the ticker and company name from the card element
        ticker = clean_text(card.find('h5', class_='card-title2').text)
        company_name = clean_text(card.find('p', class_='card-title').text)
        pregao = clean_text(card.find('p', class_='card-text').text)
        listagem = clean_text(card.find('p', class_='card-nome').text)
        if listagem:
            for abbr, full_name in b3.abbreviations_dict.items():
                new_listagem = clean_text(listagem.replace(abbr, full_name))
                if new_listagem != listagem:
                    listagem = new_listagem
                    break  # Break out of the loop if a replacement was made

        # Append the ticker and company name to the keyword list
        keyword = [ticker, company_name, pregao, listagem]
        keywords.append(keyword)
        # print(keyword)
      except Exception as e:
        # print(e)
        pass


  df = pd.DataFrame(keywords, columns=b3.columns['tickers'])
  df.reset_index(drop=True, inplace=True)
  df.drop_duplicates(inplace=True)
  return df


## NSD
def load_nsd():
    """
    Loads NSD data from a local parquet file, updates it by fetching new NSD records,
    and saves the updated dataset back to a parquet file.

    Returns:
    - pd.DataFrame: The updated DataFrame containing NSD records.
    """
    # Load NSD data from a local parquet file
    df_nsd = load_parquet('nsd')
    
    # Initialize a variable to track gaps in data fetching
    gap = 0

    # Determine the start and end points for data fetching
    if not df_nsd.empty:
        df_nsd['envio'] = pd.to_datetime(df_nsd['envio'], dayfirst=True)
        df_nsd['trimestre'] = pd.to_datetime(df_nsd['trimestre'])
        start, end = nsd_range(df_nsd)  # Function to calculate start and end points
    else:
        start, end = 1, 100  # Default range if df_nsd is empty

    rows = []  # Temporary storage for fetched NSD rows
    start_time = time.time()  # Track the operation's start time for progress estimation

    # Fetch new NSD records within the determined range
    for i, n in enumerate(range(start, end)):
        # Check for interruption conditions based on dates and gap limit
        last_date, limit_date, max_gap = nsd_dates(df_nsd)  # Function to get relevant dates and max gap
        if last_date > limit_date and gap == max_gap:
            break
        
        # Display progress
        progress = remaining_time(start_time, end-start, i)
        
        try:
            # Fetch NSD record and append to the temporary list
            row = grab_nsd(n)  # Function to fetch NSD data
            rows.append(row)
            print(n, progress, row[10], row[5], row[1], row[0])
            gap = 0  # Reset gap counter after successful fetch
        except Exception as e:
            # Increment gap counter if fetching fails
            gap += 1
            print(n, progress)

        # Save fetched data periodically
        if (end-start - i - 1) % b3.bin_size == 0:
            df_web = pd.DataFrame(rows, columns=b3.columns['nsd'])
            df_web['trimestre'] = pd.to_datetime(df_web['trimestre'], format='%d%m%Y', errors='coerce')

            rows = []  # Reset temporary storage after saving
            df_nsd = pd.concat([df_nsd.dropna(), df_web.dropna()], ignore_index=True) if not df_nsd.empty else df_web
            df_nsd = save_parquet(df_nsd, 'nsd', upload=(end-start - i - 1 == 0))

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
	first_update = '1970-01-02'

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
		last_date, limit_date, max_gap = pd.to_datetime(first_update).date(), datetime.datetime.now().date(), b3.max_gap

	return last_date, limit_date, max_gap

def grab_nsd(nsd):
	"""
	Extract information from a URL related to NSD (Número Sequencial do Documento), and RAD means Recebimento Automatizado de Documentos 

	Args:
		nsd (str): The NSD number to fetch information for.

	Returns:
		list: A list containing extracted information including companhia, dri, dri2, dre, data, versao, auditor,
			  auditor_rt, cancelamento, protocolo, envio, url, and nsd.
	"""
	# URL
	url = b3.url['nsd_pre'] + str(nsd) + b3.url['nsd_pos']

	# Getting the HTML content from the URL
	response = requests.get(url, headers=header_random())
	html_content = response.text

	# Parsing the HTML content with BeautifulSoup
	soup = BeautifulSoup(html_content, 'html.parser')

	# Extracting companhia
	nomeCompanhia_tag = soup.find('span', {'id': 'lblNomeCompanhia'})
	companhia = nomeCompanhia_tag.text.strip()
	companhia = unidecode.unidecode(companhia).upper()
	companhia = clean_text(companhia)
	companhia = word_to_remove(companhia)

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
	trimestre = descricaoCategoria.split(' - ')[1]
	if len(trimestre) == 4:
		trimestre = '31-12-' + trimestre
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

	data = [companhia, trimestre, versao, dri, dri2, dre, auditor, auditor_rt, cancelamento, protocolo, envio, url, nsd]
	data = [clean_text(item) if item not in [companhia, envio, url] else item for item in data]

	return data

## RAD DATA
def load_rad(df_nsd, df_rad=''):
    """
    Load the RAD dataset, update it with new items based on NSD data, re-download incomplete sheets,
    and save the updated dataset. Handles exceptions silently.

    Args:
    - df_nsd (pd.DataFrame): DataFrame containing NSD (NumeroSequencialDocumento) data for new RAD items.
    - df_rad (pd.DataFrame): Optional initial RAD dataset. If not provided, attempts to load from a local source.

    Returns:
    - pd.DataFrame: The updated RAD dataset including both previously stored data and new items.
    """
    try:
        # Attempt to load the RAD dataset from a local source if df_rad is not provided
        if not isinstance(df_rad, pd.DataFrame) or df_rad.empty:
            df_rad = load_parquet('rad')  # Assumes 'load_parquet' loads a DataFrame from a local file named 'rad'

        # Filter out new RAD items that are not yet in the df_rad DataFrame
        new_items = filter_new_rad_items(df_nsd, df_rad)  # Assumes this function identifies new items to add

        # Download data for the new RAD items
        df_rad_web = grab_rad(df_rad, new_items)  # Assumes 'grab_rad' fetches RAD data for given items

        # Concatenate the original and new data, remove duplicates, then save the updated dataset
        df_rad = pd.concat([df_rad, df_rad_web], ignore_index=True).drop_duplicates()
        df_rad = save_parquet(df_rad, 'rad')  # Assumes 'save_parquet' saves the DataFrame and returns the updated DataFrame
    except Exception as e:
        # Exception handling could be logging or a simple pass; currently, it silently ignores errors
        pass

    return df_rad

def filter_new_rad_items(df_nsd, df_rad):
	"""
	Filters out new items in df_nsd that do not exist in df_rad based on NSD numbers extracted from URLs.

	Args:
		df_nsd (pd.DataFrame): DataFrame containing NSD documents with columns for Company, Quarter, and URL.
		df_rad (pd.DataFrame): DataFrame with RAD documents to compare against, including similar columns.

	Returns:
		pd.DataFrame: Filtered DataFrame containing new items in df_nsd that are not present in df_rad.
	"""
	# Define columns for comparison and output
	columns = ['companhia', 'trimestre']
	df_columns = columns + ['url']

	try:
		# Prepare df_nsd: Filter by document type, sort, drop duplicates, and reset index
		df_nsd_filtered = (df_nsd[df_nsd['dre'].isin(b3.dfp)]
						.sort_values(by=columns + ['url'], ascending=[True, True, True])
						.drop_duplicates(subset=columns, keep='last')
						.reset_index(drop=True))

		# Extract NSD numbers from URLs
		df_nsd_filtered['nsd'] = df_nsd_filtered['url'].str.extract('Documento=(\d+)').astype(int)
		df_rad['nsd'] = df_rad['url'].str.extract('Documento=(\d+)').astype(int)

		# Merge the dataframes to find new entries in df_nsd compared to df_rad
		df_nsd_filtered['trimestre'] = pd.to_datetime(df_nsd_filtered['trimestre'], format='%Y-%m-%d')
		df_rad['trimestre'] = pd.to_datetime(df_rad['trimestre'], format='%Y-%m-%d')
		merged_df = pd.merge(df_nsd_filtered, df_rad, on=columns, how='outer', suffixes=('', '_rad')).fillna(0)

		# Filter for rows where df_nsd's NSD number is greater than df_rad's, indicating newer documents
		df_new_items = merged_df[merged_df['nsd'] > merged_df['nsd_rad']]
		df_new_items = df_new_items.copy()

		# Convert the 'trimestre' column to datetime with day first format
		df_new_items['trimestre'] = pd.to_datetime(df_new_items['trimestre'], format='%Y-%m-%d')

		# Sort the DataFrame in ascending order based on the 'trimestre' column
		df_new_items = df_new_items.sort_values(by=['companhia', 'trimestre'], ascending=[True, True])

		# Reorder the columns as per your 'df_columns' list
		df_new_items = df_new_items[df_columns]

	except Exception as e:
		pass
		df_new_items = pd.DataFrame(columns=df_columns)

	return df_new_items


def grab_rad(df_rad, new_items):
	size = len(new_items)
	try:
		# Iniciar o processo de coleta de dados da web
		driver, wait = load_browser()

		df_rad_web = pd.DataFrame(columns=b3.columns['acoes'])
		rad_web = []
		# Processar cada linha em filtered_nsd para coletar dados acionários
		start_time = time.time()
		# Iterando sobre cada linha do DataFrame 'filtered_nsd'
		for j, (i, row) in enumerate(new_items.iterrows()):
			companhia = row['companhia']
			trimestre = row['trimestre']
			url = row['url']

			driver.get(url)

			acoes = grab_acoes(driver, wait, companhia, trimestre, url)
			df_ind = grab_demo_fin(driver, wait, companhia, trimestre, url, "DFs Individuais")
			df_con = grab_demo_fin(driver, wait, companhia, trimestre, url, "DFs Consolidadas")

			rad_web.extend([acoes, df_ind, df_con])

			print(remaining_time(start_time, size, j), companhia, trimestre, )

			# partial save
			if (size - i - 1) % (b3.bin_size) == 0:
				df_web = pd.concat(rad_web, ignore_index=True)
				df_rad_web = pd.concat([df_rad_web, df_web], ignore_index=True)
				rad_web = []

				df_rad = pd.concat([df_rad, df_rad_web], ignore_index=True).drop_duplicates()[b3.columns['rad']]
				df_rad = df_rad.sort_values(by=['companhia', 'trimestre'], ascending=[True, True])

				if size - i - 1 == 0:
					df_rad = save_parquet(df_rad, 'rad', upload=True)
				else:
					df_rad = save_parquet(df_rad, 'rad', upload=False)

	except Exception as e:
		print('## Será que DF Individuais ou DF Consolidadas não existiram?? Ajuste as try except')
		pass

	df_rad = pd.concat([df_rad, df_rad_web], ignore_index=True).drop_duplicates()[b3.columns['rad']]
	df_rad = df_rad.sort_values(by=['companhia', 'trimestre'], ascending=[True, True])
	df_rad = save_parquet(df_rad, 'rad', upload=True)
	return df_rad

def regrab_web(df_rad_web):
    """
    Identifies incomplete sheets in the given DataFrame, removes those entries, and then re-downloads 
    the data for the missing 'companhia-trimestre' pairs to ensure completeness of the dataset.

    Args:
    - df_rad_web (pd.DataFrame): DataFrame containing RAD web data with columns including 'companhia',
      'trimestre', 'demo_det', and 'url'.

    Returns:
    - pd.DataFrame: Updated DataFrame with re-downloaded data for previously identified incomplete sheets.
    """
    # Step 1: Identify missing (incomplete) sheets based on 'companhia' and 'trimestre'
    missing_sheets = find_incomplete_sheets(df_rad_web)

    # Step 2: Remove entries identified as incomplete
    df_rad_complete, missing_items = remove_missing_items(df_rad_web, missing_sheets)

    # Step 3: Re-download the data for missing 'companhia-trimestre' pairs
    # Note: Implementation of grab_rad() function is assumed and needs to be defined based on specific data fetching logic
    df_rad_web_updated = grab_rad(df_rad_complete, missing_items)
    
    return df_rad_web_updated

def find_incomplete_sheets(df_rad_web):
	"""
	Identify the 'companhia' and 'trimestre' pairs in df_rad_web that have fewer unique 'demo_det' entries
	than the maximum found across all such pairs, suggesting these are incomplete.

	Args:
	- df_rad_web (pd.DataFrame): DataFrame containing web data, expected to have at least 'companhia',
		'trimestre', and 'demo_det' columns.

	Returns:
	- pd.DataFrame: A DataFrame with rows for each 'companhia' and 'trimestre' pair that has fewer
		unique 'demo_det' entries than the maximum found, indicating potential incompleteness.
	"""
	# Group by 'companhia' and 'trimestre', then count unique 'demo_det' values for each group
	demo_det_sheets = df_rad_web.groupby(['companhia', 'trimestre'])[['demo_det']].nunique().reset_index()

	# Create a mask to identify groups with fewer unique 'demo_det' values than the max in the dataset
	mask = demo_det_sheets['demo_det'] < demo_det_sheets['demo_det'].max()

	# Apply the mask to filter and return the potentially incomplete sheets
	missing_sheets = demo_det_sheets[mask].reset_index(drop=True)

	return missing_sheets

def remove_missing_items(df_rad_web, missing_sheets):
	"""
	Remove entries from df_rad_web that match the missing_sheets based on 'companhia' and 'trimestre',
	and prepare a dataframe of missing items with extracted NSD values sorted by 'companhia' and 'trimestre'.

	Args:
	- df_rad_web (pd.DataFrame): Original dataframe containing web data with columns including 'companhia', 'trimestre', and 'url'.
	- missing_sheets (pd.DataFrame): Dataframe indicating which sheets are missing, with columns 'companhia' and 'trimestre'.

	Returns:
	- pd.DataFrame: df_rad_complete, the dataframe after removing entries matching the missing sheets.
	- pd.DataFrame: missing_df, a dataframe of missing sheets with extracted NSD values from URLs, sorted and without duplicates.
	"""
	# Define the key columns for merging and the columns to retain in the missing_df
	columns = ['companhia', 'trimestre']
	df_columns = columns + ['url']

	# Merge the original dataframe with the missing sheets dataframe to flag the rows
	merged_df = pd.merge(left=df_rad_web, right=missing_sheets, on=columns, how='outer', indicator=True)

	# Filter out the complete data (not matching with missing sheets)
	df_rad_complete = merged_df[merged_df['_merge'] == 'left_only'].copy()

	# Filter the data that is present in both dataframes (i.e., the missing sheets)
	missing_df = merged_df[merged_df['_merge'] == 'both'].copy()

	# Apply the function to extract NSD values from URLs
	missing_df['nsd'] = missing_df['url'].str.extract('Documento=(\d+)').astype(int)

	# Sort the missing sheets dataframe by 'companhia' and 'trimestre' for readability and analysis
	missing_df = missing_df.sort_values(by=columns, ascending=[True, True])

	# Select relevant columns and remove duplicates, then reset the index for clean formatting
	missing_df = missing_df[df_columns].drop_duplicates().reset_index(drop=True)

	return df_rad_complete, missing_df

def grab_acoes(driver, wait, companhia, trimestre, url):
	"""
	Extracts stock action data from a given URL using Selenium.

	Args:
		driver (webdriver): Selenium webdriver instance.
		wait (WebDriverWait): WebDriverWait instance for handling dynamic content.
		url (str): URL to navigate to and extract data from.

	Returns:
		list: Extracted stock action data including ON, PN, ON in treasury, PN in treasury, unit, and URL.
			  Returns a list with zeros and 'UNIDADE' if an error occurs.
	"""
	try:
		# Defina suas variáveis
		stock_mkt = 'Ações'
		stock_tes = 'Ações em Tesouraria'
		stock_on = 'Ações ON'
		stock_pn = 'Ações PN'

		# Wait for and interact with dropdown menu
		select_element = wait.until(EC.element_to_be_clickable((By.XPATH, "//*[@id='cmbGrupo']")))
		Select(select_element).select_by_visible_text("Dados da Empresa")

		# Switch to the relevant iframe to access the table
		iframe_element = driver.find_element(By.ID, "iFrameFormulariosFilho")
		driver.switch_to.frame(iframe_element)

		# Locate and read the table into a DataFrame
		table = driver.find_element(By.XPATH, "//*[@id='UltimaTabela']/table")
		dados = pd.read_html(table.get_attribute('outerHTML'))[0]

		# Parsing and converting table data
		on = pd.to_numeric(dados.iloc[2, 1].replace('.', '').replace(',', ''), errors='coerce')
		pn = pd.to_numeric(dados.iloc[3, 1].replace('.', '').replace(',', ''), errors='coerce')
		on_tes = pd.to_numeric(dados.iloc[6, 1].replace('.', '').replace(',', ''), errors='coerce')
		pn_tes = pd.to_numeric(dados.iloc[7, 1].replace('.', '').replace(',', ''), errors='coerce')

		# Extracting unit from table header
		unidade = 'UNIDADE'
		match = re.search(r'\((.*?)\)', dados.iloc[0, 0])
		if match:
			unidade = match.group(1).upper()

		# Dados como lista de tuplas, utilizando as variáveis
		dados = [
			(companhia, trimestre, b3.conta_acoes[0][0], b3.conta_acoes[0][1], on, unidade, stock_mkt, stock_on, url),
			(companhia, trimestre, b3.conta_acoes[1][0], b3.conta_acoes[1][1], pn, unidade, stock_mkt, stock_pn, url),
			(companhia, trimestre, b3.conta_acoes[2][0], b3.conta_acoes[2][1], on_tes, unidade, stock_tes, stock_on, url),
			(companhia, trimestre, b3.conta_acoes[3][0], b3.conta_acoes[3][1],  pn_tes, unidade, stock_tes, stock_pn, url),
		]

		# Criação do DataFrame
		acoes = pd.DataFrame(dados, columns=b3.columns['acoes'])

	except Exception as e:
		acoes = pd.DataFrame(columns=b3.columns['acoes'])
	finally:
		# Ensure driver switches back to the default content
		driver.switch_to.default_content()

	return acoes

def grab_demo_fin(driver, wait, companhia, trimestre, url, df):
    """
    Automates data extraction for a specified financial demonstration from a web page using Selenium.

    Args:
    - driver: Selenium WebDriver, the browser instance.
    - wait: WebDriverWait object for handling dynamic content loading.
    - companhia (str): The name of the company for which data is being fetched.
    - trimestre (str): The reporting period (quarter) for the data.
    - url (str): The URL of the page from which data is being fetched.
    - df (str): The type of financial demonstration to fetch, e.g., "DFs Individuais".

    Returns:
    - pd.DataFrame: A DataFrame containing the fetched data; returns an empty DataFrame if an error occurs.
    """
    try:
        # Define financial demonstrations to exclude from the extraction process
        options_to_remove = ['Demonstração das Mutações do Patrimônio Líquido']

        # Wait and select the desired financial demonstration type from the dropdown
        select_element = wait.until(EC.element_to_be_clickable((By.XPATH, "//*[@id='cmbGrupo']")))
        Select(select_element).select_by_visible_text(df)

        # Wait for the second dropdown to become clickable and interact with it
        select_element = wait.until(EC.element_to_be_clickable((By.XPATH, "//*[@id='cmbQuadro']")))
        select = Select(select_element)
        
        # Filter out options to remove and compile a list of demonstrations to process
        options = select.options
        option_texts = [option.text for option in options if option.text not in options_to_remove]

        # Collect data for each available demonstration option
        dados = []
        for option in option_texts:
            try:
                select_element = wait.until(EC.element_to_be_clickable((By.XPATH, "//*[@id='cmbQuadro']")))
                Select(select_element).select_by_visible_text(option)

                # Grab detailed demonstration data for each option
                partial = grab_demo_det(driver, wait, companhia, trimestre, url, option, df)
                dados.append(partial)
            except Exception as inner_exception:
                # Log or handle specific demonstration option fetch error
                pass
        
        # Concatenate all fetched data into a single DataFrame
        dfs = pd.concat(dados, ignore_index=True)  # Resets index in the concatenated DataFrame
    except Exception as outer_exception:
        # Handle any errors that occur during the fetch process, returning an empty DataFrame
        dfs = pd.DataFrame(columns=b3.columns['acoes'])

    return dfs

def grab_demo_det(driver, wait, companhia, trimestre, url, option, df):
    """
    Fetches detailed financial data for a given company and quarter from a web page, based on a specific demonstration option.

    Args:
    - driver: Selenium WebDriver instance for browser automation.
    - wait: WebDriverWait object for managing dynamic content.
    - companhia (str): Name of the company.
    - trimestre (str): Reporting quarter.
    - url (str): URL of the financial report page.
    - option (str): The specific financial demonstration option to fetch.
    - df (str): The type of financial report (e.g., "DFs Individuais").

    Returns:
    - pd.DataFrame: A DataFrame containing the extracted data for the specified financial demonstration option.
    """
    try:
        # Access the iframe containing the financial data table
        iframe_element = driver.find_element(By.ID, "iFrameFormulariosFilho")
        driver.switch_to.frame(iframe_element)

        # Locate the data table and convert it into a DataFrame
        table_element = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="ctl00_cphPopUp_tbDados"]')))
        dados = pd.read_html(table_element.get_attribute('outerHTML'), skiprows=[0])[0]
        
        # Keep only the first three columns and rename them according to predefined schema
        dados = dados.iloc[:, :3]
        dados.columns = b3.columns['acoes'][2:5]

        # Clean and format data columns
        dados['conta'] = dados['conta'].astype(str)
        dados['valor'] = dados['valor'].fillna(0).astype(int)
        dados['companhia'] = companhia
        dados['trimestre'] = trimestre
        dados['url'] = url

        # Extract measurement unit from the table title and adjust the 'valor' column accordingly
        unidade_element_text = driver.find_element(By.XPATH, '//*[@id="TituloTabelaSemBorda"]').text
        unidade = re.search(r'\((.*?)\)', unidade_element_text).group(1) if re.search(r'\((.*?)\)', unidade_element_text) else 'UNIDADE'
        dados['unidade'] = unidade

        # Apply conversion if 'mil' is present in the unit
        dados['valor'] = dados.apply(lambda row: row['valor'] * 1000 if 'Mil' in row['unidade'] else row['valor'], axis=1)
        dados['unidade'] = dados['unidade'].replace({'.*Mil.*': 'UNIDADE'}, regex=True)

        # Assign the demonstration option and financial report type to the DataFrame
        dados['demo_det'] = option
        dados['demo_fin'] = df

    except Exception as e:
        # Return an empty DataFrame with predefined columns in case of an error
        dados = pd.DataFrame(columns=b3.columns['acoes'])

    finally:
        # Revert to the main content of the page after processing the iframe
        driver.switch_to.default_content()

    return dados

# OTHER TO ORGANIZE

def load_database(df=''):
	# 1. NSD # Número Sequencial de Documento

	return df