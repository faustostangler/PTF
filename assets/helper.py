import assets.functions as run

import os


# variables
dataset_folder = 'datasets'
columns = {}
columns['nsd'] = ['company', 'dri', 'dri2', 'dre', 'data', 'versao', 'auditor', 'auditor_rt', 'cancelamento', 'protocolo', 'envio', 'url', 'nsd']

dataset_path = os.curdir + '/' + dataset_folder
dataset_path = run.check_or_create_folder(dataset_path)

# Google GCS
json_key_file = 'credentials\storage admin.json'
bucket_name = 'b3_bovespa_bucket'
