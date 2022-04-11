import requests


base_url = 'https://storage.googleapis.com/datascience-public/data-eng-challenge/MOCK_DATA.json'

def get_datasets():
    res = requests.get(base_url)
    datasets = res.json()
    return datasets