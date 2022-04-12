import requests


def get_datasets(url):
    res = requests.get(url)
    datasets = res.json()
    return datasets
