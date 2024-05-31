import requests


def UsGet(stock):
    res = requests.get(
        url=f"https://im-aam.com/wp-json/us_data/v1/get_us_historical?stock={stock}")
    return res.json()


def IndoGet(stock):
    res = requests.get(
        url=f"https://im-aam.com/wp-json/us_data/v1/get_us_historical?stock={stock}")
    return res.json()