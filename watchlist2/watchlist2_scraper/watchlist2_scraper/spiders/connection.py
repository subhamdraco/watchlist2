import requests


def update(data):
    res = requests.post(
        url="https://im-aam.com/wp-json/us_data/v1/update_us_yahoo", data=data)

    return res.status_code


def getdata(stock):
    res = requests.get(
        url=f"https://im-aam.com/wp-json/us_data/v1/get_us_yahoo?stock={stock}")

    return res.json()


def insert(data):
    res = requests.post(
        url="https://im-aam.com/wp-json/us_data/v1/us_yahoo", data=data)

    return res.status_code


def insert_hist(data):
    res = requests.post(
        url="https://im-aam.com/wp-json/us_data/v1/us_histoical2", json=data)

    return res.status_code


def insert_invest(data):
    res = requests.post(
        url="https://im-aam.com/wp-json/us_data/v1/us_investing", json=data)
    return res.json()


def getdata_invest(stock):
    res = requests.get(
        url=f"https://im-aam.com/wp-json/us_data/v1/get_us_investing?stock={stock}")

    return res.json()
