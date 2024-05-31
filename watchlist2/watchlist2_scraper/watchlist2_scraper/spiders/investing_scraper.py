import logging

import pandas as pd
import json
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from io import StringIO
from connection import insert_invest, getdata_invest


class InvestScrape:

    def __init__(self):
        self.urls = []
        f = open('urls.json')
        files = json.load(f)
        for entry in files:
            if entry['url']:
                if "?" in entry['url']:
                    balance_url = entry['url'].split("?")
                    balance_url = 'https://www.investing.com' + balance_url[0] + '-balance-sheet?' + balance_url[1]
                    cash_url = 'https://www.investing.com' + balance_url[0] + '-cash-flow?' + balance_url[1]
                    self.urls.append([balance_url, cash_url, entry['stock']])
                else:
                    self.urls.append([f"https://www.investing.com{entry['url']}-balance-sheet",
                                      f"https://www.investing.com{entry['url']}-cash-flow", entry['stock']])

    def scrape_invest_cash(self, url):
        final_data = []
        options = Options()
        options.add_argument("--lang=en")
        options.add_argument("--headless=new")
        options.add_argument("--log-level=OFF")
        options.add_argument("--window-size=1920x1080")
        options.add_argument("--disable-gpu")
        driver = webdriver.Chrome(options=options)
        driver.get(url)
        driver.implicitly_wait(2)
        page_source = driver.page_source
        tables = pd.read_html(StringIO(page_source))[1:5]
        for table in tables:
            final_data.append(table.columns.to_list())
            values_data = table.values.tolist()
            values_data = [s for s in values_data if
                           not 'Depreciation/Depletion ' in s[0] and 'Capital Expenditures ' not in s[0]]
            final_data = final_data + values_data
        driver.quit()
        return final_data

    def scrape_invest_balance(self, url):
        final_data = []
        options = Options()
        options.add_argument("--lang=en")
        options.add_argument("--headless=new")
        options.add_argument("--log-level=OFF")
        options.add_argument("--window-size=1920x1080")
        options.add_argument("--disable-gpu")
        driver = webdriver.Chrome(options=options)
        driver.get(url)
        driver.implicitly_wait(2)
        page_source = driver.page_source
        tables = pd.read_html(StringIO(page_source))[1:6]
        for table in tables:
            final_data.append(table.columns.to_list())
            values_data = table.values.tolist()
            values_data = [s for s in values_data if
                           not 'Cash and Short Term Investments ' in s[
                               0] and 'Property/Plant/Equipment, Total - Net ' not in s[
                               0] and 'Accounts Payable ' not in s[0] and 'Total Long Term Debt ' not in s[
                               0] and 'Redeemable Preferred Stock, Total ' not in s[0]]
            final_data = final_data + values_data
        driver.quit()
        return final_data

    def insert_in_db(self):
        final_data = []
        # balance
        for stock_data in self.urls:
            stock = stock_data[2]
            cash_url = stock_data[1]
            balance_url = stock_data[0]
            db_res = getdata_invest(stock)
            if db_res['result'] != 'No Stocks':
                if db_res['result'][0]['balance'] and db_res['result'][0]['cash']:
                    print(f'stock: {stock} present already')
                    logging.info(f'stock: {stock} already')
                    continue
            temp = []
            print(f'stock: {stock} starting to scrape')
            balance = self.scrape_invest_balance(balance_url)
            cash = self.scrape_invest_cash(cash_url)
            temp.append({
                'stocks': stock,
                'general': '',
                'fin_summary': '',
                'income': '',
                'balance': balance,
                'cash': cash,
                'dividends': '',
                'ratios': '',
                'earnings': ''
            })
            # res = ''
            res = insert_invest(data=temp)
            print(f'stock: {stock} inserted with {res}')
            logging.info(f'stock: {stock} inserted with {res}')


if __name__ == '__main__':
    i = InvestScrape()
    i.insert_in_db()
