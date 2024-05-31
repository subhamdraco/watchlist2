from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

from watchlist2_scraper import WatchList2
import json
import gspread


def index_scrapper():
    gc = gspread.service_account(filename='keys.json')
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)
    global_index = gc.open_by_url(config['watchlist'])
    worksheet = global_index.worksheet('Stocks')
    stock_list = worksheet.get('A2:A6000')
    stock_list = [s[0] for s in stock_list]
    options = Options()
    options.add_argument("--lang=en")
    options.add_argument("--headless=new")
    options.add_argument("--log-level=OFF")
    options.add_argument("--window-size=1920x1080")
    options.add_argument("--disable-gpu")
    data = []
    count = 2
    print('Scrape Started')
    for stock in stock_list:
        try:
            driver = webdriver.Chrome(options=options)
            driver.get(f'https://finance.yahoo.com/quote/{stock}')
            index = driver.find_element(By.XPATH,
                                        '//*[@id="nimbus-app"]/section/section/section/article/section[1]/div[1]/span/span[1]')
            index = index.text.split()[0]
            if index in ['NYSE'] or index.find('Nasdaq') != -1:
                index = index.replace('NasdaqGS', 'NASDAQ').replace('NasdaqGM', 'NASDAQ').replace('NasdaqCM', 'NASDAQ')
                data.append({'range': f'A{count}:B{count}', 'values': [[stock, index]]})
                print({'range': f'A{count}:B{count}', 'values': [[stock, index]]})
                count += 1
            driver.quit()
        except:
            continue
    worksheet.batch_clear(['A2:B2000'])
    worksheet.batch_update(data)


def runner():
    data = []
    gc = gspread.service_account(filename='keys.json')
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)
    global_index = gc.open_by_url(config['watchlist'])
    worksheet = global_index.worksheet('Stocks')
    w = WatchList2()
    stock_from_earning = w.get_stocks_from_earnings_date_sheet()
    print(f'Stocks from earning {len(stock_from_earning)}')
    if len(stock_from_earning) < 500:
        stocks = w.earning_stocks_scrapper(500-len(stock_from_earning))
        count = len(worksheet.col_values(1)) + 1
        for stock in stocks:
            data.append({'range': f'A{count}', 'values': [[stock]]})
            count += 1

        worksheet.batch_update(data)


runner()
index_scrapper()
