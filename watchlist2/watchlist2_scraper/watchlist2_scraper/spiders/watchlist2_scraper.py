from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import gspread, json


class WatchList2:

    def __init__(self):
        self.stocks = []

    def increase_date_by_1(self, date_):
        date_ = date_ + timedelta(days=1)
        return date_

    def decrease_date_by_1(self, date_):
        date_ = date_ - timedelta(days=1)
        return date_

    def return_stocks(self, driver, urls):
        stocks = []
        for url in urls:
            try:
                print(url)
                driver.get(url)
                driver.implicitly_wait(3)
                page_source = driver.page_source
                tables = pd.read_html(StringIO(page_source))[0]
                stocks = stocks + tables['Symbol'].to_list()
            except Exception as e:
                print(str(e))
                continue
        return stocks

    def earning_stocks_scrapper(self, no_of_stocks):
        options = Options()
        options.add_argument("--lang=en")
        options.add_argument("--headless=new")
        options.add_argument("--log-level=OFF")
        options.add_argument("--window-size=1920x1080")
        options.add_argument("--disable-gpu")
        driver = None
        date_today = datetime.today()
        start_date = date_today + timedelta(days=1)
        threshold = date_today + timedelta(days=30)
        urls = [
            f'https://finance.yahoo.com/calendar/earnings?from={start_date.strftime("%Y-%m-%d")}&to={date_today.strftime("%Y-%m-%d")}&day={date_today.strftime("%Y-%m-%d")}&offset={x}&size=100'
            for x in range(100, 600, 100)]
        urls.insert(0,
                    f'https://finance.yahoo.com/calendar/earnings?from={start_date.strftime("%Y-%m-%d")}&to={date_today.strftime("%Y-%m-%d")}&day={date_today.strftime("%Y-%m-%d")}')

        while len(self.stocks) <= no_of_stocks:
            try:
                driver = webdriver.Chrome(options=options)
                self.stocks = self.stocks + self.return_stocks(driver, urls)
                driver.quit()
                date_today = self.increase_date_by_1(date_today)
                urls = [
                    f'https://finance.yahoo.com/calendar/earnings?from={start_date.strftime("%Y-%m-%d")}&to={date_today.strftime("%Y-%m-%d")}&day={date_today.strftime("%Y-%m-%d")}&offset={x}&size=100'
                    for x in range(100, 600, 100)]
                urls.insert(0,
                            f'https://finance.yahoo.com/calendar/earnings?from={start_date.strftime("%Y-%m-%d")}&to={date_today.strftime("%Y-%m-%d")}&day={date_today.strftime("%Y-%m-%d")}')
                if date_today > threshold:
                    break
            except Exception as e:
                print(str(e))
                if driver:
                    driver.quit()
                break
        return self.stocks

    def get_stocks_from_earnings_date_sheet(self):
        final_data = []
        gc = gspread.service_account(filename='keys.json')
        with open('config.json', 'r') as config_file:
            config = json.load(config_file)
        er = gc.open_by_url(config['ed'])
        worksheet = er.worksheet('Release_dates')
        stocks_and_index = worksheet.get('B2:D15000')
        watchlist2 = gc.open_by_url(config['watchlist'])
        worksheet = watchlist2.worksheet('Stocks')
        worksheet.batch_clear(['A2:A2000'])
        row = len(worksheet.col_values(1)) + 1
        today_date_10 = datetime.today() + timedelta(days=10)
        for data in stocks_and_index:
            if len(data) > 2:
                index = data[0]
                stock = data[1]
                earning = data[2]
                print(stock)
                if earning != 'N/A':
                    earning = datetime.strptime(earning, '%d/%m/%y')
                    if today_date_10 > earning > datetime.today():
                        final_data.append({'range': f'A{row}', 'values': [[stock]]})
                        final_data.append({'range': f'B{row}', 'values': [[index]]})
                        final_data.append({'range': f'C{row}', 'values': [['US']]})
                        row += 1
        worksheet.batch_clear(['A2:C'])
        worksheet.batch_update(final_data)
        return final_data
