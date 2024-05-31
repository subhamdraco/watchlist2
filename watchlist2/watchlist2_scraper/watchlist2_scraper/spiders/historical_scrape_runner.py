import yfinance as yf
from google.oauth2 import service_account
import gspread
import logging
import json
import pandas as pd
from connection import insert_hist


class Historical:

    def __init__(self):
        SERVICE_ACCOUNT_FILE = 'keys.json'

        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        # creds = None
        self.cred = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

        with open('config.json', 'r') as config_file:
            self.config = json.load(config_file)

        logging.basicConfig(
            level=logging.INFO,  # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            format="%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            filename="app.log",  # Log messages will be written to this file
            filemode="w"  # 'w' for overwrite, 'a' for append
        )

    def connect_to_gs(self, sample_iD):
        creds = self.cred
        client = gspread.authorize(creds)
        sheet = client.open_by_url(sample_iD)
        return sheet

    def get_historical(self, stock):
        tick = yf.Ticker(stock)
        hist_df = tick.history(period="max")
        hist_df.reset_index(inplace=True)
        hist_df = hist_df.fillna('')
        hist_df['Date'] = pd.to_datetime(hist_df['Date'], format='mixed')
        return hist_df.values.tolist()

    def insert_in_db(self, stock):
        df = self.get_historical(stock)
        temp = []
        for row in df:
            data = {"stock": stock,
                    "date": row[0].strftime('%Y-%m-%d'),
                    "open": str(row[1]).lower().replace('nan', '-'),
                    "high": str(row[2]).lower().replace('nan', '-'),
                    "low": str(row[3]).lower().replace('nan', '-'),
                    "close": str(row[4]).lower().replace('nan', '-'),
                    "adj_close": str(row[4]).lower().replace('nan', '-'),
                    "volume": str(row[5]).lower().replace('nan', '-')}
            temp.append(data)
        response = insert_hist(data={"data": temp})
        print(f'{stock}: {response}')
        logging.info(f'{stock}: {response}')
        return response

    def main(self):
        sheet = self.connect_to_gs(self.config['watchlist'])
        worksheet = sheet.worksheet('Stocks')
        stocks_ = worksheet.get('A2:A2000')
        stocks_ = [s[0] for s in stocks_]
        print(f'Total Stocks {len(stocks_)}')
        for stock in stocks_:
            try:
                self.insert_in_db(stock)
            except:
                continue


if __name__ == "__main__":
    s = Historical()
    s.main()
