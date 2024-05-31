import json
from american_yahoo_scraping import Scrapper
import logging
from connection import update, insert, getdata
import pandas as pd
from google.oauth2 import service_account
import gspread


class Main:

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

    def scrape_yahoo(self):
        sheet = self.connect_to_gs(self.config['watchlist'])
        worksheet = sheet.worksheet('Stocks')
        stocks_ = worksheet.get('A2:A2000')
        stocks_ = [s[0] for s in stocks_]
        print(f'Total Stocks {len(stocks_)}')
        count = 2
        st_count = 1
        for stock in stocks_:
            print(f'{st_count} stock: {stock}')
            res = 500
            db_res = getdata(stock)

            logging.info(f'Getting stock data for stock {stock}')
            # print(f'Getting stock data for stock {j} {stock}')
            try:
                s = Scrapper(stock)

                if db_res['result'] != 'No Stocks':
                    st_count += 1
                    if db_res['result'][0]['earning']:
                        continue
                    else:
                        pass
                    logging.info(f'Data inserted for stock {stock} with {res}')
                    print(f'Data inserted for stock {stock} with {res}')
                    continue
                earning = s.get_earnings_estimated()
                ins_holder = s.get_major_holders_and_top_institutional_holders()
                data = {
                    'stocks': str(stock),
                    'summery': str(s.get_summary_details()),
                    'income': str(s.get_income_statements()),
                    'bal_sheet': str(s.get_balance_sheets()),
                    'cash_flow': str(s.get_cash_flow()),
                    'val_measures': str(s.get_valuation_measures()),
                    'trading': str(s.get_trading_information()),
                    'fin_high': str(s.get_financial_highlights()),
                    'earning': str(earning),
                    'major_holder': str(ins_holder[0]),
                    'institute': str(ins_holder[1]),
                    'profile': str(s.get_profile_data())
                }

                res = insert(data=data)
                # res = update(data=income_data)
                # j = j + 1
                # if j % 25 == 0:
                #     subsheet.batch_update(batch_data)
                #     batch_data = []
                logging.info(f'Data inserted for stock {stock} with {res}')
                print(f'Data inserted for stock {stock} with {res}')
            except Exception as e:
                # j = j + 1
                logging.info(f'Data inserted for stock {stock} with {res} due error {e}')
                print(f'Error in stock {stock} error:{e}')
            count += 1
            st_count += 1
        # worksheet.batch_clear(['A:B'])
        # worksheet.batch_update(watch_data)


if __name__ == "__main__":
    m = Main()
    print(m.scrape_yahoo())
