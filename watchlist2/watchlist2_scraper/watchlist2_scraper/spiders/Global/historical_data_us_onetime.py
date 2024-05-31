import json
import logging
import yfinance as yf
from datetime import datetime
import pandas as pd
import pytz


class HistoricalScrap:

    def __init__(self):

        logging.basicConfig(
            level=logging.INFO,  # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            format="%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            filename="app.log",  # Log messages will be written to this file
            filemode="w"  # 'w' for overwrite, 'a' for append
        )
        with open('config.json', 'r') as config_file:
            self.config = json.load(config_file)

    def get_historical_dataframe(self, stock):
        # try:
        #     timezone = pytz.timezone('Asia/Jakarta')
        #
        #     timestamp = int(datetime.timestamp(datetime.now(timezone)))
        #
        #     query = f'https://query1.finance.yahoo.com/v7/finance/download/{stock}?period1=203817600&period2={timestamp}&interval=1d&events=history&includeAdjustedClose=true'
        #
        #     df = pd.read_csv(query)
        #
        #     df = df.fillna(''), 'website'
        # except:
        stock_ = yf.Ticker(stock)
        df_ = stock_.history(period='max')
        df_.reset_index(inplace=True)
        df_ = df_.fillna('')
        return df_, 'yahoo'

    def fill_data(self, stock):
        #
        # df = pd.read_csv('inv_amer_stock.csv')
        # stocks = df['stocks'].to_list()        # stocks = ['AKA','KUKE','CXAIW','MPLN','HTOOW','SLNHP','AUVIP','AIH']
        #
        # # his_test = self.get_historical_dataframe(stocks[0])
        # # print(his_test)
        # for stock in stocks:
        temp_data = []
        res = 200
        try:
            his_test = self.get_historical_dataframe(stock)
            if his_test[1] == 'yahoo':
                values = his_test[0].values

                for value in values:
                    if len(values[-1]) >= 7:
                        date = datetime.strptime(str(value[0]).replace('nan', '-'), "%Y-%m-%d %H:%M:%S%z").date()

                        data = {"stock": stock,
                                "date": str(date),
                                "open": str(value[1]).lower().replace('nan', '-'),
                                "high": str(value[2]).lower().replace('nan', '-'),
                                "low": str(value[3]).lower().replace('nan', '-'),
                                "close": str(value[4]).lower().replace('nan', '-'),
                                "adj_close": str(value[4]).lower().replace('nan', '-'),
                                "volume": str(value[5]).lower().replace('nan', '-')}
                        temp_data.append(data)
                        # res = insert(data={'data': temp_data})
                    else:
                        continue
            logging.info(f'Historical for {stock} stock received {res}')
            return temp_data
        except Exception as e:
            logging.info(f'Error receiving stock {stock} as {e}')
            print(f'Error receiving stock {stock} as {e}')
            return temp_data

