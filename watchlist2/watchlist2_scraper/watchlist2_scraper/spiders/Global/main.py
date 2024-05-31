from google.oauth2 import service_account
import gspread
import json, logging
from historical_data_us_onetime import HistoricalScrap


class Main:

    def __init__(self):
        SERVICE_ACCOUNT_FILE = 'keys.json'

        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        # creds = None
        self.cred = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

        logging.basicConfig(
            level=logging.INFO,  # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            format="%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            filename="app.log",  # Log messages will be written to this file
            filemode="w"  # 'w' for overwrite, 'a' for append
        )

        with open('config.json', 'r') as config_file:
            self.config = json.load(config_file)

    def connect_to_gs(self, sample_id):
        creds = self.cred
        client = gspread.authorize(creds)
        sheet = client.open_by_key(sample_id)
        return sheet

    def get_data(self, country, stock):
        list_data = []
        try:
            if country == 'US':
                h = HistoricalScrap()
                data = h.fill_data(stock)
                list_data.append(float(data[-30]['adj_close']))
                list_data.append(float(data[-15]['adj_close']))
                list_data.append(float(data[-5]['adj_close']))
                list_data.append(float(data[-1]['adj_close']))
                try:
                    list_data.append(float(data[-78]['adj_close']))
                except:
                    list_data.append(0.0)
                try:
                    list_data.append(float(data[-132]['adj_close']))
                except:
                    list_data.append(0.0)
                try:
                    list_data.append(float(data[-198]['adj_close']))
                except:
                    list_data.append(0.0)
                try:
                    list_data.append(float(data[-269]['adj_close']))
                except:
                    list_data.append(0.0)

            elif country == 'Indonesia':
                h = HistoricalScrap()
                data = h.fill_data(stock)
                list_data.append(float(data[-30]['adj_close']))
                list_data.append(float(data[-15]['adj_close']))
                list_data.append(float(data[-5]['adj_close']))
                list_data.append(float(data[-1]['adj_close']))
                try:
                    list_data.append(float(data[-78]['adj_close']))
                except:
                    list_data.append(0.0)
                try:
                    print(data[-132])
                    list_data.append(float(data[-132]['adj_close']))
                except:
                    list_data.append(0.0)
                try:
                    list_data.append(float(data[-198]['adj_close']))
                except:
                    list_data.append(0.0)
                try:
                    list_data.append(float(data[-269]['adj_close']))
                except:
                    list_data.append(0.0)

            else:
                h = HistoricalScrap()
                data = h.fill_data(stock)
                list_data.append(float(data[-30]['adj_close']))
                list_data.append(float(data[-15]['adj_close']))
                list_data.append(float(data[-5]['adj_close']))
                list_data.append(float(data[-1]['adj_close']))
                try:
                    list_data.append(float(data[-78]['adj_close']))
                except:
                    list_data.append(0.0)
                try:
                    list_data.append(float(data[-132]['adj_close']))
                except:
                    list_data.append(0.0)
                try:
                    list_data.append(float(data[-198]['adj_close']))
                except:
                    list_data.append(0.0)
                try:
                    list_data.append(float(data[-269]['adj_close']))
                except:
                    list_data.append(0.0)
        except:
            return list_data
        return list_data

    def main(self):

        spreadsheet = self.connect_to_gs(self.config['watchlist2'])
        subsheet = spreadsheet.worksheet('Stocks')
        stocks = subsheet.get('A2:C')
        batch_data = []
        i = 2
        for stock in stocks:
            print((i-1), stock[0])
            try:
                if stock[0] == 'Indonesia':
                    data = self.get_data(country=stock[0], stock=stock[-1] + '.JK')
                    if data:
                        batch_data.append({'range': f'O{i}:V{i}', 'values': [data]})
                elif stock[0] == 'Belgium':
                    data = self.get_data(country=stock[0], stock=stock[-1] + '.BR')
                    if data:
                        batch_data.append({'range': f'O{i}:V{i}', 'values': [data]})
                elif stock[0] == 'France':
                    data = self.get_data(country=stock[0], stock=stock[-1] + '.PA')
                    if data:
                        batch_data.append({'range': f'O{i}:V{i}', 'values': [data]})
                else:
                    data = self.get_data(country=stock[2], stock=stock[0])
                    if data:
                        batch_data.append({'range': f'O{i}:V{i}', 'values': [data]})
                        batch_data.append({'range': f'A{i}:C{i}', 'values': [[stock[2], stock[1], stock[0]]]})
            except:
                i = i + 1
                continue
            i = i + 1
        spreadsheet = self.connect_to_gs(self.config['global'])
        subsheet = spreadsheet.worksheet('GLOBAL INDEX (Actual)')
        subsheet.batch_update(batch_data)
        logging.info(f'{str(batch_data)} dumped')


if __name__ == "__main__":
    m = Main()
    m.main()
