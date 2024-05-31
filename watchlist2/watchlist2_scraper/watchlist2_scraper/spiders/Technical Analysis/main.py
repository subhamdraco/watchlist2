from google.oauth2 import service_account
import gspread, time, math
from itertools import islice
import json, logging
from historical_data_us_onetime import HistoricalScrap


class Main:

    def __init__(self):
        SERVICE_ACCOUNT_FILE = 'keys.json'

        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
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
        client = gspread.authorize(self.cred)
        return client.open_by_key(sample_id)

    def retry_operation(self, max_retries=5, retry_delay=2):
        retries = 0
        while retries < max_retries:
            try:
                # Perform the operation that might result in TimeoutError
                result = self.main()
                return result
            except Exception as e:
                print(e)
                print(f"Attempt {retries + 1} failed. Retrying in {retry_delay} seconds...")
                retries += 1
                time.sleep(retry_delay)

        # If all attempts fail, raise an error or handle the situation accordingly
        raise TimeoutError("Operation failed after multiple retries.")

    def get_data(self, formula, stock):
        list_data = []
        try:
            h = HistoricalScrap()
            data = h.fill_data(stock)
            if formula == "formula1":
                data_range = [i for i in range(-255, 0, 5)]
                data_range.extend([-2, -1, -132, -198, -269])

            elif formula == "formula2":
                list_data2 = [float(data[i]['adj_close']) for i in range(-130, 0)][::-1]
                list_data = [(sum([list_data2[j] for j in range(i * 5, (i + 1) * 5)]) / 5) for i in
                             range(math.floor(len(list_data2) / 5))]
                # for i in range(0, 130, 5):
                #     avg1 = sum(list_data2[i:i+5])/5
                #     list_data.append(avg1)
                #     i = i + 5
                data_range = [-132, -198, -269, -332, -398, -462, -524]

            elif formula == 'formula3':
                data_range = [-1, -5, -10, -24, -64, -132, -198, -269, -398, -524]

            for exception in data_range:
                try:
                    list_data.append(float(data[exception]['adj_close']))
                except:
                    list_data.append(0.0)
            return list_data

        except Exception as e:
            logging.error(f"Error in get_data for {stock}: {e}")
            return []

    def main(self):
        stock_data = {}
        stock_average_return = {}
        top_stocks_5_days = {}
        batch_data = []
        spreadsheet = self.connect_to_gs(self.config['global'])
        subsheet = spreadsheet.worksheet('GLOBAL INDEX (Actual)')
        stocks = subsheet.get('B2:C6000')
        i = 2

        for stock in stocks:
            print('nyse', i)
            print('get', stock[1])
            data = self.get_data('formula1', stock=stock[1])
            average = self.get_data('formula2', stock=stock[1])
            monthly = self.get_data('formula3', stock=stock[1])
            if data:
                try:
                    average_return = sum(((data[i + 1] / data[i]) - 1) for i in range(len(data) - 4)) / (
                            len(data) - 3) * 100
                    average_return = round(average_return, 2)
                    data = data[::-1]
                    stock_data[stock[1]] = [data[:23][::-1]]
                    stock_average_return[stock[1]] = average_return
                    # 5d gap Condition upto 1y
                    #if all(data[i] > data[i + 1] for i in range(4, len(data) - 1)):
                    top_stocks_5_days[stock[1]] = [data[:23][::-1]]
                    # 5d price avg upto 6m-2y
                    #elif all(average[i] > average[i + 1] for i in range(len(average) - 1)):
                        #top_stocks_5_days[stock[1]] = [data[:23][::-1]]
                    # 1d,5d,1m,3m--2y
                    #elif all(monthly[i] > monthly[i + 1] for i in range(len(monthly) - 1)):
                        #top_stocks_5_days[stock[1]] = [data[:23][::-1]]
                except:
                    pass

            logging.info(f'Data received for {stock[1]}')
        stock_average_return = dict(sorted(stock_average_return.items(), key=lambda item: item[1], reverse=True))

        if top_stocks_5_days:
            for key, value in top_stocks_5_days.items():
                try:
                    batch_data.append({'range': f'I{i}:AE{i}', 'values': value})
                    index = [stock[0].replace('NasdaqGS', 'NASDAQ').replace('NasdaqCM', 'NASDAQ') for stock in stocks if
                             key == stock[1]]
                    batch_data.append({'range': f'G{i}:H{i}', 'values': [[index[0], key]]})
                    i = i + 1
                except:
                    continue
        #
        # number_top_stocks = int(len(stock_average_return) * 0.25) if len(stock_average_return) > 100 else len(
        #     stock_average_return)
        number_top_stocks = len(stock_average_return)

        stocks = dict(islice(stock_average_return.items(), number_top_stocks))
        for key, value in stocks.items():
            try:
                if key not in top_stocks_5_days:
                    index = [stock[0].replace('NasdaqGS', 'NASDAQ').replace('NasdaqCM', 'NASDAQ') for stock in stocks if
                             key == stock[1]]
                    batch_data.append({'range': f'I{i}:AE{i}', 'values': stock_data[key]})
                    batch_data.append({'range': f'G{i}:H{i}', 'values': [[index[0], key]]})
                    i = i + 1
            except:
                continue

        subsheet = spreadsheet.worksheet('Technical Analysis')
        logging.info(str(batch_data))
        subsheet.batch_update(batch_data)


if __name__ == "__main__":
    m = Main()
    try:
        result = m.retry_operation()
    except TimeoutError:
        print("Operation failed even after retries.")
