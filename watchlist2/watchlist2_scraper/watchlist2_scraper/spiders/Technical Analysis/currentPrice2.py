from google.oauth2 import service_account
import gspread, time
import json
import yfinance as yf


class Scrapper:

    def __init__(self):
        SERVICE_ACCOUNT_FILE = 'keys.json'

        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        # creds = None
        self.cred = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

        with open('config.json', 'r') as config_file:
            self.config = json.load(config_file)

    def connect_to_gs(self, sample_id):
        client = gspread.authorize(self.cred)
        return client.open_by_key(sample_id)
    
    def current_price(self, stock):
        try:
            stock_ = yf.Ticker(stock)
            data = stock_.info
            price = str(data['currentPrice'])
        except:
            return '0.0'
        return price.strip()

    def retry_operation(self, perform , max_retries=5, retry_delay=2):
        retries = 0
        while retries < max_retries:
            try:
                # Perform the operation that might result in TimeoutError
                result = perform
                return result
            except Exception as e:
                print(e)
                print(f"Attempt {retries + 1} failed. Retrying in {retry_delay} seconds...")
                retries += 1
                time.sleep(retry_delay)

        # If all attempts fail, raise an error or handle the situation accordingly
        raise TimeoutError("Operation failed after multiple retries.")
    
    def get_price_and_fill_tech(self):
        print('Starting Price Extracting')
        spreadsheet = self.connect_to_gs(s.config['global'])
        subsheet1 = spreadsheet.worksheet('Technical Analysis')
        subsheet2 = spreadsheet.worksheet('Technical Analysis2')
        

        def process_sheet(sheet):
            batch_data = []
            company_names = sheet.col_values(92)
            company_names2 = sheet.col_values(8)
            company_names = company_names[1:]
            company_names2 = company_names2[1:]
            print(company_names[:4])
            print(f'Total Stocks: {len(company_names)}')
            for i, company in enumerate(company_names, start=1):
                if company:
                    try:
                        price = self.retry_operation(self.current_price(company))
                        print(f'{i}: Getting data for {company}--', price)
                        cell = company_names2.index(company) +2

                        batch_data.append({
                            'range': f'AB{cell}:AB{cell}',
                            'values': [[price]]
                        })

                    except Exception as e:
                        print(f"Error getting data for {company}: {e}")
            sheet.batch_update(batch_data)

        process_sheet(subsheet1)
        process_sheet(subsheet2)

if __name__ == '__main__':
    s = Scrapper()
    result = s.get_price_and_fill_tech()
    
 
    
# stock_ = yf.Ticker('QRTEA')
# data = stock_.info
# price = str(data['currentPrice'])    
# print(price)

