from google.oauth2 import service_account
import gspread, json


class TransferData:

     def __init__(self):
          SERVICE_ACCOUNT_FILE = 'keys.json'
          SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
          self.cred = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

          with open('config.json', 'r') as config_file:
               self.config = json.load(config_file)

     def connect_to_gs(self,Sample_ID):

          creds = self.cred
          client = gspread.authorize(creds)
          sheet = client.open_by_key(Sample_ID)
          return sheet
     
     
     def main(self, data, data1):
          
          nyse = []
          nasdaq = []
          jkse = []
          japan = []
          france = []
          belgium = []
          singapore = []
          sse = []
          sze = []

          for val in data:
               # print(val[0])

               if val[0] == 'NYSE':
                    nyse.append([val[1]])

               elif val[0] == 'NASDAQ':
                    nasdaq.append([val[1]])

               elif val[0] == 'JKSE':
                    jkse.append([val[1].replace('.JK','')])

               elif val[0] == 'CAC':
                    france.append([val[1].replace('.PA','')])

               elif val[0] == 'BFX':
                    belgium.append([val[1].replace('.BR','')])
          
               elif val[0] == 'NIKKEI':
                    japan.append([val[1]])

               elif val[0] == 'SGX':
                    singapore.append([val[1]])

          for val in data1:
               if val[0] == 'SSE':
                    sse.append([val[1]])

               elif val[0] == 'SZE':
                    sze.append([val[1]])

          all_cntrs = [nyse, nasdaq, jkse, france, belgium, japan, singapore, sse, sze]
          
          # print(france)   
     #   print(len(nyse))
     #   print(len(nasdaq))
          for i,sht in enumerate(list(self.config)[1:]):
               print(i,sht)
               
               if sht == 'nasdaq':
                    spreadsheet = self.connect_to_gs(self.config[f'{sht}'])
                    spreadsheet.worksheet('Top Predicted Stocks by ML (Actual)').update('B750:B',all_cntrs[i],value_input_option='user_entered')
                    print(f"{sht} done")
               else:
                    spreadsheet = self.connect_to_gs(self.config[f'{sht}'])
                    spreadsheet.worksheet('Top Predicted Stocks by ML (Actual)').update('B400:B',all_cntrs[i],value_input_option='user_entered')
                    print(f"{sht} done")
          
     #   spreadsheet = self.connect_to_gs(self.config['france'])
     #   spreadsheet.worksheet('Top Predicted Stocks by ML (Actual)').batch_clear(['B400:B700'])
     #   spreadsheet.worksheet('Top Predicted Stocks by ML (Actual)').update('B400:B700',france,value_input_option='user_entered')
     #   print("France done")

     #   spreadsheet = self.connect_to_gs(self.config['japan'])
     #   spreadsheet.worksheet('Top Predicted Stocks by ML (Actual)').batch_clear(['B400:B700'])
     #   spreadsheet.worksheet('Top Predicted Stocks by ML (Actual)').update('B400:B700',japan,value_input_option='user_entered')
     #   print("Japan done")

     #   spreadsheet = self.connect_to_gs(self.config['belgium'])
     #   spreadsheet.worksheet('Top Predicted Stocks by ML (Actual)').batch_clear(['B400:B700'])
     #   spreadsheet.worksheet('Top Predicted Stocks by ML (Actual)').update('B400:B700',belgium,value_input_option='user_entered')
     #   print("Belgium done")

     #   spreadsheet = self.connect_to_gs(self.config['singapore'])
     #   spreadsheet.worksheet('Top Predicted Stocks by ML (Actual)').batch_clear(['B400:B700'])
     #   spreadsheet.worksheet('Top Predicted Stocks by ML (Actual)').update('B400:B700',singapore,value_input_option='user_entered')
     #   print("Singapore done")

     #   spreadsheet = self.connect_to_gs(self.config['indo'])
     #   spreadsheet.worksheet('Top Predicted Stocks by ML (Actual)').batch_clear(['B400:B700'])
     #   spreadsheet.worksheet('Top Predicted Stocks by ML (Actual)').update('B400:B700',jkse,value_input_option='user_entered')
     #   print("Indonesia done")

     #   spreadsheet = self.connect_to_gs(self.config['nyse'])
     #   spreadsheet.worksheet('Top Predicted Stocks by ML (Actual)').batch_clear(['B400:B700'])
     #   spreadsheet.worksheet('Top Predicted Stocks by ML (Actual)').update('B400:B1000',nyse,value_input_option='user_entered')
     #   print("NYSE done")
     
     #   spreadsheet = self.connect_to_gs(self.config['nasdaq'])
     #   spreadsheet.worksheet('Top Predicted Stocks by ML (Actual)').batch_clear(['B750:B1050'])
     #   spreadsheet.worksheet('Top Predicted Stocks by ML (Actual)').update('B750:B1250',nasdaq,value_input_option='user_entered')
     #   print("Nasdaq done")
          print("Dumped") 
               

if __name__ == '__main__':

    t = TransferData()
    spreadsheet = t.connect_to_gs(t.config['global'])
    subsheet = spreadsheet.worksheet('Technical Analysis')
    subsheet2 = spreadsheet.worksheet('Technical Analysis2')
    data_ = subsheet.get('G2:H')
    data_1 = subsheet2.get('G2:H')
    # print(data_)
    t.main(data_, data_1)