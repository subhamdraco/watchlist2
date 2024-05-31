import json
import datetime
from google.oauth2 import service_account
import gspread
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import yfinance as yf
import logging
from io import StringIO


class Scrapper:

    def __init__(self, stock):
        # print(f'Opening Yahoo Finance Website for stock, {stock}')
        self.stock = stock

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

    def get_summary_details(self):
        print(f'{self.stock} get_summary_details')
        final_data_list = []
        data = {}
        stock_ = yf.Ticker(self.stock)
        st = stock_.info
        try:
            ear = stock_.earnings_dates
            ear.reset_index(inplace=True)
            ear.dropna(inplace=True)
        except:
            ear = {}
        try:
            date_list = ear['Earnings Date'].to_list()
        except:
            date_list = []
        try:
            data['Previous Close'] = st['previousClose']
        except:
            pass
        try:
            data['Market Cap'] = st['marketCap']
        except:
            pass
        try:
            data['Open'] = st['open']
        except:
            pass
        try:
            data['Beta (5Y Monthly)'] = st['beta']
        except:
            pass
        try:
            data['Bid'] = f"{st['bid']} X {st['bidSize']}"
        except:
            pass
        try:
            data['PE Ratio (TTM)'] = st['trailingPE']
        except:
            pass
        try:
            data['Ask'] = f"{st['ask']} X {st['askSize']}"
        except:
            pass
        try:
            data["EPS (TTM)"] = st['forwardPE']
        except:
            pass
        try:
            data["Day's Range"] = f"{st['dayLow']} X {st['dayLow']}"
        except:
            pass
        try:
            data[
                'Earnings Date'] = f'{date_list[1].date().strftime("%b, %d %Y")} - {date_list[0].date().strftime("%b, %d %Y")}'
        except:
            pass
        try:
            data["52 Week Range"] = f'{st["fiftyTwoWeekLow"]} X {st["fiftyTwoWeekHigh"]}'
        except:
            pass
        try:
            data['Forward Dividend & Yield'] = f'{st["dividendRate"]} ({st["dividendYield"]} %)'
        except:
            pass
        try:
            data["Volume"] = st["volume"]
        except:
            pass
        try:
            data['Ex-Dividend Date'] = datetime.datetime.fromtimestamp(st['exDividendDate']).strftime("%b, %d %Y")
        except:
            pass
        try:
            data["Avg. Volume"] = st['averageVolume']
        except:
            pass
        try:
            data['1y Target Est'] = st['targetMeanPrice']
        except:
            pass
        for key, value in data.items():
            final_data_list.append([key, value])

        return final_data_list

    def split_string(self, input_string, chunk_size):
        chunks = [input_string[i:i + chunk_size] for i in range(0, len(input_string), chunk_size)]
        return chunks

    def get_income_statements(self):
        print(f'{self.stock} get_income_statements')
        stock_ = yf.Ticker(self.stock)
        df_ = stock_.incomestmt
        cols = df_.columns.to_list()
        header = ['Breakdown']
        header = header + [datetime.datetime.strftime(c, '%Y-%m-%d') for c in cols]
        df_.reset_index(inplace=True)
        rows = df_.values.tolist()
        rows.insert(0, header)
        rows = [[str(row_inside).replace('nan', '-') for row_inside in row] for row in rows]
        final_data = [header]
        for row in rows:
            if 'Total Revenue' in row:
                final_data.append(row)
            elif 'Cost Of Revenue' in row:
                final_data.append(row)
            elif 'Gross Profit' in row:
                final_data.append(row)
            elif 'Operating Expense' in row or 'Total Expenses' in row:
                final_data.append(row)
            elif 'Operating Income' in row:
                final_data.append(row)
            elif 'Net Non Operating Interest Income Expense' in row:
                final_data.append(row)
            elif 'Other Income Expense' in row:
                final_data.append(row)
            elif 'Pretax Income' in row:
                final_data.append(row)
            elif 'Tax Provision' in row:
                final_data.append(row)
            elif 'Net Income Common Stockholders' in row:
                final_data.append(row)
            elif 'Diluted NI Availto Com Stockholders' in row:
                final_data.append(row)
            elif 'Basic EPS' in row:
                final_data.append(row)
            elif 'Diluted EPS' in row:
                final_data.append(row)
            elif 'Basic Average Shares' in row:
                final_data.append(row)
            elif 'Diluted Average Shares' in row:
                final_data.append(row)
            elif 'Total Operating Income As Reported' in row:
                final_data.append(row)
            elif 'Total Expenses' in row:
                final_data.append(row)
            elif 'Net Income From Continuing And Discontinued Operation' in row:
                final_data.append(row)
            elif 'Normalized Income' in row:
                final_data.append(row)
            elif 'Interest Income' in row:
                final_data.append(row)
            elif 'Interest Expense' in row:
                final_data.append(row)
            elif 'Net Interest Income' in row:
                final_data.append(row)
            elif 'EBIT' in row:
                final_data.append(row)
            elif 'EBITDA' in row:
                final_data.append(row)
            elif 'Reconciled Cost Of Revenue' in row:
                final_data.append(row)
            elif 'Reconciled Depreciation' in row:
                final_data.append(row)
            elif 'Net Income From Continuing Operation Net Minority Interest' in row:
                final_data.append(row)
            elif 'Normalized EBITDA' in row:
                final_data.append(row)
            elif 'Tax Rate for Calcs' in row:
                final_data.append(row)
            elif 'Tax Effect Of Unusual Items' in row:
                final_data.append(row)
        return final_data

    def get_balance_sheets(self):
        print(f'{self.stock} get_balance_sheets')
        stock_ = yf.Ticker(self.stock)
        df_ = stock_.balancesheet
        cols = df_.columns.to_list()
        header = ['Breakdown']
        header = header + [datetime.datetime.strftime(c, '%Y-%m-%d') for c in cols]
        df_.reset_index(inplace=True)
        rows = df_.values.tolist()
        rows.insert(0, header)
        rows = [[str(row_inside).replace('nan', '-') for row_inside in row] for row in rows]
        return rows

    def get_cash_flow(self):
        print(f'{self.stock} get_cash_flow')
        stock_ = yf.Ticker(self.stock)
        df_ = stock_.cashflow
        cols = df_.columns.to_list()
        header = ['Breakdown']
        header = header + [datetime.datetime.strftime(c, '%Y-%m-%d') for c in cols]
        df_.reset_index(inplace=True)
        rows = df_.values.tolist()
        rows.insert(0, header)
        rows = [[str(row_inside).replace('nan', '-') for row_inside in row] for row in rows]
        return rows

    def get_valuation_measures(self):
        print(f'{self.stock} get_valuation_measures')
        final_data_list = []
        try:
            stock_ = yf.Ticker(self.stock)
            df_ = stock_.info
            data = {}

            try:
                data['Market Cap (intraday)'] = df_['marketCap']
            except:
                pass

            try:
                data['Trailing P/E'] = df_['trailingPE']
            except:
                pass

            try:
                data['Forward P/E'] = df_['forwardPE']
            except:
                pass

            try:
                data['PEG Ratio (5 yr expected)'] = df_['pegRatio']
            except:
                pass

            try:
                data['Price/Sales (ttm)'] = df_['priceToSalesTrailing12Months']
            except:
                pass

            try:
                data['Price/Book (mrq)'] = df_['priceToBook']
            except:
                pass

            try:
                data['Enterprise Value/Revenue'] = df_['enterpriseToRevenue']
            except:
                pass

            try:
                data['Enterprise Value/EBITDA'] = df_['enterpriseToEbitda']
            except:
                pass

            try:
                data['Enterprise Value'] = df_['enterpriseValue']
            except:
                pass
            for key, value in data.items():
                final_data_list.append([key, value])
        except Exception as e:
            logging.info(f'Error {e}')
            final_data_list = []
        return final_data_list

    def get_trading_information(self):  # statistics
        print(f'{self.stock} get_trading_information')
        final_data_list = []
        try:
            stock_ = yf.Ticker(self.stock)
            df_ = stock_.info
            data = {}

            try:
                data['Beta (5Y Monthly)'] = df_['Beta']
            except:
                pass

            try:
                data['Avg Vol (3 month)'] = df_['averageVolume']
            except:
                pass

            try:
                data['Forward Annual Dividend Rate'] = df_['dividendRate']
            except:
                pass

            try:
                data['52-Week Change'] = df_['52WeekChange']
            except:
                pass

            try:
                data['S&P500 52-Week Change'] = df_['SandP52WeekChange']
            except:
                pass

            try:
                data['52 Week High'] = df_['fiftyTwoWeekHigh']
            except:
                pass

            try:
                data['52 Week Low'] = df_['fiftyTwoWeekLow']
            except:
                pass

            try:
                data['50-Day Moving Average'] = df_['fiftyDayAverage']
            except:
                pass

            try:
                data['200-Day Moving Average'] = df_['twoHundredDayAverage']
            except:
                pass

            try:
                data['Avg Vol (10 day)'] = df_['averageVolume10days']
            except:
                pass

            try:
                data['Shares Outstanding'] = df_['sharesOutstanding']
            except:
                pass

            try:
                data['Implied Shares Outstanding'] = df_['impliedSharesOutstanding']
            except:
                pass

            try:
                data['Float'] = df_['floatShares']
            except:
                pass

            try:
                data['% Held by Insiders'] = df_['heldPercentInsiders']
            except:
                pass

            try:
                data['% Held by Institutions'] = df_['heldPercentInstitutions']
            except:
                pass

            try:
                data['Shares Short'] = df_['sharesShort']
            except:
                pass

            try:
                data['Short Ratio'] = df_['shortRatio']
            except:
                pass

            try:
                data['Short % of Float'] = df_['shortPercentOfFloat']
            except:
                pass

            try:
                data['Short % of Shares Outstanding'] = df_['sharesOutstanding']
            except:
                pass

            try:
                data['Forward Annual Dividend Yield'] = df_['dividendYield']
            except:
                pass

            try:
                data['Trailing Annual Dividend Rate'] = df_['trailingAnnualDividendRate']
            except:
                pass

            try:
                data['Trailing Annual Dividend Yield'] = df_['trailingAnnualDividendYield']
            except:
                pass

            try:
                data['5 Year Average Dividend Yield'] = df_['fiveYearAvgDividendYield']
            except:
                pass

            try:
                data['Payout Ratio'] = df_['payoutRatio']
            except:
                pass

            try:
                data['Dividend Date'] = datetime.datetime.utcfromtimestamp(df_['exDividendDate']).strftime(
                    '%Y-%m-%d')
            except:
                pass

            try:
                data['Ex-Dividend Date'] = df_['exDividendDate']
            except:
                pass

            try:
                data['Last Split Factor'] = df_['lastSplitFactor']
            except:
                pass

            try:
                data['Last Split Date'] = datetime.datetime.utcfromtimestamp(df_['lastSplitDate']).strftime(
                    '%Y-%m-%d')
            except:
                pass
            for key, value in data.items():
                final_data_list.append([key, value])
        except Exception as e:
            logging.info(f'Error {e}')
            final_data_list = []

        return final_data_list

    def get_financial_highlights(self):  # statistics
        print(f'{self.stock} get_financial_highlights')
        try:
            final_data_list = []
            stock_ = yf.Ticker(self.stock)
            df_ = stock_.info
            data = {}
            try:
                data['Fiscal Year Ends'] = datetime.datetime.utcfromtimestamp(df_['lastFiscalYearEnd']).strftime(
                    '%Y-%m-%d')
            except:
                pass
            try:
                data['Most Recent Quarter (mrq)'] = datetime.datetime.utcfromtimestamp(
                    df_['mostRecentQuarter']).strftime(
                    '%Y-%m-%d')
            except:
                pass
            try:
                data['Profit Margin'] = df_['profitMargins']
            except:
                pass
            try:
                data['Operating Margin (ttm)'] = df_['operatingMargins']
            except:
                pass
            try:
                data['Return on Assets (ttm)'] = df_['returnOnAssets']
            except:
                pass
            try:
                data['Return on Equity (ttm)'] = df_['returnOnEquity']
            except:
                pass
            try:
                data['Revenue (ttm)'] = df_['totalRevenue']
            except:
                pass
            try:
                data['Revenue Per Share (ttm)'] = df_['revenuePerShare']
            except:
                pass
            try:
                data['Quarterly Revenue Growth (yoy)'] = df_['revenueGrowth']
            except:
                pass
            try:
                data['Gross Profit (ttm)'] = df_['grossProfits']
            except:
                pass
            try:
                data['EBITDA'] = df_['ebitda']
            except:
                pass
            try:
                data['Net Income Avi to Common (ttm)'] = df_['netIncomeToCommon']
            except:
                pass
            try:
                data['Diluted EPS (ttm)'] = df_['trailingEps']
            except:
                pass
            try:
                data['Quarterly Earnings Growth (yoy)'] = df_['earningsQuarterlyGrowth']
            except:
                pass
            try:
                data['Total Cash (mrq)'] = df_['totalCash']
            except:
                pass
            try:
                data['Total Cash Per Share (mrq)'] = df_['totalCashPerShare']
            except:
                pass
            try:
                data['Total Debt (mrq)'] = df_['totalDebt']
            except:
                pass
            try:
                data['Total Debt/Equity (mrq)'] = df_['debtToEquity']
            except:
                pass
            try:
                data['Current Ratio (mrq)'] = df_['currentRatio']
            except:
                pass
            try:
                data['Book Value Per Share (mrq)'] = df_['bookValue']
            except:
                pass
            try:
                data['Operating Cash Flow (ttm)'] = df_['operatingCashflow']
            except:
                pass
            try:
                data['Levered Free Cash Flow (ttm)'] = df_['freeCashflow']
            except:
                pass
            for key, value in data.items():
                final_data_list.append([key, value])
            return final_data_list
        except Exception as e:
            logging.info(f'Error {e}')
            final_data_list = []

        return final_data_list

    def get_earnings_estimated(self):
        print(f'{self.stock} get_earnings_estimated')
        option_s = Options()
        option_s.add_argument("--lang=en")
        option_s.add_argument("--headless=new")
        option_s.add_argument("--log-level=OFF")
        option_s.add_argument("--window-size=1920x1080")
        option_s.add_argument("--disable-gpu")
        driver = webdriver.Chrome(options=option_s)
        final_data = []
        try:
            driver.get(f'https://finance.yahoo.com/quote/{self.stock}/analysis')
            driver.implicitly_wait(2)
            page_source = driver.page_source
            tables = pd.read_html(StringIO(page_source))
            count = 1
            for table in tables:
                cols = table.columns.to_list()
                try:
                    i = cols.index('CURRENCY IN USD')
                except:
                    continue
                if count == 1:
                    cols = cols[:i]+['Earnings Estimate']+cols[i+1:]
                if count == 2:
                    cols = cols[:i] + ['Revenue Estimate'] + cols[i + 1:]
                if count == 3:
                    cols = cols[:i] + ['Earnings History'] + cols[i + 1:]
                if count == 4:
                    cols = cols[:i] + ['EPS Trend'] + cols[i + 1:]
                if count == 5:
                    cols = cols[:i] + ['EPS Revisions'] + cols[i + 1:]
                if count == 6:
                    cols = cols[:i] + ['Growth Estimates'] + cols[i + 1:]
                final_data.append(cols)
                final_data = final_data + table.values.tolist()
                count += 1
            return final_data

        except Exception as e:
            driver.quit()
            logging.info(f'Error {e}')
        driver.quit()
        return final_data

    def get_major_holders_and_top_institutional_holders(self):
        print(f'{self.stock} get_major_holders_and_top_institutional_holders')
        option_s = Options()
        option_s.add_argument("--lang=en")
        option_s.add_argument("--headless=new")
        option_s.add_argument("--log-level=OFF")
        option_s.add_argument("--window-size=1920x1080")
        option_s.add_argument("--disable-gpu")
        driver = webdriver.Chrome(options=option_s)
        major_holders = []
        institution = []
        try:
            driver.get(f'https://finance.yahoo.com/quote/{self.stock}/holders')
            driver.implicitly_wait(2)
            page_source = driver.page_source
            tables = pd.read_html(StringIO(page_source))

            major_holders.append(tables[0].columns.to_list())
            major_holders = major_holders + tables[0].values.tolist()
            institution.append(tables[1].columns.to_list())
            institution = institution + tables[1].values.tolist()
            driver.quit()
        except Exception as e:
            logging.error(f'{self.stock} error {str(e)} : holders')
            driver.quit()
        return major_holders, institution

    def get_profile_data(self):
        print(f'{self.stock} get_profile_data')
        data = {}
        final_data_list = []
        stock_ = yf.Ticker(self.stock)
        df_ = stock_.info
        try:
            data['Sector(s):'] = df_['sector']
        except:
            data['Sector(s):'] = ''
        try:
            data['Industry:'] = df_['industry']
        except:
            data['Industry:'] = ''
        try:
            data['Full Time Employees:'] = df_['fullTimeEmployees']
        except:
            data['Full Time Employees:'] = ''

        for key, value in data.items():
            final_data_list.append([key, value])
        return final_data_list


# if __name__ == "__main__":
#     # s = Scrapper('ENLAY')
#     # print(s.get_major_holders())
#     stock_ = yf.Ticker('AAPL')
#     print(stock_.get_institutional_holders())
    # #     print(s.get_earnings_estimated())
    # df = stock_.incomestmt
    # print(df)
    # print(s.get_top_institutional_holders())
