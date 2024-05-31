import pandas as pd
import scrapy
import json, os
from datetime import datetime


class DateScrape(scrapy.Spider):
    name = 'DateScrape'
    start_urls = []
    stock_ = []
    f = open('urls.json')
    files = json.load(f)
    for entry in files:
        if entry['url']:
            if "?" in entry['url']:
                url = entry['url'].split("?")
                url = 'https://www.investing.com'+url[0]+'-earnings?'+url[1]
                start_urls.append(url)
                stock_.append(entry['stock'])
            else:
                start_urls.append('https://www.investing.com' + entry['url'] + '-earnings')
                stock = entry['stock']
                stock_.append(entry['stock'])
    # start_urls = ['https://www.investing.com/equities/williams-sonoma-inc-earnings']
    custom_settings = {
        'FEED_FORMAT': 'json',
        'FEED_URI': 'data.json',
        'DOWNLOAD_DELAY': 2
    }

    def __init__(self, **kwargs):
        # Path to your JSON file
        file_path = "data.json"
        super().__init__(**kwargs)
        if os.path.exists(file_path):
            # Open the file in write mode ('w')
            with open(file_path, 'w') as json_file:
                # Writing nothing effectively clears the file
                pass

    def parse(self, response):
        index = self.start_urls.index(str(response.url))
        stock = self.stock_[index]
        earning_data = []
        res = response.xpath('//table[@class="genTbl openTbl ecoCalTbl earnings earningsPageTbl"]//tr')
        for row in res:
            earning_data.append(row.xpath("td//text()").getall())
        if earning_data:
            dates_ = pd.DataFrame({'dates': sorted(
                list(filter(None,
                            [datetime.strptime(date[0], "%b %d, %Y") if date else None for date in earning_data])))})
            dates_['dates'] = pd.to_datetime(dates_['dates'])
            today_date = datetime.today().strftime('%Y-%m-%d')
            future_dates = dates_[dates_['dates'] > today_date]
            recent_dates = dates_[dates_['dates'] < today_date]
            if not future_dates.empty and not recent_dates.empty:
                future_date = future_dates.head(1)['dates'].iloc[0].strftime('%d/%m/%Y')
                recent_date = recent_dates.tail(1)['dates'].iloc[0].strftime('%d/%m/%Y')
            else:
                future_date = '-'
                recent_date = '-'
            index = response.css('a').xpath('//i[@class="btnTextDropDwn arial_12 bold"]//text()').get()
            yield {
                'future_date': future_date,
                'recent_date': recent_date,
                'stock': stock,
                'index': index

            }
