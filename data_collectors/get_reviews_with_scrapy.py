import scrapy
import pandas as pd
from scrapy.crawler import CrawlerProcess
import numpy as np


class SteamspiderSpider(scrapy.Spider):
    name = "steamspider"
    allowed_domains = ["store.steampowered.com"]

    def start_requests(self):
        self.df = pd.read_csv("processed_data.csv")  # .head(1000)
        self.df["review_text"] = None
        self.df["review_value"] = None
        # df_temp = self.df.head(100)  # İlk 5000 satırı alıyoruz, isteğe bağlı olarak değiştirebilirsiniz
        url_list = self.df["Link"].str.strip()

        for url in url_list:
            yield scrapy.Request(url=url, callback=self.parse, meta={'url': url})

    def parse(self, response):
        name = response.css('div#appHubAppName.apphub_AppName::text').get()
        review_summary = response.css('div.summary_section span::text').getall()
        url = response.meta['url']

        if name and review_summary:
            review_text = review_summary[0].strip()
            review_value = review_summary[1].strip()
            self.df.loc[self.df['Link'] == url, 'review_text'] = review_text
            self.df.loc[self.df['Link'] == url, 'review_value'] = review_value
        
        else:
            # Hata durumunda da DataFrame'e ekleyin
            self.df.loc[self.df['Link'] == url, 'review_text'] = np.nan
            self.df.loc[self.df['Link'] == url, 'review_value'] = np.nan
        

    def closed(self, reason):
        print("All processing is done.")
        self.df.to_csv("processed_data_that_scrapped_with_scrapy_test.csv", index=False)


process = CrawlerProcess(settings={
    'FEED_FORMAT': 'json',
    'FEED_URI': 'output.json'
})
process.crawl(SteamspiderSpider)
process.start()