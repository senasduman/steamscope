import pandas as pd
import numpy as np
import scrapy
from scrapy.crawler import CrawlerProcess

class SteamspiderSpider(scrapy.Spider):
    name = "steamspider"
    allowed_domains = ["store.steampowered.com"]

    def start_requests(self):
        self.df = pd.read_csv("processed_data_scrapped_with_scrapy_and_beautifulsoup.csv")
        self.df["review_text"] = None
        self.df["review_value"] = None
        df_temp = self.df  # İlk 100 satırı alıyoruz, isteğe bağlı olarak değiştirebilirsiniz
        url_list = df_temp["Link"].str.strip()

        for url in url_list:
            yield scrapy.Request(url=url, callback=self.parse, meta={'url': url})

    def parse(self, response):
        name = response.css('div#appHubAppName.apphub_AppName::text').get()
        price = response.css("div.game_purchase_price::text").get()
        url = response.meta['url']

        if price:
            price = price.strip().replace('\r', '').replace('\n', '').replace('\t', '')
        else:
            price = '0'  

        if name:
            self.df.loc[self.df['Link'] == url, 'original_price'] = price
            self.df.loc[self.df['Link'] == url, 'review_text'] = name  # name'i review_text sütununa ekleme (örnek)
        else:
            self.df.loc[self.df['Link'] == url, 'original_price'] = np.nan


        self.logger.info(f"Processed {url}: name={name}, price={price}")

    def closed(self, reason):
        self.df.to_csv("steam_price.csv", index=False)
        self.logger.info("All processing is done and file saved.")


process = CrawlerProcess(settings={
    'FEED_FORMAT': 'json',
    'FEED_URI': 'output.json'
})
process.crawl(SteamspiderSpider)
process.start()