import boto3
import scrapy
import os

from datetime import date
from scrapy.crawler import CrawlerProcess

import sys
sys.path.append('tools')
from housing_common import *

s3 = boto3.client('s3')
local_path = 'file:///Users/Andrei/dev/housing/housing/housing/spiders/'
local = False

characteristics = [
    "Nr. camere",
    "Suprafaţă utilă",
    "Suprafaţă utilă totală",
    "Suprafaţă construită",
    "Compartimentare",
    "Confort",
    "Etaj",
    "Nr. bucătării",
    "Nr. băi",
    "An construcţie",
    "Structură rezistenţă",
    "Tip imobil",
    "Regim înălţime",
    "Nr. balcoane"
]


class AppartmentsSpider(scrapy.Spider):
    name = "appartments"

    inventory_df = load_df('inventory', local)

    if local:
        start_urls = [local_path + 'main_page.html']
    else:
        start_urls = ['https://www.imobiliare.ro/vanzare-apartamente/craiova']
        local_path = ''
    
    print("## " + start_urls[0])

    def parse(self, response):
        characteristics_dic = {}

        ids = []
        for id in response.xpath('//div[@itemtype="http://schema.org/Offer"]/@id'):
            ids.append(id.get()[6:])

        links = []
        for link in response.css('a.img-block'):
            links.append(link.css('a::attr(href)').get())

        prices = []
        for price in response.xpath('//span[@class="pret-mare"]/text()'):
            prices.append(price.get())
        
        app_dict = dict(zip(ids, zip(prices, links)))

        for id in app_dict:
            if id in self.inventory_df.index:
                characteristics_dic['id'] = id
                characteristics_dic['price'] = app_dict[id][0]
                yield characteristics_dic
            else:
                appartment_page = app_dict[id][1]
                yield scrapy.Request(appartment_page, callback=self.parse_appartment_page)
        
        next_page = response.xpath('//link[@rel="next"]/@href').get()
        if next_page is not None:
            yield scrapy.Request(next_page, callback=self.parse)


    def parse_appartment_page(self, response):
        characteristics_dic = {}

        id = response.xpath('//li[@class="identificator-oferta"]/text()').get()
        characteristics_dic['id'] = id

        characteristics_dic['url'] = response.request.url

        price = response.xpath('//div[@class="pret first blue"]/text()').extract()[0]
        characteristics_dic['price'] = price

        neighborhood = response.xpath('//a[@href="#zona"]/span/text()').get()
        characteristics_dic['neighborhood'] = neighborhood

        app_characteristics = response.css('div#b_detalii_caracteristici').css('li')
        for app_characteristic in app_characteristics:
            for characteristic in characteristics:
                if characteristic in app_characteristic.get():
                    characteristics_dic[characteristic] = app_characteristic.css('span::text').get()

        yield characteristics_dic

def fix_json(filename, today):
    f = open(filename, 'r')
    contents = f.readlines()
    f.close()

    contents.insert(0, "{\""+today+"\":")
    contents.append("}")
    f = open(filename, 'w')
    f.writelines(contents)
    f.close()

def lambda_handler(event, context):
    execute()

def execute(local=False):
    filename = 'result.json'
    location = '/tmp/'
    full_path = location + filename
    today = date.today().strftime('%Y%m%d')

    if os.path.exists(full_path):
        os.remove(full_path)

    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)',
        'FEED_FORMAT': 'json',
        'FEED_URI': full_path
    })

    process.crawl(AppartmentsSpider)
    process.start() # the script will block here until the crawling is finished

    fix_json(full_path, today)
    save_json(json.load(open(location + filename)), today, local)

if __name__ == "__main__":
    execute(True)

