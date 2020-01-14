import boto3
import scrapy
from datetime import date
from scrapy.crawler import CrawlerProcess

s3 = boto3.client('s3')

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

    start_urls = [
        'https://www.imobiliare.ro/vanzare-apartamente/craiova',
    ]

    def parse(self, response):
        for appartment in response.css('a.img-block'):
            appartment_page = appartment.css('a::attr(href)').get()
            yield scrapy.Request(appartment_page, callback=self.parse_appartment_page)
            #break

        next_page = response.xpath('//link[@rel="next"]/@href').get()
        if next_page is not None:
            yield scrapy.Request(next_page, callback=self.parse)

    def parse_appartment_page(self, response):
        characteristics_dic = {}

        id = response.css('input#homesters-ofertaID::attr(value)').get()
        characteristics_dic['id'] = id

        price = response.xpath('//div[@class="pret first blue"]/text()').extract()[0]
        characteristics_dic['price'] = price
        
        app_characteristics = response.css('div#b_detalii_caracteristici').css('li')
        for app_characteristic in app_characteristics:
            for characteristic in characteristics:
                if characteristic in app_characteristic.get():
                    characteristics_dic[characteristic] = app_characteristic.css('span::text').get()

        yield characteristics_dic

def lambda_handler(event, context):
    location = '/tmp/result.json'

    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)',
        'FEED_FORMAT': 'json',
        'FEED_URI': location
    })

    process.crawl(AppartmentsSpider)
    process.start() # the script will block here until the crawling is finished

    data = open(location, 'rb')
    #print(data)
    s3.put_object(Bucket='andrei-housing-prices', Key=date.today().strftime('%Y%m%d'), Body=data)
    print('All done.')

if __name__ == "__main__":
    lambda_handler('', '')
