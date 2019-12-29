import boto3
import scrapy
from datetime import date
from scrapy.crawler import CrawlerProcess

s3 = boto3.client('s3')

class AppartmentsSpider(scrapy.Spider):
    name = "appartments"

    start_urls = [
        'https://www.imobiliare.ro/vanzare-apartamente/craiova',
    ]

    def parse(self, response):
        for appartment in response.css('a.img-block'):
            appartment_page = appartment.css('a::attr(href)').get()
            yield scrapy.Request(appartment_page, callback=self.parse_appartment_page)
            break

    def parse_appartment_page(self, response):
        price = response.xpath('//div[@class="pret first blue"]/text()').extract()[0]
        characteristics = response.css('div#b_detalii_caracteristici')
        #if characteristics:
        #    print("## " + characteristics.get())
        yield {
            'number_of_rooms': characteristics.css('li')[0].css('span::text').get(),
            'usable_surface': characteristics.css('li')[1].css('span::text').get(),
            'total_surface': characteristics.css('li')[2].css('span::text').get(),
            'partitioning': characteristics.css('li')[3].css('span::text').get(),
            'confort': characteristics.css('li')[4].css('span::text').get(),
            'floor': characteristics.css('li')[5].css('span::text').get(),
            'number_of_kitchens': characteristics.css('li')[6].css('span::text').get(),
            'number_of_bathrooms': characteristics.css('li')[7].css('span::text').get(),
            'year_of_construction': characteristics.css('li')[8].css('span::text').get(),
            'structure': characteristics.css('li')[9].css('span::text').get(),
            'building_type': characteristics.css('li')[10].css('span::text').get(),
            'height': characteristics.css('li')[11].css('span::text').get(),
            'number_of_balconies': characteristics.css('li')[12].css('span::text').get(),
            #'preturi': preturi
        }

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
