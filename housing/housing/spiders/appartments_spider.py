import scrapy

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
        if characteristics:
            print("## " + characteristics.get())
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
            'preturi': preturi
        }

