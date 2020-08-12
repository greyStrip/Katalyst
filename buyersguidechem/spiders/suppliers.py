# -*- coding: utf-8 -*-
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from scrapy.loader import ItemLoader

from buyersguidechem.items import BuyersguidechemItem

class BuyersguidechemcrawlSpider(CrawlSpider):
    name = 'suppliersCrawl'
    allowed_domains = ['www.buyersguidechem.com']
    start_urls = ['https://www.buyersguidechem.com']

    rules = (
        Rule(LinkExtractor(allow=('/region/')), callback='parse_item', follow=True),
    )

    def parse_item(self, response):
        # buyersGuideChemSuppliersList code start
        zone = response.xpath('//span[@style="font-size:10px;"]/a/text()').getall()
        country = response.xpath('//div[@class="col_land"][@style="margin-bottom:4px;"]/text()').getall()
        suppliers = response.xpath('//div[@class="lief_line"]/a/div[@class="col_name1"]/text()').getall()
       
        for supplier in suppliers:
            countryLoader = ItemLoader(item=BuyersguidechemItem(), response=response)
            countryLoader.add_value('zone',zone)
            countryLoader.add_value('country',country)
            countryLoader.add_value('supplier',supplier)
            yield countryLoader.load_item()
        # buyersGuideChemSuppliersList code end

        
