# -*- coding: utf-8 -*-
import pandas as pd
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.linkextractors.lxmlhtml import LxmlLinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from scrapy.loader import ItemLoader
import logging
from scrapy.utils.log import configure_logging
from logging.handlers import RotatingFileHandler 
from buyersguidechem.items import BuyersguidechemItem

class BuyersguidechemcrawlSpider(CrawlSpider):  
    
    name = 'chemicalsCrawl'
    allowed_domains = ['www.buyersguidechem.com']
    start_urls = [
                # 'https://www.buyersguidechem.com/ABClisteEin.php?buchstabex=A', 
                # 'https://www.buyersguidechem.com/ABClisteEin.php?buchstabex=B', 
                # 'https://www.buyersguidechem.com/ABClisteEin.php?buchstabex=C',
                #  'https://www.buyersguidechem.com/ABClisteEin.php?buchstabex=D',
                # 'https://www.buyersguidechem.com/ABClisteEin.php?buchstabex=E',
                # 'https://www.buyersguidechem.com/ABClisteEin.php?buchstabex=F',
                'https://www.buyersguidechem.com/ABClisteEin.php?buchstabex=G',
                'https://www.buyersguidechem.com/ABClisteEin.php?buchstabex=H',
                'https://www.buyersguidechem.com/ABClisteEin.php?buchstabex=I',
                'https://www.buyersguidechem.com/ABClisteEin.php?buchstabex=J',
                'https://www.buyersguidechem.com/ABClisteEin.php?buchstabex=K',
                'https://www.buyersguidechem.com/ABClisteEin.php?buchstabex=L',
                'https://www.buyersguidechem.com/ABClisteEin.php?buchstabex=M',
                'https://www.buyersguidechem.com/ABClisteEin.php?buchstabex=N',
                'https://www.buyersguidechem.com/ABClisteEin.php?buchstabex=O',
                # 'https://www.buyersguidechem.com/ABClisteEin.php?buchstabex=P',
                # 'https://www.buyersguidechem.com/ABClisteEin.php?buchstabex=Q',
                # 'https://www.buyersguidechem.com/ABClisteEin.php?buchstabex=R',
                # 'https://www.buyersguidechem.com/ABClisteEin.php?buchstabex=S',
                # 'https://www.buyersguidechem.com/ABClisteEin.php?buchstabex=T',
                # 'https://www.buyersguidechem.com/ABClisteEin.php?buchstabex=U',
                # 'https://www.buyersguidechem.com/ABClisteEin.php?buchstabex=V',
                # 'https://www.buyersguidechem.com/ABClisteEin.php?buchstabex=W',
                #  'https://www.buyersguidechem.com/ABClisteEin.php?buchstabex=X',
                #  'https://www.buyersguidechem.com/ABClisteEin.php?buchstabex=Y',
                #  'https://www.buyersguidechem.com/ABClisteEin.php?buchstabex=Z',
                
                ]

    # def parse(self, response):
        
    #     allchemicals = response.xpath('//strong/a/@href').getall()
    #     if allchemicals:
    #         for chem in allchemicals:
    #             next_page = response.urljoin(chem)
    #             yield scrapy.Request(next_page, callback=self.parse_first_level)

    def parse(self, response):
        
        allchemicals = response.xpath('//div[@class="prod_line"]/a/@href').getall()
        if allchemicals:
            for chem in allchemicals:
                next_page = response.urljoin(chem)
                yield scrapy.Request(next_page, callback=self.parse_second_level)

    def parse_second_level(self, response):
        
        allchemicals = response.xpath('//div[@class="prod_line"]/a/@href').getall()
        if allchemicals:
            for chem in allchemicals:
                #chemLoader = ItemLoader(item=BuyersguidechemItem(), response=response)
                next_page = response.urljoin(chem)
                yield scrapy.Request(next_page, callback=self.parse_item)
    
    def parse_item(self, response):
        chemLoader = ItemLoader(item=BuyersguidechemItem(), response=response)
        if self._get_cas_no(response) is not None:
            chemLoader.add_value('cas',self._get_cas_no(response))
            chemLoader.add_value('chemicalName',self._get_chemical_name(response))
            chemLoader.add_value('suppliers',self._get_supplier_details(response))                        
        else:
            chemLoader.add_value('chemicalName',self._get_chemical_name(response))
            chemLoader.add_value('suppliers',self._get_supplier_details(response))
        yield chemLoader.load_item()
            


    def _get_cas_no(self,response):
        propertiesData = response.xpath('//td[@align="right"]/table[@style="BORDER-COLLAPSE: collapse; width:100%;"]').extract()
        df = pd.read_html(propertiesData[0])[0]
        cas = df[df.get(0)=='CAS'].get(1).get(1) 
        if cas:
            return cas.strip()

        return None
        # cas_no = response.xpath('//tr/td[@style="font-size:9pt;"][@align="left"]/text()').get()
        # if cas_no is not None:
        #     return cas_no.strip()
        # else:
        #     return cas_no

    def _get_chemical_name(self, response):
        chemicalNames = response.xpath('//div[@class="prod_line"][not (@style="background-color:#FFFF99; width:100%; vertical-align:top;")]/a/span[@class="col_prod"]/text()').getall()
        if chemicalNames:
            return chemicalNames
        else:
            return response.xpath('//div[@style="width: 380px; font-size: 9pt; "]/h1/text()').getall()

    def _get_supplier_details(self, response):
        supplierList=[]
        supplierDetails = response.xpath('//div[@class="lief_line"]')
        for details in supplierDetails:
                if self._validate(response) :
                    supplierList.append({"name":details.xpath('span[@class="col_name1"]/text()').get().strip(),"country":details.xpath('span[@class="col_land"]/text()').get().strip()})
                else:
                    supplierList.append({"name":details.xpath('a/span[@class="col_name1"]/text()').get().strip(),"country":details.xpath('a/span[@class="col_land"]/text()').get().strip()})
        return supplierList


    def _get_validation_cas(self,response):
        return response.xpath('//td[@valign="top"]/div/span[@style="margin-left:12pt;"]/h1/text()').get()

    def _validate(self,response):
        chemicalNames = response.xpath('//div[@class="prod_line"][not (@style="background-color:#FFFF99; width:100%; vertical-align:top;")]/a/span[@class="col_prod"]/text()').getall()
        if chemicalNames and self._get_cas_no(response)==self._get_validation_cas(response):
            return True
        else:
            return False



    