import scrapy
from scrapy.loader.processors import MapCompose, Join, TakeFirst

#buyersGuideChemSuppliersList items
# def extract_country(value):
#     if isinstance(value,list):
#         if 'BGC' in value:
#             value.remove('BGC')
#         if 'Region' in value:
#             value.remove('Region')
#     return value

# class BuyersguidechemItem(scrapy.Item):
#     zone = scrapy.Field(
#         input_processor = extract_country,
#         output_processor = TakeFirst(),
#     )
#     country = scrapy.Field(
#         output_processor = TakeFirst(),
#     )
#     supplier = scrapy.Field(
#         output_processor = TakeFirst(),
#     )

# buyersGuideChemSuppliersList items

def extract_value(value):
    return ''.join(map(str,value))

class BuyersguidechemItem(scrapy.Item):
    cas = scrapy.Field(
        output_processor= TakeFirst(),
    )
    chemicalName = scrapy.Field()
    suppliers = scrapy.Field()
