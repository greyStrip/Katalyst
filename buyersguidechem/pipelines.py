import pymongo
import cx_Oracle
import logging
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

class BuyersguidechemPipeline(object):
    def process_item(self, item, spider):
        return item

class MongoPipeline(object):

    collection_name = 'masterData'

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE', 'items')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        
        # self.db[self.collection_name].create_index(
        # [("cas", pymongo.ASCENDING), ("chemicalName", pymongo.ASCENDING)],unique=True)

        self.db[self.collection_name].create_index([("chemicalName", pymongo.ASCENDING)],unique=True)
        logging.log(logging.DEBUG,"Inserting data in masterData table start"+str(item))
        self.db[self.collection_name].insert_one(ItemAdapter(item).asdict())
        logging.log(logging.DEBUG,"Inserting data in masterData table end")
        return item

class DuplicatesPipeline(object):

    def __init__(self):
        self.ids_seen = set()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        #logging.log(logging.DEBUG,"ids_seen: "+self.ids_seen)
        #logging.log(logging.DEBUG,"adapter: "+adapter)
        if (set(adapter['chemicalName']).issubset(self.ids_seen)):
            raise DropItem("Duplicate item found: %r" % item['cas'])
        else:
            self.ids_seen.add(adapter['chemicalName'])
            return item

class DropInvalidPipeline(object):
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        if not adapter["country"]:
            raise DropItem("Invalid item found")
        else:
            return item

class MongoSupplierPipeline(object):

    collection_name = 'supplierData'

    def __init__(self, mongo_uri, mongo_db):
        #self.logger = logging.getLogger()
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE', 'items')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        
        self.db[self.collection_name].create_index([("supplier", pymongo.ASCENDING)],unique=True)
        #self.logger("Inserting data in supplierData table start"+str(item))
        self.db[self.collection_name].insert_one(ItemAdapter(item).asdict())
        #self.logger("Inserting data in supplierData table end")
        return item

class OraclePipeline(object):
    zone_sql = "insert into country_data(country, country_zone) values (:country, :country_zone)"
    supplier_sql = "insert /*+ ignore_row_on_dupkey_index (supplier_data,SUPPLIER_PK) */ into supplier_data values (:supplier_name, :country)"
    chemical_sql = "insert /*+ ignore_row_on_dupkey_index (chemical_data,CHEMICAL_PK) */ into chemical_data( chemical_cas_no, chemical_name, chemical_synonyms) values(:chemical_cas_no, :chemical_name, :chemical_synonyms)"
    mapping_sql = "insert /*+ ignore_row_on_dupkey_index (chemical_supplier_mapping,PK_MAPPING) */ into chemical_supplier_mapping( cas_no, supplier_name) values (:cas_no, :supplier_name)"
    others_sql = "insert /*+ ignore_row_on_dupkey_index (chemical_without_cas,PK_OTHERS) */ into chemical_without_cas( chemical_name, chemical_synonyms, supplier_name) values(:chemical_name, :chemical_synonyms, :supplier_name)"
    connection =''

    def __init__(self, oracle_username, oracle_password, oracle_uri):
        self.oracle_username = oracle_username
        self.oracle_password = oracle_password
        self.oracle_uri = oracle_uri

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            oracle_username=crawler.settings.get('ORACLE_USERNAME'),
            oracle_password=crawler.settings.get('ORACLE_PASSWORD'),
            oracle_uri=crawler.settings.get('ORACLE_URI'),
        )

    def open_spider(self, spider):
        try:
            cx_Oracle.init_oracle_client(
                lib_dir="/Users/shalutripathi/Client_Library/Oracle_Client_Library/instantclient_19_3", 
                config_dir="/Users/shalutripathi/Client_Library/Oracle_Client_Library/mydir/", 
                error_url="https://example.com/install.html",
                driver_name="MyApp : 1.0",
            )
            self.connection = cx_Oracle.connect(self.oracle_username, self.oracle_password, self.oracle_uri)
            print('Successfully connected ')
        except cx_Oracle.Error as error:
            print('Connection Error Occured ')
            print(error)

    def close_spider(self, spider):
        self.connection.close()

    # def process_item_supplier(self, item, spider):
    #     #zone_sql = "insert into country_data(country, country_zone) values (:country, :country_zone)"
    #     supplier_sql = "insert into supplier_data( supplier_name, supplier_country) values (:supplier_name, :country)"
    #     country = item['country']
    #     # country_zone = item['zone'] 
    #     supplier_name = item['supplier']
    #     try:
    #         # cursor = self.connection.cursor()
    #         # cursor.execute(zone_sql,[country,country_zone])
    #         # self.connection.commit()

    #         cursor2 = self.connection.cursor()
    #         cursor2.execute(supplier_sql,[supplier_name,country])
    #         self.connection.commit()

    #         # cursor.close()
    #         cursor2.close()

    #     except cx_Oracle.Error as exception:
    #         self.connection.rollback()
    #         print('Error while executing db commands')
    #         print(exception)


    def process_item(self, item, spider): 
        if item.get('cas'):
            chemical_cas_no = item.get('cas')
        sortedSynonyms = list(sorted(item.get('chemicalName'), key = len))
        chemical_name = sortedSynonyms[0]
        synonyms = sortedSynonyms[1::]
        synonym = ','.join(map(str,synonyms))

        try:
            cursor = self.connection.cursor()
            suppliers = item.get('suppliers')
            for supplier in suppliers:
                supplier_name = supplier.get('name')
                country = supplier.get('country')
                cursor.execute(self.supplier_sql,[supplier_name,country])
                # logging.info("Supplier Data populated: %r" % supplier_name)
            if item.get('cas'):
                cursor.execute(self.chemical_sql,[chemical_cas_no,chemical_name,synonym])
                # logging.info("Chemical Data populated: %r" % chemical_cas_no)
                for supplier in suppliers:
                    cursor.execute(self.mapping_sql,[chemical_cas_no,supplier.get('name')])
                    # logging.info("Mapping Data populated: %r" % chemical_cas_no)
            else:
                for supplier in suppliers:
                    cursor.execute(self.others_sql,[chemical_name,synonym,supplier.get('name')])
                    # logging.info("Chemical Data Without cas populated: %r" % chemical_name)
            self.connection.commit()
        except cx_Oracle.Error as exception:
            self.connection.rollback()
            logging.error("Error while executing db commands: %r" % exception)
        finally:
            cursor.close()
            