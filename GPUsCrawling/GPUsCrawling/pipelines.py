# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import pyodbc
import logging
import json

from itemadapter import ItemAdapter

logger = logging.getLogger()

class SavingMSSQLPipeline(object):
    def __init__(self):
        self.conn = None
        self.curr = None
        self.create_connection()
        logger.info("Connect database successfully!")

    ## Create connection string with database
    def create_connection(self):
        server = 'localhost'
        database = 'Crawling'
        username = 'sa'
        password = '123456'
        conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
        self.conn = pyodbc.connect(conn_str)
        self.curr = self.conn.cursor()

    def process_item(self, item, spider):
        check = self.duplicate_item(item)
        
        #logger.info("process_item = %s" + str(check))

        if check == 1:
            spider.logger.warn("Item already in database: " + item['itemId'])
        else:
            spider.logger.warn("Store Item: " + item['itemId'])
            self.store_db(item)

        #return item

    ## Check to see if itemID is already in database 
    def duplicate_item(self, item):
        logger.info("Checking duplicate item!")
        try:
            self.curr.execute("SELECT COUNT(1) FROM [Crawling].[dbo].[GPU] where itemId = ?", (item['itemId']))
            result = self.curr.fetchone()
        except pyodbc.Error as e:
            error_message = str(e)
            logger.error("Error occurred:", error_message)
            logger.error("SQL State:", e.args[0])
            logger.error("Error Message:", e.args[1])

        return result    

    ## Storing item into database
    def store_db(self, item):
        others_serialized = json.dumps(item['others']) if isinstance(item['others'], dict) else item['others']

        try:
            SQL_STATEMENT = """ INSERT INTO [Crawling].[dbo].[GPU]([ItemID],[Title],[Branding],[Rating],[RatingNum],[PriceCurr],[Shipping],[ImageUrl],[GPUUrl],[Referer],[Others]) VALUES (?,?,?,?,?,?,?,?,?,?,?) """
            self.curr.execute(SQL_STATEMENT, item['itemId'] if 'itemId' in item else "NaN", item['title'] if 'title' in item else "NaN", item['branding']  if 'branding' in item else "NaN", item['rating'] if 'rating' in item else "0", item['rating_num'] if 'rating_num' in item else "0", item['priceCurr']  if 'priceCurr' in item else "0", item['shipping']  if 'shipping' in item else "NaN", item['imageUrl'] if 'imageUrl' in item else "NaN", item['url']  if 'url' in item else "NaN", item['referer'] if 'referer' in item else "NaN", others_serialized)
            self.conn.commit()
        except pyodbc.Error as e:
            error_message = str(e)
            logger.error("Error occurred:", error_message)
            logger.error("SQL State:", e.args[0])
            logger.error("Error Message:", e.args[1])


    ## Close cursor & connection to database 
    def close_spider(self, spider):    
        self.curr.close()
        self.conn.close()