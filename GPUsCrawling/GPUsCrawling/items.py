# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
import re

from urllib.parse import unquote, urlparse
from pathlib import PurePosixPath
from itemloaders.processors import TakeFirst, MapCompose

def brand_transfered(value):
    if(value is None):
        return ""
    return value

def item_id_transfered(value):
    paths = PurePosixPath(unquote(urlparse(value).path)).parts
    return paths[-1]

def price_shipping_transfered(value):
    if(value is None):
        return 0
    if(value == "Free Shipping"):
        value = "0"
    if(value == "Special Shipping"):
        value = "0"
    else:
        price_shipping_string_list = re.findall(r'[\d\.\d]+', value)
        if(price_shipping_string_list[0]):
            return float(price_shipping_string_list[0])
        else:
            return 0
    return float(value)

def rating_transfered(value):
    if(value is None):
        return 0
    rating_string_list = ["0"]
    if(value):
        rating_string_list = re.findall(r'[\d\.\d]+',value)
    # if(len(rating_string_list)) {
    #     return 0
    # }
    if(rating_string_list[0]):
        return float(rating_string_list[0])
    return 0

def rating_num_transfered(value):
    if(value is None):
        return 0
    num = int(re.sub("\D","",value))
    if(num):
        return num
    return 0

def image_transfered(value):
    if(value is None):
        return ""
    return value

def title_transfered(value):
    if(value is None):
        return ""
    return value

def price_transfered(value):
    if(value is None):
        return 0
    return float(value)

def url_transfered(value):
    if(value is None):
        return ""
    return value

def referer_transfered(value):
    if(value is None):
        return ""
    return value

class GpuVideoGraphicItem(scrapy.Item):

    # define the fields for your item here like:
    itemId = scrapy.Field(input_processor = MapCompose(item_id_transfered), output_processor=TakeFirst())
    title = scrapy.Field(default=None, input_processor = MapCompose(title_transfered), output_processor=TakeFirst())
    branding = scrapy.Field(default=None, input_processor = MapCompose(brand_transfered), output_processor=TakeFirst())
    rating = scrapy.Field(default=None, input_processor = MapCompose(rating_transfered), output_processor=TakeFirst())
    rating_num = scrapy.Field(default=None, input_processor = MapCompose(rating_num_transfered), output_processor=TakeFirst())
    priceCurr = scrapy.Field(default=None, input_processor = MapCompose(price_transfered), output_processor=TakeFirst())
    shipping = scrapy.Field(default=None, input_processor = MapCompose(price_shipping_transfered), output_processor=TakeFirst())
    imageUrl = scrapy.Field(default=None, input_processor = MapCompose(image_transfered), output_processor=TakeFirst())
    url = scrapy.Field(default=None, input_processor = MapCompose(url_transfered), output_processor=TakeFirst())
    referer = scrapy.Field(default=None, input_processor = MapCompose(referer_transfered), output_processor=TakeFirst())
    others = scrapy.Field(default=None, output_processor=TakeFirst())