import re
import scrapy
import logging

from itemloaders import ItemLoader
from scrapy.loader import ItemLoader
from GPUsCrawling.items import GpuVideoGraphicItem

logger = logging.getLogger()

class GpusSpider(scrapy.Spider):
    name = "GPUsSpider"
    allowed_domains = ["www.newegg.com"]
    start_urls = ["https://www.newegg.com/GPUs-Video-Graphics-Cards/SubCategory/ID-48"]
   
    def __init__(self, pages="1,100"):
        rangePages = pages.split(",")
        for i in range(int(rangePages[0]), int(rangePages[1]) + 1):
            list_url = "https://www.newegg.com/GPUs-Video-Graphics-Cards/SubCategory/ID-48/Page-" + str(i)
            print(list_url)
            self.start_urls.append(list_url)

    def parse_detail(self, response, item):

        max_resolution = ""
        display_port = ""
        hdmi = ""
        direct_x = ""
        model = ""

        gpu_item_details = response.css("#product-details .tab-panes")

        count = len(gpu_item_details.xpath('//*[@id="product-details"]/div[1]/div').extract())

        if count == 3:
            element = 2
        else:
            element = 1

        for table_horizontal in gpu_item_details.css(".tab-pane:nth-child(" + str(element) + ") .table-horizontal"):
            if table_horizontal.css("caption::text").extract_first() == "Model":
                tr = table_horizontal.css("tbody tr")
                for r in tr:
                    th = r.css("th::text").extract_first()
                    if th == "Model":
                        td = r.css("td::text").extract_first()
                        model = td

            elif table_horizontal.css("caption::text").extract_first() == "Ports":
                tr = table_horizontal.css("tbody tr")
                for r in tr:
                    th = r.css("th::text").extract_first()
                    if th == "DisplayPort":
                        td = r.css("td::text").extract_first()
                        display_port = td
                    if th == "HDMI":
                        td = r.css("td::text").extract_first()
                        hdmi = td

            elif table_horizontal.css("caption::text").extract_first() == "3D API":
                tr = table_horizontal.css("tbody tr")
                for r in tr:
                    th = r.css("th::text").extract_first()
                    if th == "DirectX":
                        td = r.css("td::text").extract_first()
                        direct_x = td

            elif table_horizontal.css("caption::text").extract_first() == "Details":
                tr = table_horizontal.css("tbody tr")
                for r in tr:
                    th = r.css("th::text").extract_first()
                    if th == "Max Resolution":
                        td = r.css("td::text").extract_first()
                        max_resolution = td
            else:
                continue

        item["others"] = {
            "MaxResolution": max_resolution,
            "DisplayPort": display_port,
            "HDMI": hdmi,
            "DirectX": direct_x,
            "Model": model
        }

        yield item  


    def parse(self, response):
        
        for gpu_item in response.css(".item-cell"):

            detail_url = gpu_item.css(".item-title::attr(\"href\")").extract_first()

            price = gpu_item.css(".price-current strong::text").extract_first()
            price_decimal = gpu_item.css(".price-current sup::text").extract_first()

            price_current = "0"
            if price and price_decimal:
                price_current = re.sub("\D", "", price) + price_decimal

            itemId = gpu_item.css(".item-container::attr(\"id\")").extract_first()

            logger.info("itemId: " + str(itemId))

            if itemId is not None:
                item_loader = ItemLoader(item=GpuVideoGraphicItem(), selector=gpu_item)
                #item_loader.add_css("item_id", ".item-title::attr(\"href\")")
                item_loader.add_value("itemId", itemId)
                item_loader.add_css("title", ".item-title::text")
                item_loader.add_css("rating", ".item-rating::attr(\"title\")")
                item_loader.add_css("rating_num", ".item-rating-num::text")
                item_loader.add_css("branding", ".item-brand img::attr(\"title\")")
                item_loader.add_css("shipping", ".price-ship::text")
                item_loader.add_css("imageUrl", ".item-img img::attr(\"src\")")
                item_loader.add_value("priceCurr", price_current)
                item_loader.add_value("url", detail_url)
                item_loader.add_value("referer", response.url)

                # Redirect to item detail
                yield scrapy.Request(detail_url, callback=self.parse_detail, cb_kwargs=dict(item=item_loader.load_item()))
            else:
                logger.info("To continue Item " + str(detail_url))
                continue    