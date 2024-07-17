from scrapy import Request
from scrapy import Spider
from scrapy.selector import Selector
from scrapy import FormRequest
# from selenium.webdriver.chrome.service import Service
# from selenium.webdriver.chrome.service import Service as ChromeService
# from selenium.webdriver.chrome.options import Options
# from selenium import webdriver
# from webdriver_manager.chrome import ChromeDriverManager
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support.select import Select
from crawler_daily.items import Nhadatban24h_Ban_Item
# from selenium.webdriver.chrome.webdriver import WebDriver as ChromeDriver
# from webdriver_manager.chrome import ChromeDriverManager

import datetime
import uuid
import json
aa=1

# <form class="variations_form" action="https://l" method="post" enctype="multipart/form-data" data-product_id="41423" \
#     data-product="[
# {"attributes":{"attribute_cau-hinh":"Core i7-1355U, RAM 16GB, SSD 1TB, FHD [21.900.000]"} },
# {"attributes":{"attribute_cau-hinh":"Core i5-1335U, RAM 8GB, SSD 512GB, FHD [14.900.000]"}}
# ]" current-image="41432">
	

class CrawlerSpider(Spider):
    name = "nhadatban24h_thue_daily"
    allowed_domains = ["nhadatban24h.vn"]
    start_urls = [
        "https://nhadatban24h.vn/nha-dat-cho-thue-ci36.html",
    ]
    def __init__(self,feed_uri=None):
        # options = Options()
        # options.headless = True
        # service = Service(executable_path='/home/tranvietcuong/chromedriver/chromedriver')
        self.links_list = []
        self.limit_date = "2 ngày trước"
        self.page_limit = 0
        self.name = "nhadatban24h_thue_daily"
        # self.custom_settings = {
        #         'FEED_URI': feed_uri,
        #         'FEED_FORMAT': 'csv'
        # }
        # self.driver = webdriver.Chrome(service=service, options=options)



    def parse(self, response):
        print(response.url)
        self.page_limit = self.page_limit + 1
        
        links= Selector(response).xpath('//div[contains(@class,"re__card-info-content")]/h3[contains(@class,"re__card-title")]/a[contains(@class,"pr-title")]/@href').extract()
        for link in links:
            
            # print(i)
            # i=i+1
            
            link= "https://nhadatban24h.vn"+link
            print(link)
            self.links_list.append(link)
            # yield Request(url=link, callback=self.parse_news)
            # yield item
        
        # next_page = None
        next_page =  Selector(response).xpath('//div[contains(@class,"re__pagination-group")]/a[contains(@title,"Trang tiếp")]/@href').extract_first()
        print("*********")
        print(len(links))
        print(next_page)
        print("************")
        if self.page_limit > 1 and self.limit_date in response.body.decode('utf-8'):
            next_page = None
        if  next_page is not None:
            print(next_page)
            next_page = "https://nhadatban24h.vn/nha-dat-cho-thue-ci36.html" + next_page
            yield Request(next_page, callback=self.parse)
        if next_page is None:
            print("num link = " + str(len(self.links_list)) )
            for link in self.links_list:
                yield Request(url=link, callback=self.parse_news)
        # yield Request(url="https://laptoptcc.com/cua-hang/laptop-hp-elitebook-840-g5-i5-8350u-8gb-256gb-14-fhd/", callback=self.parse_laptop)


    def parse_news(self, response):
        print("**************crawling "+response.url+"***********")

        URL=response.url
        
        # self.driver.get(response.url)
        # try:
        #     wait = WebDriverWait(self.driver, 10)
        #     wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="main-content"]//div[contains(@class,"card-container")]/div[contains(@class,"base-card-wrapper")]')))
        # except:
        #     pass
        Title = Selector(response).xpath('//div[contains(@class,"re__pr-info")]/h3[contains(@class,"re__pr-title pr-title")]/text()').get()
        
        Created_date=Selector(response).xpath('//div[contains(@class,"re__pr-short-info")]/div[./span[contains(text(),"Ngày cập nhật")]]/span[contains(@class,"value")]/text()').get()
        Crawled_date = datetime.date.today()
        Post_id=Selector(response).xpath('//div[contains(@class,"re__pr-short-info")]/div[./span[contains(text(),"Mã tin đăng")]]/span[contains(@class,"value")]/text()').get()
        Price = Selector(response).xpath('//div[contains(@class,"re__pr-short-info")]/div[./span[contains(text(),"Mức giá")]]/span[contains(@class,"value")]/text()').get()
        Acreage = Selector(response).xpath('//div[contains(@class,"re__pr-short-info")]/div[./span[contains(text(),"Diện tích")]]/span[contains(@class,"value")]/text()').get()
        Location = ""
        Location_elements = Selector(response).xpath('//span[./i[contains(@class,"fa-map-marker")]]//text()').extract()
        
        
        for Location_element in Location_elements:

            Location = Location+"-"+Location_element 
        
        RE_Type = Selector(response).xpath('//span[contains(@class,"re__pr-specs-product-type")]/a/text()').get()
        
        Legal = Selector(response).xpath('//div[contains(@class,"re__pr-specs-content")]/div[./span[contains(text(),"Pháp lý")]]/span[contains(@class,"re__pr-specs-content-item-value")]/text()').get()
        Street_size = Selector(response).xpath('//div[contains(@class,"re__pr-specs-content")]/div[./span[contains(text(),"Đường trước nhà")]]/span[contains(@class,"re__pr-specs-content-item-value")]/text()').get()
        Facade = Selector(response).xpath('//div[contains(@class,"re__pr-specs-content")]/div[./span[contains(text(),"Pháp lý")]]/span[contains(@class,"re__pr-specs-content-item-value")]/text()').get()
        Vertical_length = Selector(response).xpath('//div[contains(@class,"re__pr-specs-content")]/div[./span[contains(text(),"Chiều ngang")]]/span[contains(@class,"re__pr-specs-content-item-value")]/text()').get()
        Horizontal_length = Selector(response).xpath('//div[contains(@class,"re__pr-specs-content")]/div[./span[contains(text(),"Chiều dài")]]/span[contains(@class,"re__pr-specs-content-item-value")]/text()').get()
        Direction = Selector(response).xpath('//div[contains(@class,"re__pr-specs-content")]/div[./span[contains(text(),"Hướng nhà")]]/span[contains(@class,"re__pr-specs-content-item-value")]/text()').get()
        Num_rooms = Selector(response).xpath('//div[contains(@class,"re__pr-specs-content")]/div[./span[contains(text(),"Phòng ngủ")]]/span[contains(@class,"re__pr-specs-content-item-value")]/text()').get()
        Num_toilets = Selector(response).xpath('//div[contains(@class,"re__pr-specs-content")]/div[./span[contains(text(),"Nhà tắm")]]/span[contains(@class,"re__pr-specs-content-item-value")]/text()').get()
        Num_floors = Selector(response).xpath('//div[contains(@class,"re__pr-specs-content")]/div[./span[contains(text(),"Số tầng")]]/span[contains(@class,"re__pr-specs-content-item-value")]/text()').get()
        Description = Selector(response).xpath('//div[contains(@class,"js__pr-description")]//text()').getall()
        # //*[@id="tbl2"]/tbody/tr[1]/td[2]/b
        Contact_name = Selector(response).xpath('//div[contains(@class,"re__contact-name")]/text()').get()
        Contact_phone = Selector(response).xpath('//a[contains(@class,"phoneLinkpopup")]/@href').get()
        

        # options = self.driver.find_elements(By.XPATH, '//*[@id="cau-hinh"]/option[contains(@class,"attached")]')
        
            # response = Selector(text=html)
         
        item = Nhadatban24h_Ban_Item()
        item['URL'] = URL 
        item['Title']= Title
        item['Created_date'] = Created_date
        item['Crawled_date']= Crawled_date
        item['Post_id'] = Post_id
        item['Price']= Price
        item['Acreage']= Acreage
        item['Location'] = Location
        item['RE_Type']= RE_Type
        item['Legal'] = Legal
        item['Street_size']= Street_size
        item['Vertical_length'] = Vertical_length
        item['Horizontal_length'] = Horizontal_length
        item['Facade'] = Facade
        item['Direction'] = Direction
        item['Num_rooms']= Num_rooms
        item['Num_toilets'] = Num_toilets
        item['Num_floors']= Num_floors
        item['Description'] = Description
        item['Contact_name']= Contact_name
        item['Contact_phone'] = Contact_phone
        # self.driver.execute_script("arguments[0].setAttribute('aria-checked', 'false')", li)
        yield item


    # def closed(self, reason):
    #     # Close the browser when the spider is done
    #     self.driver.quit()       


   