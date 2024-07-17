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
from crawler_daily.items import Cafeland_Ban_Item
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
    name = "cafeland_ban_daily"
    allowed_domains = ["nhadat.cafeland.vn"]
    start_urls = [
        "https://nhadat.cafeland.vn/nha-dat-ban/",
    ]
    def __init__(self,feed_uri=None):
        # options = Options()
        # options.headless = True
        # service = Service(executable_path='/home/tranvietcuong/chromedriver/chromedriver')
        self.links_list = []
        self.check_dupicate = 0
        self.page_limit = 0
        self.limit_date = "2 asas ngày trước"
        self.name="cafeland_ban_daily"
        # self.custom_settings = {
        #         'FEED_URI': feed_uri,
        #         'FEED_FORMAT': 'csv'
        # }
        # self.driver = webdriver.Chrome(service=service, options=options)



    def parse(self, response):
        print(response.url)
        self.page_limit +=1
        if response.url == "https://nhadat.cafeland.vn/nha-dat-ban/":
            self.check_dupicate +=1
        links= Selector(response).xpath('//div[contains(@class,"property-list")]/div[contains(@class,"row-item")]/div[contains(@class,"info-real")]/div[contains(@class,"reales-title")]/a[contains(@class,"realTitle")]/@href').extract()
        for link in links:
            if self.check_dupicate ==2:
                continue
            
            # print(i)
            # i=i+1
            
            # link= "https://nhadatban24h.vn"+link
            print(link)
            self.links_list.append(link)
            # yield Request(url=link, callback=self.parse_news)
            # yield item
        
        
        next_page =  Selector(response).xpath('//ul[contains(@class,"pagination")]/li/a[contains(text(),"»")]/@href').extract_first()
        # next_page = None
        print("*********")
        print(len(links))
        print(next_page)
        print("************")
        if self.page_limit > 2 and self.limit_date in response.body.decode('utf-8'):
            next_page = None
        if  next_page is not None and next_page is not "https://nhadat.cafeland.vn/nha-dat-ban/":
            print(next_page)
            if self.check_dupicate==2:
                next_page = None
            # next_page = "https://nhadatban24h.vn/mua-ban-nha-dat-ci38.html" + next_page
            else:
                yield Request(next_page, callback=self.parse)
        if next_page is None or next_page == "https://nhadat.cafeland.vn/nha-dat-ban/":
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
        
        Title = Selector(response).xpath('//div[contains(@class,"detail-property")]//h1[contains(@class,"head-title")]/text()').get()
        Created_date=Selector(response).xpath('//div[contains(@class,"reales-location")]/div[contains(@class,"col-right")]/div[contains(@class,"infor")]/i[contains(text(),"Cập nhật:")]/text()').get()
        Crawled_date = datetime.date.today()
        Post_id=Selector(response).xpath('//div[contains(@class,"reales-location")]/div[contains(@class,"col-right")]/div[contains(@class,"infor")]/text()').get()
        
        Price = Selector(response).xpath('//div[contains(@class,"reals-info-group")]/div/div[./div[1][contains(text(),"Giá bán")]]/div[contains(@class,"infor-data")]/text()').get()
        Acreage = Selector(response).xpath('//div[contains(@class,"reals-info-group")]/div/div[./div[1][contains(text(),"Diện tích")]]/div[contains(@class,"infor-data")]/text()').get()
        Location = ""
        
        
        Location_elements = Selector(response).xpath('//div[contains(@class,"reales-location")]/div[contains(@class,"col-left")]/div[contains(@class,"infor")]/div[2]/text()').get()
        

        Location = Location+"-"+Location_elements
        
        
        RE_Type = Selector(response).xpath('//div[contains(@class,"reals-house-item opt-mattien")]/span[contains(@class,"value-item")]/text()').get()
        
        Legal = Selector(response).xpath('//div[contains(@class,"reals-house-item opt-phaply")]/span[contains(@class,"value-item")]/text()').get()
        Street_size = Selector(response).xpath('//div[contains(@class,"reals-house-item opt-duong")]/span[contains(@class,"value-item")]/text()').get()
        Direction = Selector(response).xpath('//div[contains(@class,"reals-house-item opt-huongnha")]/span[contains(@class,"value-item")]/text()').get()
        Num_rooms = Selector(response).xpath('//div[contains(@class,"reals-house-item opt-songu")]/span[contains(@class,"value-item")]/text()').get()
        if Num_rooms  is None:
            Num_rooms = Selector(response).xpath('//div[contains(@class,"reals-house-item opt-sopngu")]/span[contains(@class,"value-item")]/text()').get()
        Num_toilets = Selector(response).xpath('//div[contains(@class,"reals-house-item opt-sotoilet")]/span[contains(@class,"value-item")]/text()').get()
        Num_floors = Selector(response).xpath('//div[contains(@class,"reals-house-item opt-sotang")]/span[contains(@class,"value-item")]/text()').get()
        
        Description = Selector(response).xpath('string(//div[contains(@class,"reals-description")]/div[contains(@class,"content")])').getall()
        # //*[@id="tbl2"]/tbody/tr[1]/td[2]/b
        Contact_name = Selector(response).xpath('//div[contains(@class,"profile-info")]/div[contains(@class,"profile-name")]/a/h4/strong/text()').get()
        
        Contact_phone = Selector(response).xpath('//div[contains(@class,"profile-info")]/div[contains(@class,"profile-phone")]/a/@onclick').get()

        Contact_email = Selector(response).xpath('//div[contains(@class,"profile-info")]/div[contains(@class,"profile-email")]/a/text()').get()
        Contact_email_domain = Selector(response).xpath('//div[contains(@class,"profile-info")]/div[contains(@class,"profile-email")]/a/@data-hidden-domain').get()
        Contact_email = Contact_email + Contact_email_domain
        Contact_address = Selector(response).xpath('//div[contains(@class,"profile-info")]/div[contains(@class,"profile-addr")]/text()').get()
        

        # options = self.driver.find_elements(By.XPATH, '//*[@id="cau-hinh"]/option[contains(@class,"attached")]')
        
            # response = Selector(text=html)
         
        item = Cafeland_Ban_Item()
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
        item['Direction'] = Direction
        item['Num_rooms']= Num_rooms
        item['Num_toilets'] = Num_toilets
        item['Num_floors']= Num_floors
        item['Description'] = Description
        item['Contact_name']= Contact_name
        item['Contact_phone'] = Contact_phone
        item['Contact_email']= Contact_email
        item['Contact_address'] = Contact_address
        # self.driver.execute_script("arguments[0].setAttribute('aria-checked', 'false')", li)
        yield item


    # def closed(self, reason):
    #     # Close the browser when the spider is done
    #     self.driver.quit()       


   