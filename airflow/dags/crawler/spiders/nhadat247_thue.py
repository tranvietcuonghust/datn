from scrapy import Request
from scrapy import Spider
from scrapy.selector import Selector
from scrapy import FormRequest
# from selenium.webdriver.chrome.service import Service as ChromeService
# from selenium.webdriver.chrome.options import Options
# from selenium import webdriver
# from webdriver_manager.chrome import ChromeDriverManager
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support.select import Select
from crawler_daily.items import Nhadat247_Thue_Item
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
    name = "nhadat247_thue_daily"
    allowed_domains = ["nhadat247.com.vn"]
    start_urls = [
        "https://nhadat247.com.vn/nha-dat-cho-thue/?order=NgayDang-desc",
    ]
    def __init__(self,feed_uri=None):
        # options = Options()
        # options.headless = True
        self.links_list = []
        self.page_limit = 0
        self.limit_date = "Hadsfôm kifasdfa"
        self.name = "nhadat247_thue_daily"
        # self.custom_settings = {
        #         'FEED_URI': feed_uri,
        #         'FEED_FORMAT': 'csv'
        # }
        # self.driver = webdriver.Chrome(executable_path='/usr/local/bin/chromedriver_linux64',options=options)




    def parse(self, response):
        print(response.url)
        self.page_limit = self.page_limit + 1
        
        links= Selector(response).xpath('//div[contains(@class,"content-left")]/div[contains(@class,"for-user")]/ul/li/a/@href').extract()
        for link in links:
            # print(i)
            # i=i+1
            
            link= "https://nhadat247.com.vn"+link
            self.links_list.append(link)
            # yield Request(url=link, callback=self.parse_news)
            # yield item
        

       
        next_page =  Selector(response).xpath('//div[contains(@class,"pager_controls")]/a[contains(text(),"Trang sau")]/@href').extract_first()
        print("*********")
        print(len(links))
        print(next_page)
        print("************")
        if self.page_limit > 2 and self.limit_date in response.body.decode('utf-8'):
                next_page = None
        if  next_page is not None:
            print(next_page)
            next_page = "https://nhadat247.com.vn" + next_page
            yield Request(next_page, callback=self.parse)
        if next_page is None:
            print("num link = " + str(len(self.links_list)) )
            for link in self.links_list:
                yield Request(url=link, callback=self.parse_news)
        # yield Request(url="https://laptoptcc.com/cua-hang/laptop-hp-elitebook-840-g5-i5-8350u-8gb-256gb-14-fhd/", callback=self.parse_laptop)


    def parse_news(self, response):
        print("**************crawling "+response.url+"***********")

        URL=response.url
        

        
        Title = Selector(response).xpath('//div[contains(@class,"content-left")]/div[contains(@class,"product-detail")]/h1/text()').get()
        Created_date=Selector(response).xpath('//*[@id="ContentPlaceHolder1_ProductDetail1_divprice"]/div/text()[2]').get()
        Crawled_date = datetime.date.today()
        Post_id=Selector(response).xpath('//*[@id="ContentPlaceHolder1_ProductDetail1_divprice"]/div/text()').get()
        Price = Selector(response).xpath('//*[@id="ContentPlaceHolder1_ProductDetail1_divprice"]/span[contains(@class,"spanprice")]/text()').get()
        Acreage = Selector(response).xpath('//*[@id="ContentPlaceHolder1_ProductDetail1_divprice"]/span[2]/text()').get()
        Location_elements = Selector(response).xpath('//*[@id="ContentPlaceHolder1_ProductDetail1_divlocation"]/descendant-or-self::*/text()').getall()
        Location = ''.join( _ for _ in Location_elements).strip()
        # for Location_element in Location_elements:
        #     Location = Location +"-"+Location_element  
            
        RE_Type = Selector(response).xpath('//div[contains(@class,"pd-dacdiem")]/table/tbody/tr[./td[1]/b[contains(text(),"Loại tin rao")]]/td[2]/text()').get()
        Legal = Selector(response).xpath('//div[contains(@class,"pd-dacdiem")]/table/tbody/tr[./td[1]/b[contains(text(),"Pháp lý")]]/td[2]/text()').get()
        Street_size = Selector(response).xpath('//div[contains(@class,"pd-dacdiem")]/table/tbody/tr[./td[1]/b[contains(text(),"Đường vào")]]/td[2]/text()').get()
        Facade = Selector(response).xpath('//div[contains(@class,"pd-dacdiem")]/table/tbody/tr[./td[1]/b[contains(text(),"Mặt tiền")]]/td[2]/text()').get()
        Direction = Selector(response).xpath('//div[contains(@class,"pd-dacdiem")]/table/tbody/tr[./td[1]/b[contains(text(),"Hướng nhà")]]/td[2]/text()').get()
        Num_rooms = Selector(response).xpath('//div[contains(@class,"pd-dacdiem")]/table/tbody/tr[./td[1]/b[contains(text(),"Số phòng")]]/td[2]/text()').get()
        Num_toilets = Selector(response).xpath('//div[contains(@class,"pd-dacdiem")]/table/tbody/tr[./td[1]/b[contains(text(),"Số toilet")]]/td[2]/text()').get()
        Num_floors = Selector(response).xpath('//div[contains(@class,"pd-dacdiem")]/table/tbody/tr[./td[1]/b[contains(text(),"Số tầng")]]/td[2]/text()').get()
        Description = Selector(response).xpath('//div[contains(@class,"pd-desc-content")]/text()').get()
        # //*[@id="tbl2"]/tbody/tr[1]/td[2]/b
        Contact_name = Selector(response).xpath('//div[contains(@class,"pd-contact")]//tr[./td[1]/b[contains(text(),"Tên liên hệ")]]/td[2]/b/text()').get()
        Contact_phone = Selector(response).xpath('//div[contains(@class,"pd-contact")]//tr[./td[1]/b[contains(text(),"Điện thoại")]]/td[2]/text()').get()
        Contact_email = Selector(response).xpath('//div[contains(@class,"pd-contact")]//tr[./td[1]/b[contains(text(),"Email")]]/td[2]/text()').get()
        Contact_address = Selector(response).xpath('//div[contains(@class,"pd-contact")]//tr[./td[1]/b[contains(text(),"Địa chỉ")]]/td[2]/text()').get()
        Contact_city = Selector(response).xpath('//div[contains(@class,"pd-contact")]//tr[./td[1]/b[contains(text(),"Tỉnh thành")]]/td[2]/text()').get()
        Contact_type = Selector(response).xpath('//div[contains(@class,"pd-contact")]//tr[./td[1]/b[contains(text(),"Loại tin")]]/td[2]/text()').get()

        # options = self.driver.find_elements(By.XPATH, '//*[@id="cau-hinh"]/option[contains(@class,"attached")]')
        
            # response = Selector(text=html)
         
        item = Nhadat247_Thue_Item()
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
        item['Facade'] = Facade
        item['Direction'] = Direction
        item['Num_rooms']= Num_rooms
        item['Num_toilets'] = Num_toilets
        item['Num_floors']= Num_floors
        item['Description'] = Description
        item['Contact_name']= Contact_name
        item['Contact_phone'] = Contact_phone
        item['Contact_email'] = Contact_email
        item['Contact_address'] = Contact_address
        item['Contact_city'] = Contact_city
        item['Contact_type']= Contact_type
        # self.driver.execute_script("arguments[0].setAttribute('aria-checked', 'false')", li)
        yield item


    # def closed(self, reason):
    #     # Close the browser when the spider is done
    #     self.driver.quit()       


   