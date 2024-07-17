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
from crawler_daily.items import Alomuabannhadat_Thue_Item
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
    name = "alomuabannhadat_thue_daily"
    allowed_domains = ["alomuabannhadat.vn"]
    start_urls = [
        "https://alomuabannhadat.vn/cho-thue/"
    ]
    def __init__(self,feed_uri=None, *args, **kwargs):
        # options = Options()
        # options.headless = True
        self.links_list = []
        self.page_limit = 0
        self.limit_date = "Ccccập nhật: 2 ngr"
        self.name = "alomuabannhadat_thue_daily"
        # self.custom_settings = {
        #     'FEED_URI': feed_uri,
        #     'FEED_FORMAT': 'csv'
        # }
        # self.driver = webdriver.Chrome(executable_path='/usr/local/bin/chromedriver_linux64',options=options)




    def parse(self, response):
        print(response.url)
        self.page_limit = self.page_limit + 1
        
        

        products= Selector(response).xpath('//*[@id="properties-search"]/div[contains(@class,"wrap-property")]/div[contains(@class,"property")]')
        for product in products:
            data ={}
            link = product.xpath('./div[contains(@class,"property-image")]/a/@href').get()
            location = product.xpath('./div[contains(@class,"info")]/aside/ul/li[2]/text()').get()
            data['link']=link
            data['location']=location
            # print(i)
            # i=i+1
            
            # link= "https://nhadat247.com.vn"+link
            self.links_list.append(data)
            # yield Request(url=link, callback=self.parse_news)
            # yield item    
        

              
        next_page =  Selector(response).xpath('//*[@id="properties-search"]//ul[contains(@class,"pagination")]/ul[contains(@class,"pagination")]/li[./a[contains(text(),"»")]]/a/@href').extract_first()
        print("*********")
        print(len(products))
        print(next_page)
        print("************")
        if self.page_limit > 2 and self.limit_date in response.body.decode('utf-8'):
            next_page = None
        if self.page_limit == 500:
            next_page = None
        # next_page = None
        if  next_page is not None and next_page is not "https://alomuabannhadat.vn/cho-thue/":
            print(next_page)
            # next_page = "https://nhadat247.com.vn" + next_page
            yield Request(next_page, callback=self.parse)
        if next_page is None or next_page == "https://alomuabannhadat.vn/cho-thue/":
            print("num link = " + str(len(self.links_list)) )
            for link in self.links_list:
                url = link['link']
                location = link['location']
                yield Request(url=url, callback=self.parse_news,meta={'location': location})
        # yield Request(url="https://laptoptcc.com/cua-hang/laptop-hp-elitebook-840-g5-i5-8350u-8gb-256gb-14-fhd/", callback=self.parse_laptop)


    def parse_news(self, response):
        print("**************crawling "+response.url+"***********")

        URL=response.url
        

         
        Title = Selector(response).xpath('//*[@id="property-detail"]/header[contains(@class,"property-title")]/h1/text()').get()
        Created_date=Selector(response).xpath('//*[@id="quick-summary"]/dl/dt[contains(text(),"Ngày đăng:")]/following-sibling::dd[1]/text()').get()
        Crawled_date = datetime.date.today().strftime("%d-%m-%Y")
        Post_id=Selector(response).xpath('//*[@id="quick-summary"]/dl/dt[contains(text(),"Mã tài sản:")]/following-sibling::dd[1]/text()').get()
        Price = Selector(response).xpath('//*[@id="quick-summary"]/dl/dd/span[contains(@class,"tag price")]/text()').get()
        Acreage = Selector(response).xpath('//*[@id="quick-summary"]/dl/dt[contains(text(),"Diện tích:")]/following-sibling::dd[1]/text()').get()
        Location_element = Selector(response).xpath('//*[@id="quick-summary"]/dl/dt[contains(text(),"Vị trí:")]/following-sibling::dd[1]/text()').get()
        Location = response.meta['location']
        Location = Location +"-"+Location_element  
        
        
        RE_Type = Selector(response).xpath('//*[@id="property-features"]/ul/li[contains(text(),"Loại địa ốc")]/text()').get()
        Legal = Selector(response).xpath('//*[@id="quick-summary"]/dl/dt[contains(text(),"Pháp lý:")]/following-sibling::dd[1]/text()').get()
        Vertical_length = Selector(response).xpath('//*[@id="property-features"]/ul/li[contains(text(),"Chiều ngang")]/text()').get()
        Horizontal_length = Selector(response).xpath('//*[@id="property-features"]/ul/li[contains(text(),"Chiều dài")]/text()').get()
        Direction = Selector(response).xpath('//*[@id="property-featcontactures"]/ul/li[contains(text(),"Hướng xây dựng")]/text()').get()
        Num_rooms = Selector(response).xpath('//*[@id="property-features"]/ul/li[contains(text(),"Số phòng ngủ")]/text()').get()
        Num_toilets = Selector(response).xpath('//*[@id="property-features"]/ul/li[contains(text(),"Số phòng vệ sinh")]/text()').get()
        Num_floors = Selector(response).xpath('//*[@id="property-features"]/ul/li[contains(text(),"Số lầu")]/text()').get()
        
        Description = Selector(response).xpath('string(//*[@id="description"]/p)').get()
        
        Contact_name = Selector(response).xpath('//*[@id="contact-agent"]/div/section/div[1]/aside/div/a/h4/strong/text()').get()
        
        Contact_phone = Selector(response).xpath('//*[@id="contact-agent"]/div/section/div[1]/aside/div/figure[1]/span[contains(@class,"phone-number")]/a/@onclick').get()
        Contact_email = Selector(response).xpath('//*[@id="contact-agent"]/div/section/div[1]/aside/div/figure[2]/a/text()').get()
        Contact_address = Selector(response).xpath('//*[@id="contact-agent"]/div/section/div[1]/aside/div/figure[3]/text()').get()
        # options = self.driver.find_elements(By.XPATH, '//*[@id="cau-hinh"]/option[contains(@class,"attached")]')
        
            # response = Selector(text=html)
         
        item = Alomuabannhadat_Thue_Item()
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
        item['Direction'] = Direction
        item['Vertical_length'] = Vertical_length
        item['Horizontal_length'] = Horizontal_length
        item['Num_rooms']= Num_rooms
        item['Num_toilets'] = Num_toilets
        item['Num_floors']= Num_floors
        item['Description'] = Description
        item['Contact_name']= Contact_name
        item['Contact_phone'] = Contact_phone
        item['Contact_email'] = Contact_email
        item['Contact_address'] = Contact_address
        # self.driver.execute_script("arguments[0].setAttribute('aria-checked', 'false')", li)
        yield item


    # def closed(self, reason):
    #     # Close the browser when the spider is done
    #     self.driver.quit()       


