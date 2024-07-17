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
from crawler_daily.items import Nhadatvui_Ban_Item
# from selenium.webdriver.chrome.webdriver import WebDriver as ChromeDriver
# from webdriver_manager.chrome import ChromeDriverManager
# from datetime import datetime, timedelta
import datetime
import uuid
import json
aa=1

# <form class="variations_form" action="https://l" method="post" enctype="multipart/form-data" data-product_id="41423" \
#     data-product="[
# {"attributes":{"attribute_cau-hinh":"Core i7-1355U, RAM 16GB, SSD 1TB, FHD [21.900.000]"} },
# {"attributes":{"attribute_cau-hinh":"Core i5-1335U, RAM 8GB, SSD 512GB, FHD [14.900.000]"}}
# ]" current-image="41432">s
	

class CrawlerSpider(Spider):
    name = "nhadatvui_ban_daily"
    allowed_domains = ["nhadatvui.vn"]
    start_urls = [
        "https://nhadatvui.vn/mua-ban-nha-dat",
    ]
    def __init__(self,feed_uri=None):
        # options = Options()
        # options.headless = True
        # service = Service(executable_path='/home/tranvietcuong/chromedriver/chromedriver')
        self.links_list = []
        self.page_limit = 0
        yesterday= datetime.datetime.now() - datetime.timedelta(days=2)
        # self.limit_date = yesterday.strftime('%d/%m/%Y') 
        self.limit_date = "Asdasfa"
        self.name = "nhadatvui_ban_daily"
        # self.custom_settings = {
        #         'FEED_URI': feed_uri,
        #         'FEED_FORMAT': 'csv'
        # }
        # self.driver = webdriver.Chrome(service=service, options=options)



    def parse(self, response):
        print(response.url)
        self.page_limit = self.page_limit + 1
        print(self.limit_date)
        links= Selector(response).xpath('//*[@id="wrapper"]//div[contains(@class,"main-search-product")]//div[contains(@class,"box-container-inner")]/div[contains(@class,"box-item")]/a/@href').extract()
        for link in links:
            
            # print(i)
            # i=i+1
            
            print(link)
            self.links_list.append(link)
            # yield Request(url=link, callback=self.parse_news)
            # yield item
        
        
        next_page =  Selector(response).xpath('//a[contains(@aria-label,"pagination.next")]/@href').extract_first()
        # next_page = None
        print("*********")
        print(len(links))
        print(next_page)
        print("************")
        if self.page_limit == 500:
                next_page=None
        if self.page_limit > 2 and self.limit_date in response.body.decode('utf-8'):
            next_page = None
        if  next_page is not None:
            print(next_page)
            
          
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
        
        Title = Selector(response).xpath('//*[@id="wrapper"]//div[contains(@class,"product-title-price")]/div[contains(@class,"left-title-price")]/h1/text()').get()
        
        Created_date=Selector(response).xpath('//div[contains(@class,"product-status")]/div[./span[1][contains(text(),"Ngày đăng")]]/span[2]/text()').get()
        Crawled_date = datetime.date.today()
        Post_id=Selector(response).xpath('//div[contains(@class,"product-status")]/div[./span[1][contains(text(),"Mã tin")]]/span[2]/text()').get()
        
        Price = Selector(response).xpath('//*[@id="wrapper"]//span[contains(@class,"price")]/text()').get()
        
        Acreage = Selector(response).xpath('//*[@id="content-tab-info"]/div/div/ul/li[./span[1]/span[contains(text(),"Diện tích")]]/span[2]/text()').get()
        Location = ""
        Location_elements = Selector(response).xpath('//*[@id="wrapper"]//div[contains(@class,"product-title-price")]/div[contains(@class,"left-title-price")]//div[./i[contains(@class,"fa-custom-pin1")]]/span/text()').get()
        Location = Location+"-"+Location_elements

        
        RE_Type = Selector(response).xpath('//*[@id="wrapper"]/div[2]/div/div/div[1]/div[1]/ul/li[2]/a/span/text()').get()

        Legal = Selector(response).xpath('//*[@id="content-tab-info"]/div/div/ul/li[./span[1]/span[contains(text(),"Giấy tờ pháp lý")]]/span[2]/text()').get()
        Street_size = Selector(response).xpath('//*[@id="content-tab-info"]/div/div/ul/li[./span[1]/span[contains(text(),"Đường rộng")]]/span[2]/text()').get()
        Vertical_length = Selector(response).xpath('//*[@id="content-tab-info"]/div/div/ul/li[./span[1]/span[contains(text(),"Chiều rộng")]]/span[2]/text()').get()
        Horizontal_length = Selector(response).xpath('//*[@id="content-tab-info"]/div/div/ul/li[./span[1]/span[contains(text(),"Chiều dài")]]/span[2]/text()').get()

        Direction = Selector(response).xpath('//*[@id="content-tab-info"]/div/div/ul/li[./span[1]/span[contains(text(),"Hướng")]]/span[2]/text()').get()
        Num_rooms = Selector(response).xpath('//*[@id="content-tab-info"]/div/div/ul/li[./span[1]/span[contains(text(),"Phòng ngủ")]]/span[2]/text()').get()
        Num_toilets = Selector(response).xpath('//*[@id="content-tab-info"]/div/div/ul/li[./span[1]/span[contains(text(),"Phòng tắm")]]/span[2]/text()').get()
        Num_floors = Selector(response).xpath('//*[@id="content-tab-info"]/div/div/ul/li[./span[1]/span[contains(text(),"Số tầng")]]/span[2]/text()').get()
        
        Description = Selector(response).xpath('string(//*[@id="content-tab-custom"])').getall()
        # //*[@id="tbl2"]/tbody/tr[1]/td[2]/b
        
        Contact_name = Selector(response).xpath('//*[@id="wrapper"]//div[contains(@class,"show-user-info")]/div[2]/div/a/text()').get()
        
        Contact_phone = Selector(response).xpath('//*[@id="btn-click-statistic"]/@data-phone').get()
        

        # options = self.driver.find_elements(By.XPATH, '//*[@id="cau-hinh"]/option[contains(@class,"attached")]')
        
            # response = Selector(text=html)
         
        item = Nhadatvui_Ban_Item()
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


   