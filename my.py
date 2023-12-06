import io
import logging
import os
import re
import time
import scrapy
import pickle

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from scrapy.selector import Selector
from selenium.webdriver.common.keys import Keys

from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from apiclient.http import MediaIoBaseDownload
import fitz
import cloud.scraper as scraper
from cloud.utils import CHROMIUM_PATH, get_filename_from_headers
 
 
 
class ESCNJSraper(scraper.DefaultScraper):
    name = "escnj"
    CLIENT_SECRET_FILE = "client_secret.json"
    API_NAME = "drive"
    API_VERSION = "v3"
    SCOPES = ["https://www.googleapis.com/auth/drive"]
    
    def start_requests(self):
        self.BASE_URL = "https://www.escnj.us/members"
        pickle_file = f"token_{self.API_NAME}_{self.API_VERSION}.pickle"
        cred = None
        if os.path.exists(pickle_file):
            with open(pickle_file, "rb") as token:
                cred = pickle.load(token)

        if not cred or not cred.valid:
            if cred and cred.expired and cred.refresh_token:
                cred.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(self.CLIENT_SECRET_FILE, self.SCOPES)
                cred = flow.run_local_server()

            with open(pickle_file, "wb") as token:
                pickle.dump(cred, token)
    
        service = build(self.API_NAME, self.API_VERSION, credentials=cred)
        logging.error(self.API_NAME.capitalize(), "service created successfully.\n")
        self.service = service
        yield scrapy.Request(url=self.BASE_URL, callback=self.start_parsing) 
        
        
    async def start_parsing(self, response):
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(
                headless=True,
                executable_path=CHROMIUM_PATH,
            )
            context = browser.new_context()
            self.context = context
            page = context.new_page()          
            page.goto(
                self.BASE_URL,
                timeout=60000,
                wait_until="networkidle",
            )
            
            #  Wait for iframe to appear
            handle = page.wait_for_selector("#minibaseSubmit3554")

            handle.click()
            page.wait_for_selector(".sw-flex-item-group")

            async for res in self.parse_page(page):
                    yield res
            page.close()
           
    @staticmethod
    def get_text_element(element_html):
        soup = BeautifulSoup(element_html, "html.parser")
        return soup.text

    @staticmethod
    def analysis_date(date_str):
        if ";" in date_str:
            datelist = date_str.split(";")
            effective = datelist[0].split("-")[0]
            expiration = datelist[-1].split("to")[-1]
        else:
            datelist = date_str.split("-")
            effective = datelist[0]
            expiration = datelist[0]
        return effective.strip(), expiration.strip()

    async def parse_content(self, table_element):
        try :         
            item = {
                "source_url": self.BASE_URL,
                "buyer_lead_agency": "Educational Services Commission of New Jersey (ESCNJ)",
                "buyer_lead_agency_state": "NJ",
                "service_area_state": ["NJ"],
                "service_area_national": False,
                "cooperative_language": True,
                "contract_type": "COMPETITIVELY_BID_CONTRACT",
                "buyer_contacts": [
                    {
                        "phone": "732-777-9848 ext. 3120",
                        "email": "coop@escnj.us",
                        "address": "1660 Stelton Road, Second Floor, Piscataway, New Jersey 08854",
                    }
                ],
                "cooperative_affiliation": "Educational Services Commission of New Jersey (ESCNJ)",
            }
            td_list = table_element.css("td")
            title = self.get_text_element(td_list[2].get())
            contract_number = self.get_text_element(td_list[4].get())
            date_string = self.get_text_element(td_list[6].get())
            link = td_list[10].css("a::attr(href)").get()
            effective, expiration = self.analysis_date(date_string)
            item["title"] = title
            item["contract_number"] = contract_number
            item["effective"] = effective
            item["expiration"] = expiration

            # ---------Start Analysis Files---------------
            
            docPageHandle = self.get_new_page_handle(
                "https://www.escnj.us" + link, "iframe"
            )
            links = docPageHandle.css("iframe::attr(src)").getall()
            left_url = links[0]
            right_url = links[1]
            
            # ---------Get Vendor Information-------------
            
            left_page_handler = self.get_new_page_handle(left_url, ".flip-entry")
            pdf_urls = left_page_handler.css(".flip-entry-info > a::attr(href)").getall()
            pdf_titles = left_page_handler.css(".flip-entry-title::text").getall()
            pdf_url = ''
            for i in range(len(pdf_titles)):
                if 'Vendor-Information' in pdf_titles[i] or 'Vendor Information' in pdf_titles[i] or 'Vendor-Contact' in pdf_titles[i] or 'Vendor Contact' in pdf_titles[i] or 'Contact-Information' in pdf_titles[i]or 'Contact Information' in pdf_titles[i] :
                    pdf_url = pdf_urls[i]
                    break
            if pdf_url == '' :
                item['supplier_contracts'] = []
                return
            
            #--------Get Supplier list-----------------------------------
            
            download_url = 'https://drive.google.com/u/0/uc?id=' + pdf_url.split('/')[5] + '&export=download'
            # download_url = 'https://drive.google.com/uc?id='+pdf_url.split('/')[5] + '&confirm=t'
            result = await self.parse_google_doc(download_url)
            
            suppliers = []
            items = []
            suppliers_count = len(result)
            for i in range(len(result)) :
                suppliers.append(result[i]['suppliers'])
                item['suppliers'] = result[i]['suppliers']
                item['supplier_contacts'] = [result[i]['contacts']]
                new_item = item.copy()
                items.append(new_item)
                
            
            #-------Get Contract list----------------------------------
            contract_folder_url = ''
            contract_file_url = ''
            for i in range(len(pdf_titles)):
                if 'Vendor Document' in pdf_titles[i] or 'Vendor Documentation' in pdf_titles[i] or 'Vendor-Document' in pdf_titles[i] or 'Vendor-Documentation' in pdf_titles[i]:
                    if 'folders' in pdf_urls[i]:
                        contract_folder_url = pdf_urls[i]
                    elif 'file' in pdf_urls[i]:
                        contract_file_url = pdf_urls[i]
                    break
            if contract_folder_url != '' :
                contract_folder_url = 'https://drive.google.com/embeddedfolderview?id=' + contract_folder_url.split('/')[5] + '#list'
                contract_page_handler = self.get_new_page_handle(contract_folder_url, ".flip-entry")
                pdf_urls = contract_page_handler.css(".flip-entry-info > a::attr(href)").getall()
                pdf_titles = contract_page_handler.css(".flip-entry-title::text").getall()
                
                for i in range(suppliers_count):
                    for j in range(len(pdf_titles)) :    
                        if self.compare_string(items[i]['suppliers'],pdf_titles[j]):
                            download_url = 'https://drive.google.com/u/0/uc?id=' + pdf_urls[j].split('/')[5] + '&export=download'
                                    
                            document = await self.download_pdf(download_url, items[i]) 
                            if document == None:
                                document = None
                            else:
                                items[i].update({'contract_files': [document]})
                            break
            elif contract_file_url != '' :
                download_url = 'https://drive.google.com/u/0/uc?id=' + contract_file_url.split('/')[5] + '&export=download'
                document = await self.download_pdf(download_url, items[0]) 
                if document == None :
                    document = None
                else :
                    items[0].update({'contract_files': [document]})  
            
            #--------Get Right Content's Documents list-----------------
            right_page_handler = self.get_new_page_handle(right_url, ".flip-entry")
            pdf_urls = right_page_handler.css(".flip-entry-info > a::attr(href)").getall()
            pdf_titles = right_page_handler.css(".flip-entry-title::text").getall()
            pdf_url = ''
            
            if suppliers_count == 1:
                for i in range(len(pdf_urls)):
                    download_url = 'https://drive.google.com/u/0/uc?id=' + pdf_urls[i].split('/')[5] + '&export=download'
                    document = await self.download_pdf(download_url, items[0]) 
                    if document == None :
                        document = None
                    else :
                        items[0].update({self.get_document_type(pdf_titles[i]): [document]})  
                    
            else :
                for i in range(suppliers_count):
                    for j in range(len(pdf_titles)) :
                        if self.compare_string(items[i]['suppliers'],pdf_titles[j]):
                            if 'folders' in pdf_urls[j] :
                                contract_folder_url = 'https://drive.google.com/embeddedfolderview?id=' + pdf_urls[j].split('/')[5] + '#list'
                                await self.download_folder(contract_folder_url,items[i])
                                break 
                            elif 'file' in pdf_urls[j] :
                                download_url = 'https://drive.google.com/u/0/uc?id=' + pdf_urls[j].split('/')[5] + '&export=download'
                                document = await self.download_pdf(download_url, items[0]) 
                                if document == None :
                                    document = None
                                else :
                                    items[0].update({self.get_document_type(pdf_titles[j]): [document]})  
                            
            for item in items:
                yield item           
        except :
            # p = 'd'
            logging.error("Error")
    async def parse_google_doc(self, url):
         
        request = self.service.files().get_media(fileId=ID)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
        doc = fitz.open(stream=fh, filetype="pdf")
        text = ""
        for page in doc:
            text += page.get_text()

    
        seek_pos = 0
        result = []
        if 'Vendor' in text:
            seek_pos = text.index('Vendor') 
        elif 'RFP' in text: 
            seek_pos = text.index('RFP') 
        else: return result 
        while 'Bid' in text[seek_pos:] :
            pos1 = text.index('Vendor',seek_pos)
            pos2 = text.index('Representative',pos1)
            suppliers = text[(pos1+6):(pos2-1)].replace('\n','').strip()
            pos1  = pos2
            
            
            pos2 = text.index('Address',pos1)
            name = text[(pos1+14):(pos2-1)].replace('\n','').strip()
            pos1  = pos2
        
            pos2 = text.index('Telephone #',pos1)
            address = text[(pos1+7):(pos2-1)].replace('\n','').strip()
            pos1  = pos2
            
            pos2 = text.index('Fax #',pos1)
            phone = text[(pos1+11):(pos2-1)].replace('\n','').strip()
            pos1  = pos2            
            
            pos1 = text.index('Email',pos1)
            pos2 = text.index('Website',pos1)
            email = text[(pos1+5):(pos2-1)].replace('\n','').strip()
            

            pos1 = pos2
            if 'Bid' in text[pos1:]:
                pos2 =  text.index('Bid',pos1)
                website = text[(pos1+7):(pos2-1)].replace('\n','').strip()
            elif 'RFP' in text[pos1:]:
                pos2 =  text.index('RFP',pos1)
                website = text[(pos1+7):(pos2-1)].replace('\n','').strip()
            else:
                website = text[(pos1+7):].replace('\n','').strip()
            supplier_contracts = {}
            supplier_contracts['name'] = name
            supplier_contracts['address'] = address
            supplier_contracts['phone'] = phone
            supplier_contracts['email'] = email
            supplier_contracts['website'] = 'https://' + website
            
            result.append({
                'suppliers' : suppliers,
                'contacts' : supplier_contracts
            }) 
            seek_pos = pos2
        
        return result
            
    def compare_string(self,string1, string2):
        result1 = [word for s in string1.split() for word in s.split("-")]
        result2 = [word for s in string2.split() for word in s.split("-")]
        return result1[0] == result2[0]
    def get_new_page_handle(self, link, selector):
        page = self.context.new_page()
        page.goto(
            link,
            timeout=60000,
            wait_until="networkidle",
        )
        # #  Wait for iframe to appear
        handle = page.wait_for_selector(selector)
        selector = Selector(text=page.content())
        time.sleep(0.1)
        page.close()
        
        return selector

    async def parse_table(self, page):
        selector = Selector(text=page.content())
        tables = selector.css("table")
         
        
        for i in range(len(tables)) :
            async for res in self.parse_content(tables[i]):
                yield res

    async def parse_page(self, page):
        selectorOfTable = Selector(text=page.content())
        async for res in self.parse_table(page):
            yield res
       

        next_page_button = selectorOfTable.css('a[aria-label="Go to Page 2"]')
        skip_page_button = []
        page_number = 2
        # skip_page_button =  selectorOfTable.css('a[aria-label="Skip to Page 2"]').get()
        while len(next_page_button) > 0 or len(skip_page_button) > 0:
            if len(next_page_button) > 0:
                button = page.wait_for_selector(
                    'a[aria-label="Go to Page ' + str(page_number) + '"]'
                )
                button.click()
                time.sleep(0.5)
                async for res in self.parse_table(page):
                    yield res 
                page_number = page_number + 1
                selector = Selector(text=page.content())
                next_page_button = selector.css(
                    'a[aria-label="Go to Page ' + str(page_number) + '"]'
                )
                skip_page_button = selector.css(
                    'a[aria-label="Skip to Page ' + str(page_number) + '"]'
                )
            elif len(skip_page_button) > 0:
                button = page.wait_for_selector(
                    'a[aria-label="Skip to Page ' + str(page_number) + '"]'
                )
                button.click()
                time.sleep(0.5)
                async for res in self.parse_table(page):
                    yield res 
                page_number = page_number + 1
                selector = Selector(text=page.content())
                next_page_button = selector.css(
                    'a[aria-label="Go to Page ' + str(page_number) + '"]'
                )
                skip_page_button = selector.css(
                    'a[aria-label="Skip to Page ' + str(page_number) + '"]'
                )
            else:
                break
    async def download_pdf(self, url, item):
        try:
            request = scrapy.Request(url=url, method="GET")
            response = await self.crawler.engine.download(request, self)

            if response.status == 200:
                filename = os.path.basename(url.split("?")[0])
                file_args = {
                    "document_name": filename,
                    "document_body": io.BytesIO(response.body),
                    "human_name": filename,
                }

                document = await self.create_document(item, **file_args)
                return document
            else:
                return None
        except:
             
            return None
     
    def get_document_type(self,name):
        pricing_includes = ["price", "pricing",'award']
        name = name.lower()
        for include_key in pricing_includes:
            if include_key in name:
                return "pricing_files"

        return "other_docs_files"

    async def download_folder(self,url,item):
        contract_page_handler = self.get_new_page_handle(url, ".flip-entry")
        pdf_urls = contract_page_handler.css(".flip-entry-info > a::attr(href)").getall()
        pdf_titles = contract_page_handler.css(".flip-entry-title::text").getall()
        
        for j in range(len(pdf_titles)) :    
            download_url = 'https://drive.google.com/u/0/uc?id=' + pdf_urls[j].split('/')[5] + '&export=download'
            
            document = await self.download_pdf(download_url, item) 
            if document == None:
                document = None
            else:
                item.update({self.get_document_type(pdf_titles[j]): [document]})
            break
    async def download_file(self,ID):
        request = self.service.files().get_media(fileId=ID)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
        doc = fitz.open(stream=fh, filetype="pdf")
        text = ""
        for page in doc:
            text += page.get_text()
        logging.error(text)