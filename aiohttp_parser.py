import time
import asyncio
from wsgiref import headers
import aiohttp
import random
import requests
from bs4 import BeautifulSoup

aa=[]
bb=[]
xxx=1

def get_HEADERS():
    """     UA_LIST = [
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0) Gecko/20100101 Firefox/77.0',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
        ] """
    UA_LIST = []
    with open('user-agents.txt','r') as USER_AGENTS:
        for USER_AGENT in USER_AGENTS:
            UA_LIST.append(USER_AGENT[:-2])
    HEADERS = {
        'authority': 'www.amazon.com.tr',
        'method': 'GET',
        'scheme': 'https',
        'accept': 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7',
        'ect': '4g',
        'sec-ch-ua': '"Opera GX";v="83", "Chromium";v="97", ";Not A Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'image',
        'sec-fetch-mode': 'no-cors',
        'sec-fetch-site': 'same-origin',
        'User-Agent': random.choice(UA_LIST)
        }
    return HEADERS

def number_of_pages(URL):
    while True:
        RESPONSE = requests.get(URL, headers=get_HEADERS())
        REF = BeautifulSoup(RESPONSE.text, "html.parser").find('a', class_='s-pagination-next')
        if RESPONSE.status_code == 200 and REF != None:
            TOTAL_NUMBER_OF_PAGES = REF.previous_sibling.text
            return int(TOTAL_NUMBER_OF_PAGES)
        else:
            del RESPONSE

def generating_urls(URL, TOTAL_NUMBER_OF_PAGES):
    #print("Kullanılacak URL'ler üretiliyor.")
#   Generates the urls of the wanted pages to scrape
    PAGE_INDEX_OF_URL = URL.index("&page=")
    URL_PART_1 = URL[:int(PAGE_INDEX_OF_URL+6)]
    URL_PART_2 = URL[int(PAGE_INDEX_OF_URL+7):]
    del PAGE_INDEX_OF_URL
    for i in range(1, int(TOTAL_NUMBER_OF_PAGES)+1):
        URLS.append(URL_PART_1 + str(i) + URL_PART_2)
    del URL_PART_1, URL_PART_2

async def async_get(FINAL_URL, SESSION):
    global aa, bb, xxx
    async with SESSION.get(FINAL_URL) as RESPONSE:
        if RESPONSE.status != 200:
            bb.append(str(RESPONSE.status))
            print(str(RESPONSE.status) + " başaramadık rıza baba "+asyncio.current_task().get_name())
            FAILED_TASK_ = asyncio.create_task(async_get(FINAL_URL, SESSION),name=xxx)
            xxx=xxx+1
            await asyncio.gather(FAILED_TASK)
            return await RESPONSE.text()
        elif RESPONSE.status == 200:
            aa.append(str(RESPONSE.status))
            print(str(RESPONSE.status), asyncio.current_task().get_name())
            return await RESPONSE.text()

async def async_main():
    async with aiohttp.ClientSession() as SESSION:
        TASKS_ = []
        for FINAL_URL in URLS:
            TASKS_.append(asyncio.create_task(async_get(FINAL_URL, SESSION)))
        print("TASKS_: "+str(len(TASKS_)))
        return await asyncio.gather(*TASKS_)

def listing_cards(RESULTS):
#   print("Sonuçlar ayıklanıyor")
#   Filtering htmls and saving product cards.
    print("len(RESULTS): "+str(len(RESULTS)))
    for RESULT in RESULTS: 
        try:
            print("result: "+ str(len(RESULT)))
            CARDS.extend(BeautifulSoup(RESULT, features="html.parser").find_all('div', {'data-asin': True,  'data-component-type': 's-search-result'}))
        except:
            print("result: "+str(RESULT))
    return CARDS

def processing_cards(CARDS):
#   Processing cards and filtering title, data-asin and price values.
    #print("Ayıklanan ürünler işleniyor.")
    for CARD in CARDS:
        try:
            TITLE = CARD.find('img', {'alt': True}).attrs["alt"]
        except:
            TITLE = "Not Found"
        try:
            DATA_ASIN = CARD.attrs["data-asin"]
        except:
            DATA_ASIN = "Not Found"
        try:
            PRICE_WHOLE = CARD.find('span', class_='a-price-whole').text[0:-1]
            PRICE_WHOLE = ''.join(PRICE_WHOLE.split('.'))
        except:
            PRICE_WHOLE = "Not Found"
        try:
            PRICE_FRACTION = CARD.find('span', class_='a-price-fraction').text
        except:
            PRICE_FRACTION = "Not Found"
        try:
            PRICE_SYMBOL = CARD.find('span', class_='a-price-symbol').text
        except:
            PRICE_SYMBOL = "Not Found"
        try:
            LINK = "https://www.amazon.com.tr" + str(CARD.find('a', {'href': True}).attrs["href"])
        except:
            LINK = "Not Found"

        if TITLE == "Not Found" or DATA_ASIN == "Not Found" or PRICE_WHOLE == "Not Found" or PRICE_FRACTION == "Not Found" or PRICE_SYMBOL == "Not Found" or LINK == "Not Found":
            pass
        else:
            PRICE = PRICE_WHOLE + "." + PRICE_FRACTION
            DATA = {'data-asin': DATA_ASIN, 'title': TITLE, 'link': LINK, 'price': PRICE, 'price_symbol': PRICE_SYMBOL}
            PRODUCTS.append(DATA)

def main(URL):

    global URLS, CARDS, PRODUCTS, RESULTS

    URLS = []
    DATA = {}
    PRODUCTS = []
    CARDS = []
    TASKS_ = []
    RESULTS = []

#   Retrieving total page number
    TOTAL_NUMBER_OF_PAGES = number_of_pages(URL)
#   Iterating and generating urls with page numbers
    generating_urls(URL, TOTAL_NUMBER_OF_PAGES)
#   Defining loop and retrieving result htmls
    start = time.time()
    #runner(1, TOTAL_NUMBER_OF_PAGES)
    loop = asyncio.get_event_loop()
    RESULTS = loop.run_until_complete(async_main())
    finish = time.time()
    listing_cards(RESULTS)
    processing_cards(CARDS)
    total_time = finish-start
    #for PRODUCT in PRODUCTS:
        #print(PRODUCT)
    #print("Saat : " + str(time.localtime().tm_hour) + "." + str(time.localtime().tm_min) + "\tTaranan ürün sayısı : " + str(len(PRODUCTS)) + "\t" + "\tToplam işlem süresi : " + str(total_time))

    PRODUCT_COUNT = str(len(PRODUCTS))
    del URLS, DATA, CARDS, TASKS_
    print("aa: "+str(len(aa)))
    print("bb: "+str(len(bb)))
    print("PRODUCT_COUNT: "+PRODUCT_COUNT)
    #return PRODUCTS, PRODUCT_COUNT, str(round(total_time, 0))

if __name__ == "__main__":
    URL = "https://www.amazon.com.tr/s?i=computers&bbn=12601951031&rh=n%3A12601951031%2Cp_6%3AA1UNQM1SR2CHM%7CA3O5TP4R0OZYXZ&dc&page=2&qid=1647209639&rnid=15358539031&ref=sr_pg_2"
    URL2 = "https://www.amazon.com.tr/s?i=computers&bbn=12601898031&rh=n%3A12601898031&dc&fs=true&page=2&qid=1647450701&rnid=15358539031&ref=sr_pg_2"
    main(URL)
