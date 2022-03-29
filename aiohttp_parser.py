import time
import asyncio
import aiohttp
import random
import requests
from bs4 import BeautifulSoup

def get_HEADERS():
    UA_LIST = []
    with open('user-agents-updated.txt','r') as USER_AGENTS:
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
    for i in range(1, int(TOTAL_NUMBER_OF_PAGES)+1):
        URLS.append(URL + "&page={}".format(i))

async def async_get(FINAL_URL, SESSION):
    x = 0
    while x < 5:
        async with SESSION.get(FINAL_URL, headers=get_HEADERS()) as RESPONSE:  
            if RESPONSE.status == 200:
                f.writelines((str(RESPONSE.status) + " " + asyncio.current_task().get_name()) + " " + str(x) + " " + str(FINAL_URL) +"\n")
                return await RESPONSE.text()
            x = x+1
            f.writelines((str(RESPONSE.status) + " " + asyncio.current_task().get_name()) + " " + str(x) + " " + str(FINAL_URL) +"\n")
            await asyncio.sleep(0.1)

async def async_main():
    CONNECTOR = aiohttp.TCPConnector(limit_per_host=20)
    async with aiohttp.ClientSession(connector=CONNECTOR) as SESSION:
        TASKS_ = []
        for FINAL_URL in URLS:
            TASKS_.append(asyncio.create_task(async_get(FINAL_URL, SESSION)))
        return await asyncio.gather(*TASKS_)

def listing_cards(RESULTS):
    for RESULT in RESULTS:
        try:
            CARDS.extend(BeautifulSoup(RESULT, features="html.parser").find_all('div', {'data-asin': True,  'data-component-type': 's-search-result'}))
        except:
            continue
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

def main(Kategori_Adı, URL):
    global URLS, CARDS, PRODUCTS, RESULTS, f
    URLS = []
    DATA = {}
    PRODUCTS = []
    CARDS = []
    TASKS_ = []
    RESULTS = []

    f = open('log.txt','a')
    f.writelines(Kategori_Adı+"\n")

    TOTAL_NUMBER_OF_PAGES = number_of_pages(URL)
    generating_urls(URL, TOTAL_NUMBER_OF_PAGES)
    start = time.time()
    asyncio.new_event_loop()
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
    CARDS_COUNT = str(len(CARDS))
    del URLS, DATA, CARDS, TASKS_
    return PRODUCTS, CARDS_COUNT, PRODUCT_COUNT, str(round((total_time),2))

if __name__ == "__main__":
    main(Kategori_Adı, URL)
