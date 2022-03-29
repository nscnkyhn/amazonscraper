import sqlite3
from xml.dom.pulldom import START_DOCUMENT
import aiohttp_parser
import time
import asyncio

CONN = sqlite3.connect("amazon_database.db")
CUR = CONN.cursor()

async def async_work(Kategori_ID, PRODUCT, TABLE_NAME):
    try:
        OLD_PRICE = CUR.execute('SELECT NEW_PRODUCT_PRICE FROM {} WHERE "DATA_ASIN"="{}"'.format(TABLE_NAME, PRODUCT["data-asin"])).fetchone()[0]
        CUR.execute('INSERT OR REPLACE INTO {}(Kategori_ID, DATA_ASIN, PRODUCT_TITLE, PRODUCT_LINK, CURRENCY, NEW_PRODUCT_PRICE, OLD_PRODUCT_PRICE) VALUES(?,?,?,?,?,?,?)'.format(TABLE_NAME), (Kategori_ID, PRODUCT["data-asin"], PRODUCT["title"], PRODUCT["link"], PRODUCT["price_symbol"], PRODUCT["price"], OLD_PRICE))
    except:
        CUR.execute('INSERT OR REPLACE INTO {}(Kategori_ID, DATA_ASIN, PRODUCT_TITLE, PRODUCT_LINK, CURRENCY, NEW_PRODUCT_PRICE) VALUES(?,?,?,?,?,?)'.format(TABLE_NAME), (Kategori_ID, PRODUCT["data-asin"], PRODUCT["title"], PRODUCT["link"], PRODUCT["price_symbol"], PRODUCT["price"]))

async def async_main(Kategori_ID, RES, TABLE_NAME):
    TASKS_ = []
    for PRODUCT in RES[0]:
        TASKS_.append(asyncio.create_task(async_work(Kategori_ID, PRODUCT, TABLE_NAME)))
    await asyncio.gather(*TASKS_)

def save_results(Kategori_ID, Kategori_Adı, RES):
    TABLE_NAME = Kategori_Adı.replace(' ', '_')
    CUR.execute('CREATE TABLE IF NOT EXISTS {}(PRODUCT_ID INTEGER NOT NULL, Kategori_ID INTEGER, DATA_ASIN TEXT UNIQUE,  PRODUCT_TITLE TEXT, PRODUCT_LINK TEXT, CURRENCY TEXT, DISCOUNT INTEGER, OLD_PRODUCT_PRICE INTEGER, NEW_PRODUCT_PRICE INTEGER, PRIMARY KEY("PRODUCT_ID" AUTOINCREMENT))'.format(TABLE_NAME))
    loop = asyncio.new_event_loop()
    loop.run_until_complete(async_main(Kategori_ID, RES, TABLE_NAME))
    CUR.execute('UPDATE {} SET DISCOUNT = round(((OLD_PRODUCT_PRICE-NEW_PRODUCT_PRICE)/OLD_PRODUCT_PRICE)*100, 0)'.format(TABLE_NAME))
    CUR.execute("UPDATE Main SET Kaydedilen_Ürün_Sayısı = (SELECT count(*) FROM {}) WHERE Kategori_Adı = '{}'".format(TABLE_NAME, Kategori_Adı))

def runner():
    global START_COUNTER
    print(START_COUNTER)
    if START_COUNTER == MAX_COUNTER:
        return
    Kategori_ID = URLS_TO_PARSE[START_COUNTER][0]
    Kategori_Adı = URLS_TO_PARSE[START_COUNTER][1]
    Kategori_Linki = URLS_TO_PARSE[START_COUNTER][2]
    print(Kategori_ID, Kategori_Adı, "taranıyor.")
    RESULT = aiohttp_parser.main(Kategori_Adı, Kategori_Linki)
    save_results(Kategori_ID, Kategori_Adı, RESULT)
    pr = "Kategori : " + Kategori_Adı + "\t\tTaranan ürün sayısı : " + str(RESULT[1]) + "\tKaydedilen ürün sayısı : " + str(RESULT[2]) + "\t Toplam işlem süresi : " + RESULT[3]
    print(pr)
    with open('main_log.txt', 'a') as f:
        f.writelines(pr)
    CONN.commit()
    START_COUNTER = START_COUNTER + 1
    runner()

def main():
    global URLS_TO_PARSE, START_COUNTER, MAX_COUNTER
    TOTAL_TIME_START = time.time()
    URLS_TO_PARSE = CUR.execute("SELECT Kategori_ID, Kategori_Adı, Kategori_Linki FROM Main WHERE Durum = '1'").fetchall()
    START_COUNTER = 0
    MAX_COUNTER = len(URLS_TO_PARSE)
    """     for Kategori_Adı, Kategori_Linki in URLS_TO_PARSE:
        print(Kategori_Adı, "taranıyor.")
        RESULT = aiohttp_parser.main(Kategori_Adı, Kategori_Linki)
        save_results(Kategori_Adı, RESULT)
        pr = "Kategori : " + Kategori_Adı + "\t\tTaranan ürün sayısı : " + str(RESULT[1]) + "\tKaydedilen ürün sayısı : " + str(RESULT[2]) + "\t Toplam işlem süresi : " + RESULT[3]
        print(pr)
        with open('main_log.txt', 'a') as f:
            f.writelines(pr.encode('utf-8')+"\n")
        CONN.commit() """
    runner()
            
    TOTAL_TIME_END = time.time()
    print("Bütün kategoriler için geçen toplam süre : " + str(round(TOTAL_TIME_END-TOTAL_TIME_START,2)))

if __name__ == "__main__":
    main()
    CONN.commit()
    CUR.execute("VACUUM")