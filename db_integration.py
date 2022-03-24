import sqlite3
import aiohttp_parser
import time
import asyncio

CONN = sqlite3.connect("amazon_database.db")
CUR = CONN.cursor()


async def work(PRODUCT, TABLE_NAME):
    try:
        OLD_PRICE = CUR.execute('SELECT NEW_PRODUCT_PRICE FROM {} WHERE "DATA_ASIN"="{}"'.format(TABLE_NAME, PRODUCT["data-asin"])).fetchone()[0]
        CUR.execute('INSERT OR REPLACE INTO {}(DATA_ASIN, PRODUCT_TITLE, PRODUCT_LINK, CURRENCY, NEW_PRODUCT_PRICE, OLD_PRODUCT_PRICE) VALUES(?,?,?,?,?,?)'.format(TABLE_NAME), (PRODUCT["data-asin"], PRODUCT["title"], PRODUCT["link"], PRODUCT["price_symbol"], PRODUCT["price"], OLD_PRICE))
    except:
        CUR.execute('INSERT OR REPLACE INTO {}(DATA_ASIN, PRODUCT_TITLE, PRODUCT_LINK, CURRENCY, NEW_PRODUCT_PRICE) VALUES(?,?,?,?,?)'.format(TABLE_NAME), (PRODUCT["data-asin"], PRODUCT["title"], PRODUCT["link"], PRODUCT["price_symbol"], PRODUCT["price"]))

async def async_main(RES, TABLE_NAME):
    TASKS_ = []
    for PRODUCT in RES[0]:
        TASKS_.append(asyncio.create_task(work(PRODUCT, TABLE_NAME)))
    await asyncio.gather(*TASKS_)

def save_results(Kategori_Adı, RES):
    TABLE_NAME = Kategori_Adı.replace(' ', '_')
    CUR.execute("CREATE TABLE IF NOT EXISTS {}(DATA_ASIN TEXT UNIQUE,  PRODUCT_TITLE TEXT, PRODUCT_LINK TEXT, CURRENCY TEXT, DISCOUNT INTEGER, OLD_PRODUCT_PRICE INTEGER, NEW_PRODUCT_PRICE INTEGER)".format(TABLE_NAME))
    loop = asyncio.get_event_loop()
    loop.run_until_complete(async_main(RES, TABLE_NAME))
    CUR.execute('UPDATE {} SET DISCOUNT = round(((OLD_PRODUCT_PRICE-NEW_PRODUCT_PRICE)/OLD_PRODUCT_PRICE)*100, 0)'.format(TABLE_NAME))  

def main():
    TOTAL_TIME_START = time.time()
    URLS_TO_PARSE = CUR.execute("SELECT Kategori_Adı, Kategori_Linki FROM Main WHERE Durum = '1'").fetchall()
    for Kategori_Adı, Kategori_Linki in URLS_TO_PARSE:
        RESULT = aiohttp_parser.main(Kategori_Linki)
        save_results(Kategori_Adı, RESULT)
        print("Kategori : " + Kategori_Adı + "\tTaranan ürün sayısı : " + str(RESULT[1]) + "\t Toplam işlem süresi : " + RESULT[2])
        #with open(str(Kategori_Adı)+'.txt', 'w', encoding='utf-8') as f:
            #for i in RES[0]:
                #f.write(str(i)+"\n")
    TOTAL_TIME_END = time.time()
    print("Bütün kategoriler için geçen toplam süre : " + str(TOTAL_TIME_END-TOTAL_TIME_START))

if __name__ == "__main__":
    main()
    CONN.commit()