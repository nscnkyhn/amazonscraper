import sqlite3
import aiohttp_parser
import time

CONN = sqlite3.connect("amazon_database.db")
CUR = CONN.cursor()

def save_results(Kategori_Adı, RES):
    TABLE_NAME = Kategori_Adı.replace(' ', '_')
    CUR.execute("CREATE TABLE IF NOT EXISTS {}(DATA_ASIN TEXT UNIQUE,  PRODUCT_TITLE TEXT, PRODUCT_LINK TEXT, CURRENCY TEXT, DISCOUNT INTEGER, OLD_PRODUCT_PRICE INTEGER, NEW_PRODUCT_PRICE INTEGER)".format(TABLE_NAME))
    CURRENT_LIST_OF_PRODUCTS = CUR.execute('SELECT DATA_ASIN FROM {}'.format(TABLE_NAME)).fetchall()
    for i in range(len(CURRENT_LIST_OF_PRODUCTS)):
        CURRENT_LIST_OF_PRODUCTS[i]=CURRENT_LIST_OF_PRODUCTS[i][0]
    CUR.execute('UPDATE {} SET OLD_PRODUCT_PRICE = NEW_PRODUCT_PRICE'.format(TABLE_NAME))
    for PRODUCT in RES[0]:
        if PRODUCT["data-asin"] not in CURRENT_LIST_OF_PRODUCTS:
            CUR.execute('INSERT OR REPLACE INTO {}(DATA_ASIN, PRODUCT_TITLE, PRODUCT_LINK, CURRENCY, NEW_PRODUCT_PRICE) VALUES(?,?,?,?,?)'.format(TABLE_NAME), (PRODUCT["data-asin"], PRODUCT["title"], PRODUCT["link"], PRODUCT["price_symbol"], PRODUCT["price"]))
        else:
            CUR.execute('UPDATE {} SET NEW_PRODUCT_PRICE = "{}" WHERE DATA_ASIN = "{}"'.format(TABLE_NAME, PRODUCT["price"], PRODUCT["data-asin"]))
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