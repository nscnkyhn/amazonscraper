import sqlite3
import aiohttp_parser
import time

#url = "https://www.amazon.com.tr/s?i=fashion&bbn=13546844031&rh=n%3A13546844031%2Cp_6%3AA1UNQM1SR2CHM%7CA3O5TP4R0OZYXZ%2Cp_n_size_browse-vebin%3A13681704031&dc&fs=true&page=1&pf_rd_i=13546649031&pf_rd_m=A1UNQM1SR2CHM&pf_rd_p=c1d872b6-273d-43fb-9493-093f9cc5ea4f&pf_rd_r=GMVNXJR2E31PA03EMKZP&pf_rd_s=merchandised-search-7&pf_rd_t=101&qid=1646849977&rnid=13663958031&ref=sr_pg_2"

CONN = sqlite3.connect("amazon_database.db")
CUR = CONN.cursor()

def save_results(Kategori_Adı, RES):
    TABLE_NAME = Kategori_Adı.replace(' ', '_')
    CUR.execute("CREATE TABLE IF NOT EXISTS {}(DATA_ASIN text unique,  PRODUCT_TITLE text, PRODUCT_LINK text, CURRENCY text, DISCOUNT integer, OLD_PRODUCT_PRICE integer, NEW_PRODUCT_PRICE integer)".format(TABLE_NAME))
    time.sleep(1)
    CUR.execute("UPDATE {} SET OLD_PRODUCT_PRICE = NEW_PRODUCT_PRICE".format(TABLE_NAME))
    for PRODUCT in RES[0]:
        CUR.execute('INSERT OR REPLACE INTO {}(DATA_ASIN, PRODUCT_TITLE, PRODUCT_LINK, NEW_PRODUCT_PRICE) VALUES(?,?,?,?)'.format(TABLE_NAME), (PRODUCT["data-asin"], PRODUCT["title"], PRODUCT["link"], PRODUCT["price"]))

    CUR.execute("UPDATE {} SET DISCOUNT =(SELECT (NEW_PRODUCT_PRICE-OLD_PRODUCT_PRICE)/100 FROM {})".format(TABLE_NAME, TABLE_NAME))

def main():
    TOTAL_TIME_START = time.time()
    LIST_TO_PARSE = CUR.execute("SELECT Kategori_Adı, Kategori_Linki FROM Main WHERE Durum = '1'").fetchall()
    for Kategori_Adı, Kategori_Linki in LIST_TO_PARSE:
        RES = aiohttp_parser.main(Kategori_Linki)
        save_results(Kategori_Adı, RES)
        print("Kategori : " + Kategori_Adı + "\tTaranan ürün sayısı : " + str(RES[1]) + "\t Toplam işlem süresi : " + str(RES[2]))
        #with open(str(Kategori_Adı)+'.txt', 'w', encoding='utf-8') as f:
            #for i in RES[0]:
                #f.write(str(i)+"\n")
    TOTAL_TIME_END = time.time()
    print("Bütün kategoriler için geçen toplam süre : " + str(TOTAL_TIME_END-TOTAL_TIME_START))

if __name__ == "__main__":
    main()
    CONN.commit()
    print("commited")