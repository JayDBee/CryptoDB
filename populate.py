"""
Special Thanks to
>https://pynative.com/python-postgresql-insert-update-delete-table-data-to-perform-crud-operations/
"""


import os
import psycopg2
from time import sleep 
from pycoingecko import CoinGeckoAPI
cg = CoinGeckoAPI()



#Establish the connection
conn = psycopg2.connect(
database= "spr2022bdb3", user='spr2022bdb3', 
password='xf?e48kRrc', host='dbclass.cs.pdx.edu'
)
cursor = conn.cursor()      #Creating a cursor object using the cursor() method



def create_cryptoTable():
    #creates the cryptocurrencies table
    sql = '''CREATE TABLE cryptocurrencies(
            Id VARCHAR(60),
            Symbol VARCHAR(60),
            Name VARCHAR(60),
            PRIMARY KEY(Id)
    )'''
    cursor.execute(sql)


def create_ranks():
    sql = '''CREATE TABLE crypto_ranks(
            Id VARCHAR(40),
            market_cap_rank INTEGER,
            coingecko_rank INTEGER,
            PRIMARY KEY(Id),
            FOREIGN KEY(Id)
                REFERENCES cryptocurrencies(Id)
    )'''
    cursor.execute(sql)


def create_records():
    sql = '''CREATE TABLE crypto_records(
            Id VARCHAR(40),
            All_time_high NUMERIC,
            ATH_date VARCHAR(30),
            All_time_low NUMERIC,
            ATL_date VARCHAR(40),
            PRIMARY KEY(Id),
            FOREIGN KEY(Id)
                REFERENCES cryptocurrencies(Id)
    )'''
    cursor.execute(sql)


def create_market():
    sql = '''CREATE TABLE crypto_marketdata(
            Id VARCHAR(40),
            curr_price_usd NUMERIC,
            total_supply NUMERIC,
            circulating_supply NUMERIC,
            last_updated VARCHAR(30),
            PRIMARY KEY(Id),
            FOREIGN KEY(Id)
                REFERENCES cryptocurrencies(Id)
    )'''
    cursor.execute(sql)


def create_langs():
    sql = '''CREATE TABLE crypto_langs(
            Id VARCHAR(40),
            Language VARCHAR(5),
            PRIMARY KEY(Id,Language),
            FOREIGN KEY(Id)
                REFERENCES cryptocurrencies(Id)
    )'''
    cursor.execute(sql)


def drop_tables():
    #drops the cryptocurrencies table
    sql = '''DROP TABLE IF EXISTS cryptocurrencies CASCADE'''
    cursor.execute(sql)
    sql = '''DROP TABLE IF EXISTS crypto_ranks'''
    cursor.execute(sql)
    sql = '''DROP TABLE IF EXISTS crypto_records'''
    cursor.execute(sql)
    sql = '''DROP TABLE IF EXISTS crypto_marketdata'''
    cursor.execute(sql)
    sql = '''DROP TABLE IF EXISTS crypto_langs'''
    cursor.execute(sql)


#INPUT: list of dicts
#ID,Symbol,Name
def insert_in_currencies(record):
    val_insert = (record["id"],record["symbol"],record["name"])

    query = '''INSERT INTO cryptocurrencies(Id, Symbol, Name)
        VALUES(%s,%s,%s)'''
    cursor.execute(query, val_insert)


def wrap_currencies():
    #insert fill into cryptocurrencies table
    for record in cg.get_coins_list():   
        insert_in_currencies(record)


#INPUT: list of dicts
#ID,market_cap_rank,coingecko_rank
def insert_in_rank(val_insert):
    query = '''INSERT INTO crypto_ranks(Id, market_cap_rank, coingecko_rank)
                VALUES(%s,%s,%s)'''
    cursor.execute(query, val_insert)

def wrap_ranks(id_set):
    count = 1
    rank_table = []

    for id in id_set:
        #request limit is set to 50 per minute
        if(count % 50 == 0):
            break
        else:
            #get_coin_by_id returns a dictionary to res
            res = cg.get_coin_by_id(id)

            res_id = res["id"]
            mrkt_rank = res["market_cap_rank"]
            gecko_rank = res["coingecko_rank"]

            rank_table.append((res_id, mrkt_rank, gecko_rank))
            count +=1

    for record in rank_table:
        try:
            insert_in_rank(record)
        except:
            print("unable to insert " + str(record) + " in rank database")


def insert_in_records(val_insert):
    query = '''INSERT INTO crypto_records( Id, All_time_high, ATH_date, All_time_low, ATL_date)
                VALUES(%s,%s,%s,%s,%s)'''
    cursor.execute(query, val_insert)


def wrap_records(id_set):
    count = 1
    record_table = []
    for id in id_set:
        if(count % 50 == 0):
            break
        else:
            res = cg.get_coin_by_id(id)
            m_result = res["market_data"]
            coin_id = res["id"]

            try:
                ath = m_result["ath"]["usd"]
                ath_d = m_result["ath_date"]["usd"]
                atl = m_result["atl"]["usd"]
                atl_d = m_result["atl_date"]["usd"]

                record_table.append((coin_id, ath, ath_d, atl, atl_d))
            except:
                print("unable to find ath, ath_date, atl, or atl_date")

            count +=1

    for record in record_table:
        try:
            insert_in_records(record)
        except:
            print("unable to insert " + str(record) + " in record database")


def insert_in_marketdata(val_insert):
    query = '''INSERT INTO crypto_marketdata( Id, curr_price_usd, total_supply, circulating_supply, last_updated)
                VALUES(%s,%s,%s,%s,%s)'''
    cursor.execute(query, val_insert)


def wrap_marketdata(id_set):
    count = 1
    market_table = []
    for id in id_set:
        if(count % 50 == 0):
            break
        else:
            res = cg.get_coin_by_id(id)
            m_result = res["market_data"]
            coin_id = res["id"]

            try:
                cur_price = m_result["current_price"]["usd"]
                tot_supply = m_result["total_supply"]
                cir_supply = m_result["circulating_supply"]
                last_update = m_result["last_updated"]

                market_table.append((coin_id, cur_price, tot_supply, cir_supply, last_update))
            except:
                print("unable to find cur_price,total_supply, cir_supply, or last_udpate")
            count +=1

    for record in market_table:
        try:
            insert_in_marketdata(record)
        except:
            print("unable to insert " + str(record) + " in market database")


def insert_in_langs(val_insert):
    query = '''INSERT INTO crypto_langs( Id, Language)
                VALUES(%s,%s)'''
    cursor.execute(query, val_insert)


def wrap_langs(id_set):
    count = 1
    for id in id_set:
        if(count % 50 == 0):
                break
        else:
            res = cg.get_coin_by_id(id)
            count +=1

            #res["localization"] contains a list of languages
            for lang in res["localization"]:
                try:
                    insert_in_langs((id,lang))
                except:
                    print("unable to insert " + str(lang) + " in language database")



if __name__ == '__main__':

    #if needed
    #drop_tables()

    #if not already created
    create_cryptoTable()
    create_ranks()
    create_records()
    create_market()
    create_langs()

    #collects all id's in the current coingecko db
    ids = []
    for item in cg.get_coins_list():
        ids.append(item["id"])

    #calls to fill databases
    wrap_currencies()
    conn.commit()

    wrap_ranks(ids)
    conn.commit()
    sleep(60)

    wrap_records(ids)
    conn.commit()
    sleep(60)

    wrap_marketdata(ids)
    conn.commit()
    sleep(60)

    wrap_langs(ids)

    conn.commit()
    conn.close()