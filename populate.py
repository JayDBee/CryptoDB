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
            All_time_high DECIMAL(10,10),
            ATH_date VARCHAR(30),
            All_time_low DECIMAL(10,10),
            ATL_date VARCHAR(40),
            PRIMARY KEY(Id),
            FOREIGN KEY(Id)
                REFERENCES cryptocurrencies(Id)
    )'''
    cursor.execute(sql)


def create_market():
    sql = '''CREATE TABLE crypto_marketdata(
            Id VARCHAR(40),
            curr_price_usd DECIMAL(10,10),
            total_supply DECIMAL(15,5),
            circulating_supply DECIMAL(10,10),
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
    sql = '''DROP TABLE cryptocurrencies CASCADE'''
    cursor.execute(sql)
    sql = '''DROP TABLE crypto_ranks'''
    cursor.execute(sql)
    sql = '''DROP TABLE crypto_records'''
    cursor.execute(sql)
    sql = '''DROP TABLE crypto_marketdata'''
    cursor.execute(sql)
    sql = '''DROP TABLE crypto_langs'''
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
        VALUES(%s,%d,%d)'''
    cursor.execute(query, val_insert)


def wrap_ranks(ids):
    seconds = 60
    count = 1
    rank_table = []
    for id in ids:
        if(count % seconds == 0):
            break
        else:
            res = cg.get_coin_by_id(id)
            rank = (res["id"],res["market_cap_rank"],res["coingecko_rank"])
            rank_table.append(rank)
            count +=1

    for record in rank_table:
        try:
            insert_in_rank(record)
        except:
            print("unable to insert " + str(record) + " in rank")


def insert_in_records(val_insert):
    query = '''INSERT INTO crypto_records(
                        Id, All_time_high, ATH_date, 
                        All_time_low, ATL_date)
                VALUES(%s,%f,%s,%f,%s)'''
    cursor.execute(query, val_insert)


def wrap_records(ids):
    seconds = 60
    count = 0
    record_table = []
    for id in ids:
        if(count % seconds == 0):
            break
        else:
            x = cg.get_coin_by_id(id)
            res = x["market_data"]
            record = [x["id"],
                        res["ath"]["usd"],
                        res["ath_date"]["usd"],
                        res["atl"]["usd"],
                        res["atl_date"]["usd"]]
            record_table.append(record)
            count +=1

    for r in record_table:
        insert_in_records(r)


def insert_in_marketdata(val_insert):
    query = '''INSERT INTO crypto_marketdata(
                        Id, curr_price_usd, total_supply, 
                        circulating_supply, last_updated)
                VALUES(%s,%f,%f,%f,%s)'''
    cursor.execute(query, val_insert)


def wrap_marketdata(ids):
    seconds = 60
    count = 0
    market_table = []
    for id in ids:
        if(count % seconds == 0):
            break
        else:
            x = cg.get_coin_by_id(id)
            res = x["market_data"]
            market = [x["id"],
                        res["current_price"]["usd"],
                        res["circulating_supply"],
                        res["total_supply"],
                        res["last_updated"]]
            market_table.append(market)
            count +=1

    for record in market_table:
        insert_in_marketdata(record)


def insert_in_langs(val_insert):
    query = '''INSERT INTO crypto_langs( Id, Language)
                VALUES(%s,%s)'''
    cursor.execute(query, val_insert)


def wrap_langs(ids):
    seconds = 60
    count = 0
    for id in ids:
        if(count % seconds == 0):
                break
        else:
            x = cg.get_coin_by_id(id)
            count +=1
            for lang in x["localization"]:
                insert_in_langs((id,lang))


if __name__ == '__main__':
    ids = []
    for item in cg.get_coins_list():
        ids.append(item["id"])

    #calls to fill 
    #wrap_currencies()
    wrap_ranks(ids)

    conn.commit()
    conn.close()
"""
    create_cryptoTable()    #if not already created
    create_ranks()
    create_records()
    create_market()
    create_langs()

    #drop_tables()
    #creates a list of id's in the exchange

    sleep(60)
    wrap_records(ids)
    sleep(60)
    wrap_marketdata(ids)
    sleep(60)
    wrap_langs(ids)
"""