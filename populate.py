"""
Special Thanks to
>https://pynative.com/python-postgresql-insert-update-delete-table-data-to-perform-crud-operations/
"""


import os
import psycopg2
#from config import config
from pycoingecko import CoinGeckoAPI
cg = CoinGeckoAPI()



#CREATE/DROP TABLE "TEST"
#cursor.execute("DROP TABLE IF EXISTS TEST")
#cursor.execute("CREATE TABLE TEST()")


def create_tables():
    #creates the cryptocurrencies table
    sql = '''CREATE TABLE cryptocurrencies(
            Id VARCHAR(20),
            Name VARCHAR(20),
            Symbol VARCHAR(20),
            PRIMARY KEY(Id)
    )'''
    cursor.execute(sql)

    sql = '''CREATE TABLE crypto_ranks(
            Id VARCHAR(20),
            market_cap_rank INT,
            coingecko_rank INT,
            PRIMARY KEY(Id),
            FOREIGN KEY(Id)
                REFERENCES cryptocurrencies(Id)
    )'''
    cursor.execute(sql)

    sql = '''CREATE TABLE crypto_market_data(
            Id VARCHAR(20),
            curr_price_usd DEC(10,10),
            All_time_high DEC(10,10),
            ATH_date VARCHAR(30),
            All_time_low DEC(10,10),
            ATL_date VARCHAR(30)
            PRIMARY KEY(Id),
            FOREIGN KEY(Id)
                REFERENCES cryptocurrencies(Id)
    )'''
    cursor.execute(sql)

    sql = '''CREATE TABLE crypto_supply(
            Id VARCHAR(20),
            total_supply DEC(15,5),
            circulating_supply DEC(10,10),
            volume DEC(10,10),
            last_updated VARCHAR(30)
            PRIMARY KEY(Id),
            FOREIGN KEY(Id)
                REFERENCES cryptocurrencies(Id)
    )'''
    cursor.execute(sql)

    sql = '''CREATE TABLE crypto_langs(
            Id VARCHAR(20),
            Language VARCHAR(20),
            PRIMARY KEY(Id,Language),
            FOREIGN KEY(Id)
                REFERENCES cryptocurrencies(Id)

    )'''
    cursor.execute(sql)


def insert_in_currencies():
    #ID == VARCHAR(20), Symbol == VARCHAR(20), name == VARCHAR(20)



if __name__ == '__main__':

    #Establish the connection
    conn = psycopg2.connect(
        database= "spr2022bdb3", user='spr2022bdb3', 
        password='xf?e48kRrc', host='dbclass.cs.pdx.edu'
    )
    #Creating a cursor object using the cursor() method
    cursor = conn.cursor()

    #if not already created
    #create_tables()


    conn.commit()
    conn.close()