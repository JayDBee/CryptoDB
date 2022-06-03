#This file is  a test for the Coingecko API

#Documentation
#https://github.com/man-c/pycoingecko

import os
from pycoingecko import CoinGeckoAPI
cg = CoinGeckoAPI()

"""
#TESTING GROUNDS

@OUTPUT:{'gecko_says': '(V3) To the Moon!'}
@INPUT: print(f'{cg.ping()}')

@OUTPUT: HUGE LIST of DICTS
@INPUT: print(f'{cg.get_coins_list()}')

-----

@OUTPUT: example - {'id': 'zyx', 'symbol': 'zyx', 'name': 'ZYX'}
@INPUT: for list in cg.get_coins_list():
    @OUTPUT: prints all coins ID + SYMBOL + NAME
    @INPUT: print(list)

    @OUPUT: prints all ID's
    @INPUT: print(list["id"])

-----
@OUTPUT: 
id
symbol
*name
asset_platform_id
platforms
block_time_in_minutes
*hashing_algorithm
*categories
public_notice
additional_notices
localization
description
links
image
country_origin
*genesis_date
contract_address
sentiment_votes_up_percentage
sentiment_votes_down_percentage
*market_cap_rank
*coingecko_rank
*coingecko_score
developer_score
community_score
liquidity_score
public_interest_score
*market_data
community_data
developer_data
public_interest_stats
status_updates
*last_updated
tickers

@INPUT:
    x = cg.get_coin_by_id('zyx')
    for list in x:
        print(list)



@OUTPUT:
{'name': 'ZYX', 
        'tickers': 
                 [{'base': '0X377C6E37633E390AEF9AFB4F5E0B16689351EED4', 
                 'target': 'WBNB', 
                 'market': {'name': 'PancakeSwap (v2)', 
                            'identifier': 'pancakeswap_new', 
                            'has_trading_incentive': False}, 
                'last': 7.337259058268e-05, 
                'volume': 26423.535576781316, 
                'converted_last': {'btc': 7.43044e-07, 
                                'eth': 1.221e-05, 
                                'usd': 0.0222204}, 
                'converted_volume': {'btc': 0.01961117, 
                                    'eth': 0.3222923, 
                                    'usd': 586.46}, 
                'trust_score': 'green', 
                'bid_ask_spread_percentage': 0.612414, 
                'timestamp': '2022-06-02T06:22:53+00:00', 
                'last_traded_at': '2022-06-02T06:22:53+00:00', 
                'last_fetch_at': '2022-06-02T06:51:34+00:00', 
                'is_anomaly': False, 
                'is_stale': False, 
                'trade_url': 'https://pancakeswap.finance/swap?inputCurrency=0x377c6e37633e390aef9afb4f5e0b16689351eed4&outputCurrency=wbnb', 
                'token_info_url': None, 
                'coin_id': 'zyx', 
                'target_coin_id': 'wbnb'
                }]
}
@INTPUT: 
    cg.get_coin_ticker_by_id('zyx')

-------
@OUPUT:
        id
        symbol
        name
        localization
        image
        market_data
        community_data
        developer_data
        public_interest_stats
@INPUT:
    x = cg.get_coin_history_by_id('zyx', '30-12-2021')
    for list in x:
        print(list)

"""

#Creates list of all Id's, sybmols, & names
ids = []
symbols = []
names = []

#initialize list
for list in cg.get_coins_list():
    ids.append(list["id"])
    symbols.append(list["symbol"])
    names.append(list["name"])

#index of last item in list
l = len(ids) - 1

##################################
#Creates list of data for id's
print(cg.get_coin_by_id(ids[0]))


"""
*name
*platforms
*hashing_algorithm
*categories
*genesis_date
*market_cap_rank
*coingecko_rank
*coingecko_score
*market_data
*last_updated



minute = 60
index = 0
col_list = []

for item in ids:
    if index > 40:
        break
    else:
        coin_data = cg.get_coin_by_id(item)
        col_list.append({'name': coin_data["name"],
                'platforms': coin_data["platforms"],
                'hashing_alg': coin_data["hashing_algorithm"],
                'category': coin_data["categories"],
                #debating on using category or not
                'genesis_date': coin_data["genesis_date"],
                'market_cap_rank': coin_data["market_cap_rank"],
                'gecko_rank': coin_data["coingecko_rank"],
                'gecko_score': coin_data["coingecko_score"],
                #'market_data': coin_data["market_data"],
                #contains too much total market data
                'last_updated': coin_data["last_updated"]
                })
        index += 1
print(col_list)

"""


"""
What I want to do is to gather a comprehenssive list of id's 
To import these lists directly into PSQL
"""
