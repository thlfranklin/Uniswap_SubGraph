#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Exploring uniswap tx using graphQl

@author: ThaFranklin

Created on 12/09/2021
"""
#%% 
import os 
import sys 
import json
import pandas as pd 

pd.set_option('display.max_columns',100)
pd.set_option('precision', 3)
pd.set_option('display.float_format', lambda x: '%.3f' % x)
# pd.set_option('display.max_colwidth',15) 

s_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(s_path)


#%% 
# reading basic pools info extracted from 
# https://graphql.bitquery.io/

prefix_files = 'POOL_INFO_'
path_files = s_path
l_dir = os.listdir(path_files)
l_files = [_file for _file in l_dir if prefix_files in _file]
df_pools = pd.DataFrame()
print('files to read: ')
print(l_files)

#%% 
for _file in l_files:
  with open(os.path.join(s_path, _file), 'r') as file_json:
    data_json = json.load(file_json)
  # drop header from dict 
  data_json = data_json['ethereum']['dexTrades']
  df_pool_info = pd.json_normalize(data_json, max_level=1)
  df_pools = df_pools.append(df_pool_info)

# %%
df_pools['date_dt'] = pd.to_datetime(df_pools['timeInterval.minute']).dt.date 
avgSize_usd = df_pools.groupby(['baseCurrency.symbol', 'quoteCurrency.symbol', 'date_dt'])[['trades', 'baseAmount']].sum()
avgSize_usd['avgSize_base'] = avgSize_usd['baseAmount']/avgSize_usd['trades']

avgSize_usd.to_csv(prefix_files+'stats.csv')

# avgSize_usd

#%% 
df_pools.loc[
  # df_pools['baseCurrency.symbol'].isin(['1INCH',])
  df_pools['baseCurrency.symbol'] != 'ETH'
  ].groupby(
    [
    # 'date_dt', 
    'baseCurrency.symbol',
    'quoteCurrency.symbol'
    ]
    )['trades'].sum()

# %%
# %%
# bring TVL info from Uniswap subgraph 
# https://thegraph.com/hosted-service/subgraph/uniswap/uniswap-v3
# https://docs.uniswap.org/sdk/subgraph/subgraph-examples

with open(os.path.join(s_path, 'TVL_2000.json'), 'r', encoding='utf8') as file_json:
  data_json = json.load(file_json)

df_tvl = pd.json_normalize(data_json['data']['pools'], max_level=1)
df_tvl.head()

# %%
l_base = df_pools['baseCurrency.symbol'].unique()
df_tvl = df_tvl.loc[df_tvl['token0.symbol'].isin(l_base)]
l_quote = df_pools['quoteCurrency.symbol'].unique()
df_tvl = df_tvl.loc[df_tvl['token1.symbol'].isin(l_quote)]

df_tvl.head()
df_tvl.to_csv('TVL.csv')

#%% 
# reading basic mints info extracted from Uniswap subgraph
'''
{ 
  pool(id: "0xe8c6c9227491c0a8156a0106a0204d881bb7e531")
  {
    token0 {
      symbol
      id
      decimals
    }
    token1 {
      symbol
      id
      decimals
    }
    mints{
      origin
      amount
      amount0
      amount1
      tickLower
      tickUpper
      transaction{
        id
        timestamp
        gasUsed
        gasPrice
      }
    }
  }
}
'''
#%% 
prefix_files = 'MINT_INFO.json'
path_files = s_path
l_dir = os.listdir(path_files)
l_files = [_file for _file in l_dir if prefix_files in _file]
df_mints = pd.DataFrame()
print('files to read: ')
print(l_files)

#%% 
for _file in l_files:
  with open(os.path.join(s_path, _file), 'r') as file_json:
    data_json = json.load(file_json)
  # drop header from dict 
  data_json = data_json['data']['pool']
  df_i = pd.json_normalize(data_json, record_path=['mints'], meta=[['token0','symbol'],['token1','symbol']])
  df_mints = df_mints.append(df_i)

df_mints.head()

# %%
df_mints['date'] = pd.to_datetime(df_mints['transaction.timestamp'], unit='s', origin='unix')
df_mints['transaction.gasUsed'] = df_mints['transaction.gasUsed']
print(f'Uniswap Mint transactions from {df_mints.date.min()} to {df_mints.date.max()}')

# %%
import matplotlib.pyplot as plt

ax = df_mints['transaction.gasUsed'].hist(bins=16)
plt.title('gas limit in Uniswap mint transactions ~ 600 thousand GAS')
# plt.xticks([])
plt.ylabel('GAS * 10**5')
plt.xlabel('tx')
plt.show()

# %%
df_mints['gasPrice_gwei'] = df_mints['transaction.gasPrice'].astype(float)/(10**9)
ax = df_mints['gasPrice_gwei'].hist(bins=16)
# plt.title('gas price in Uniswap mint transactions')
# plt.ylabel('freq')
plt.yticks([])
plt.xlabel('GAS price (gwei)')
plt.show()

# %% UNISWAP DAY DATA

prefix_files = 'DAY_DATA_v2'
path_files = s_path
l_dir = os.listdir(path_files)
l_files = [_file for _file in l_dir if prefix_files in _file]
df_mints = pd.DataFrame()
print('files to read: ')
print(l_files)

df_daydata = pd.DataFrame()

# %%
for _file in l_files:
  with open(os.path.join(s_path, _file), 'r', encoding='utf8') as file_json:
    data_json = json.load(file_json)
  # drop header from dict 
  data_json = data_json['data']['pools']
  # data_json = data_json['data']['poolDayDatas']
  # df_i = pd.json_normalize(data_json)
  df_daydata = df_daydata.append(df_i)

df_daydata.head()

# %%
df_daydata['date_normal'] = pd.to_datetime(df_daydata['date'], unit='s', origin='unix')
# %%
df_daydata.groupby(['pool.token0.symbol', 'pool.token1.symbol'])[['date_normal']].count()
# df_daydata.groupby(['pool.token0.symbol', 'pool.token1.symbol', 'date_normal', 'token0Price', 'token1Price'])[['volumeUSD', 'volumeToken0', 'volumeToken1', 'txCount','feesUSD', ]].sum().to_csv('test_day_data.csv')



# %% matic uniswap
# https://thegraph.com/hosted-service/subgraph/ianlapham/uniswap-v3-polygon
'''
{
  poolDayDatas(
    where: 
      {date_gt:1641556869,
      volumeUSD_gt:1000000}) {
    pool {
      id
      token0{
        symbol
      }
      token1{
        symbol
      }
    }
    date
    volumeUSD
    tvlUSD
  }
}
'''

prefix_files = 'UNISWAP_POLYGON_DAY_GT'
path_files = s_path
l_dir = os.listdir(path_files)
l_files = [_file for _file in l_dir if prefix_files in _file]
print('files to read: ')
print(l_files)

df_day_matic = pd.DataFrame()

for _file in l_files:
  with open(os.path.join(s_path, _file), 'r', encoding='utf8') as file_json:
    data_json = json.load(file_json)
  # drop header from dict 
  # data_json = data_json['data']['pools']
  data_json = data_json['data']['poolDayDatas']
  df_i = pd.json_normalize(data_json)
  df_day_matic = df_day_matic.append(df_i)

df_day_matic['date'] = pd.to_datetime(df_day_matic['date'], unit='s', origin='unix')
df_day_matic[['tvlUSD', 'volumeUSD']] = df_day_matic[['tvlUSD', 'volumeUSD']].astype(float)
df_day_matic.head()

# %% wMATIC/USDT on polygon 30 bps
# 0x781067ef296e5c4a4203f81c593274824b7c185d

prefix_files = 'UNISWAP_POLYGON_MINT_INFO'
path_files = s_path
l_dir = os.listdir(path_files)
l_files = [_file for _file in l_dir if prefix_files in _file]
df_mints_matic = pd.DataFrame()
print('files to read: ')
print(l_files)

#%% 
for _file in l_files:
  with open(os.path.join(s_path, _file), 'r') as file_json:
    data_json = json.load(file_json)
  # drop header from dict 
  data_json = data_json['data']['pool']
  df_i = pd.json_normalize(data_json, record_path=['mints'], meta=[['token0','symbol'],['token1','symbol']])
  df_mints_matic = df_mints_matic.append(df_i)

df_mints_matic['date'] = pd.to_datetime(df_mints_matic['transaction.timestamp'], unit='s', origin='unix')
df_mints_matic.head()

print(f'Uniswap Mint transactions from {df_mints_matic.date.min()} to {df_mints_matic.date.max()}')


# %%
# %% volume and TVL for all Uniswap Matic

prefix_files = 'UNISWAP_POLYGON_ALLDATA'
path_files = s_path
l_dir = os.listdir(path_files)
l_files = [_file for _file in l_dir if prefix_files in _file]
df_matic = pd.DataFrame()
print('files to read: ')
print(l_files)

#%% 
for _file in l_files:
  with open(os.path.join(s_path, _file), 'r') as file_json:
    data_json = json.load(file_json)
  # drop header from dict 
  data_json = data_json['data']['uniswapDayDatas']
  df_i = pd.json_normalize(data_json)
  df_matic = df_matic.append(df_i)
df_matic.head()

#%%
df_matic['date'] = pd.to_datetime(df_matic['date'], unit='s', origin='unix')
df_matic[['tvlUSD', 'volumeUSD', 'txCount']] = df_matic[['tvlUSD', 'volumeUSD', 'txCount']].astype(float)
df_matic.set_index(['date'], inplace=True)

# %%
