#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
uniswap subgraph 

@author: ThaFranklin

Created on 14/02/2022
"""
# %% 
import os 
import sys
import matplotlib.pyplot as plt
import requests
import numpy as np
import pandas as pd 

pd.set_option('display.max_columns',100)
pd.set_option('precision', 3)
pd.set_option('display.float_format', lambda x: '%.3f' % x)
# pd.set_option('display.max_colwidth',15) 

s_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(s_path)

#%% 
# function to use requests.post to make an API call to the subgraph url
def run_query(s_query, http_query='https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3'):

    # endpoint where you are making the request
    request = requests.post(http_query, json={'query': s_query})
    if request.status_code == 200:
        return request.json()


# reading basic mints info extracted from Uniswap subgraph
mints = """
{
  mints(first: 1000, skip: %d){
    timestamp
    pool{
      id
    }
    token0{
      symbol
    }
    token1{
      symbol
    }
    origin
    sender
    owner
    amount
    amount0
    amount1
    amountUSD
    tickLower
    tickUpper
    transaction{
      id
      blockNumber
      gasUsed
      gasPrice
    }
  }
}
"""
# skip = 0
# mints % (skip)

df_mints = pd.DataFrame()

for i in range(7):
  # The `skip` argument must be between 0 and 5000
  skip = ((i-1) * 1000) if i > 0 else 0
  # can't use str.format() because of {} unformatted in query string
  result = run_query((mints % skip))
  try:
    df_i = pd.json_normalize(result, record_path=['data','mints'])
    df_mints = df_mints.append(df_i, ignore_index=True)
  except KeyError:
    print(result)
  
df_mints['date'] = pd.to_datetime(df_mints['timestamp'], unit='s', origin='unix')
df_mints['type'] = 'mint'
# %%
burns = """
{ 
  burns(first: 1000, skip: %d){
    timestamp
    pool{
      id
    }
    token0{
      symbol
    }
    token1{
      symbol
    }
    origin
    owner
    amount
    amount0
    amount1
    amountUSD
    tickLower
    tickUpper
    transaction{
      id
      blockNumber
      gasUsed
      gasPrice
    }
  }
}
"""
df_burns = pd.DataFrame()

for i in range(7):
  # The `skip` argument must be between 0 and 5000
  skip = ((i-1) * 1000) if i > 0 else 0
  # can't use str.format() because of {} unformatted in query string
  result = run_query((burns % skip))
  try:
    df_i = pd.json_normalize(result, record_path=['data','burns'])
    df_burns = df_mints.append(df_i, ignore_index=True)
  except KeyError:
    print(result)
  
df_burns['date'] = pd.to_datetime(df_burns['timestamp'], unit='s', origin='unix')
df_burns['type'] = 'burn'
# %%
swaps = """
{
  swaps(first: 1000, skip: %d, orderDirection: desc){
    timestamp
    pool{
      id
    }
    token0{
      symbol
    }
    token1{
      symbol
    }
    transaction{
      id
      blockNumber
    }
    sender
    recipient
    origin
    amount0
    amount1
    amountUSD
    tick
  }
}
"""

df_swaps = pd.DataFrame()

for i in range(7):
  # The `skip` argument must be between 0 and 5000
  skip = ((i-1) * 1000) if i > 0 else 0
  # can't use str.format() because of {} unformatted in query string
  result = run_query((swaps % skip))
  try:
    df_i = pd.json_normalize(result, record_path=['data','swaps'])
    df_swaps = df_swaps.append(df_i, ignore_index=True)
  except KeyError:
    print(result)
  
df_swaps['date'] = pd.to_datetime(df_swaps['timestamp'], unit='s', origin='unix')
df_swaps['type'] = 'swap'

# %% all blockNumber with some tx
df_blockNumber_swap = df_swaps.groupby('transaction.blockNumber')['timestamp'].agg(['count', 'max'])
df_blockNumber_mint = df_mints.groupby('transaction.blockNumber')['timestamp'].agg(['count', 'max'])
df_blockNumber_burn = df_burns.groupby('transaction.blockNumber')['timestamp'].agg(['count', 'max'])

l_cols = ['transaction.blockNumber', 'count', 'max']
df_blockNumber = df_blockNumber_mint.merge(df_blockNumber_burn, on=l_cols[0], how = 'outer', suffixes=('_mint', '_burn'))
df_blockNumber = df_blockNumber.merge(df_blockNumber_swap, on=l_cols[0], how = 'outer')
df_blockNumber['date'] = pd.to_datetime(df_blockNumber['max'], unit='s', origin='unix')

# filter blockNumber with mint, burn and swap
df_jit_lp = df_blockNumber.loc[pd.notna(df_blockNumber['count_burn']) & pd.notna(df_blockNumber['count_mint'])& pd.notna(df_blockNumber['count']), :]

df_jit_lp.style
#%% 
def aggregate_and_plot(type):
  # type = 'swaps'
  print(type)

  if type == 'swaps':
    df = df_swaps.copy()
  elif type == 'burns':
    df = df_burns.copy()
  elif type == 'mints':
    df = df_mints.copy()
  else:
    print('IS NOT A VALID TYPE')
    return

  df['yyyy-mm'] = pd.to_datetime(df['timestamp'], origin='unix', unit='s').dt.strftime('%Y-%m')
  df['pair'] = df['token0.symbol'] + '_' + df['token1.symbol'] 
  df['amountUSD'] = df['amountUSD'].astype(float)
  # df['amountUSD'] = np.log(df['amountUSD'].astype(float)).fillna(0)
  df_pool = df.pivot_table(
    values= 'amountUSD',
    columns= 'pair', 
    index= 'yyyy-mm', 
    aggfunc= 'sum',
    # fill_value= 0
    )

  # print(df_pool.head())
  # print((df_pool.mean(axis=0) > 100000).sum())
  # col_mask = df_pool.loc[:, df_pool.mean(axis=0) > 500000].columns
  # col_mask = df_pool.loc[:, df_pool.mean(axis=0) > 10].columns
  # print(col_mask)
  df_pool.sum(axis=1).plot.bar(logy=True, colormap='Set1', figsize=(5,5))

  # df_pool.loc[:,col_mask].plot.bar(logy=True, figsize=(10,10))
  # plt.legend(loc='upper right', bbox_to_anchor=(0.5, 0., 0.5, 0.5))

aggregate_and_plot('burns')

#%% 
aggregate_and_plot('mints')

#%% 
aggregate_and_plot('burns')

#%% 
df_blockNumber['swap'] = df_swaps.groupby('transaction.blockNumber')['timestamp'].count()

df_jit_lp.sort_values(
  ['date', 'transaction.id', 'pool.id', 'origin'], 
  ignore_index= True,
  inplace=True
  )