# original: https://www.kaggle.com/code/jacksoncrow/download-nasdaq-historical-data/notebook

import os

offset = 0
limit = 3000
period = os.environ.get('STOCK_DATA_PERIOD', '1y') # valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max


# ## Download all NASDAQ traded symbols
os.system('pip install pandas > /dev/null 2>&1')

import pandas as pd

data = pd.read_csv("http://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt", sep='|')
data_clean = data[data['Test Issue'] == 'N']
symbols = data_clean['NASDAQ Symbol'].tolist()
print('total number of symbols traded = {}'.format(len(symbols)))

# ## Download Historic data


os.system('pip install yfinance > /dev/null 2>&1')
os.system('mkdir hist')

import yfinance as yf
import os, contextlib

from datetime import datetime 
start_time = datetime.now() 




limit = limit if limit else len(symbols)
end = min(offset + limit, len(symbols))
is_valid = [False] * len(symbols)
# force silencing of verbose API
with open(os.devnull, 'w') as devnull:
    with contextlib.redirect_stdout(devnull):
        for i in range(offset, end):
            s = symbols[i]
            data = yf.download(s, period=period)
            if len(data.index) == 0:
                continue
        
            is_valid[i] = True
            data.to_csv('hist/{}.csv'.format(s))

print('Total number of valid symbols downloaded = {}'.format(sum(is_valid)))

time_elapsed = datetime.now() - start_time 
print('Time elapsed (hh:mm:ss.ms) {}'.format(time_elapsed))


valid_data = data_clean[is_valid]
valid_data.to_csv('symbols_valid_meta.csv', index=False)

# ## Separating ETFs and Stocks

# os.system('mkdir stocks')
# os.system('mkdir etfs')

# etfs = valid_data[valid_data['ETF'] == 'Y']['NASDAQ Symbol'].tolist()
# stocks = valid_data[valid_data['ETF'] == 'N']['NASDAQ Symbol'].tolist()

# import shutil
# from os.path import isfile, join

# def move_symbols(symbols, dest):
#     for s in symbols:
#         filename = '{}.csv'.format(s)
#         shutil.move(join('hist', filename), join(dest, filename))
        
# move_symbols(etfs, "etfs")
# move_symbols(stocks, "stocks")

# os.system('rmdir hist')
