# -*- coding: utf-8 -*-
"""
Created on Sat Jan 13 04:56:38 2018

@author: Ureridu
"""

import requests
import json
import pandas


''' This Function gets Kline data from Binance '''

def get_kline(symbol, interval, start=None, end=None, limit=None):
    valid_intervals = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M']
    if interval not in valid_intervals:
        raise TypeError("""Invalid Metric. Please select from:
                        '1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h',
                        '8h', '12h', '1d', '3d', '1w', '1M' """)

    url = ('https://api.binance.com/api/v1/'
           + 'klines'
           + '?symbol=' + symbol
           + '&interval=' + interval
           )

    if start:
        url += '&startTime=' + str(start)
    if end:
        url += '&endTime=' + str(end)
    if limit:
        url += '&limit=' + str(limit)
    print(url)

    bc = 0
    while 1:
        try:
            resp = requests.get(url)
            if '[[' in resp.text:
                break
        except Exception as e:
            print(e)
            bc += 1
            if bc >= 5:
                raise EnvironmentError('Kline Request Failed')

    '''  Example output.  As seen on Binance

    1499040000000,      // Open time
    "0.01634790",       // Open
	"0.80000000",       // High
	"0.01575800",       // Low
	"0.01577100",       // Close
	"148976.11427815",  // Volume
	1499644799999,      // Close time
	"2434.19055334",    // Quote asset volume
	308,                // Number of trades
	"1756.87402397",    // Taker buy base asset volume
	"28.46694368",      // Taker buy quote asset volume
	"17928899.62484339" // Can be ignored

    '''   

    data = json.loads(resp.text)
    columns = ['Open_Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close_Time',
               'Quote_Asset_Volume', 'Number of Trades', 'Taker_Buy_Base_Asset_Volume',
               'Taker_Buy_Quote_Asset_Volume', 'Ignore']

    out_frame = pandas.DataFrame()
    append_dict = {}
    for kline in data:
        for i, val in enumerate(kline):
            append_dict[columns[i]] = val
        out_frame = out_frame.append(append_dict, ignore_index=True)

    out_frame.reset_index(inplace=True, drop=True)
    for col in columns:
        out_frame[col] = out_frame[col].astype(float)
    
    out_frame['Mid_High/Low'] = (out_frame['High'] + out_frame['Low']) / 2
    out_frame['Mid_Open/Close'] = (out_frame['Open'] + out_frame['Close']) / 2

    return out_frame


def kline_combine(symbol, interval, num_intervals, start=None, end=None, limit=None):

    ' 60,000 = 1 sec '
    interval_dict = {
                    '1m': 60 * 60000,
                    '3m': 60 * 60000 * 3,
                    '5m': 60 * 60000 * 5,
                    '15m': 60 * 60000 * 15,
                    '30m': 60 * 60000 * 30,
                    '1h': 60 * 60000 * 60,
                    '2h': 60 * 60000 * 60 * 2,
                    '4h': 60 * 60000 * 60 * 4,
                    '6h': 60 * 60000 * 60 * 6,
                    '8h': 60 * 60000 * 60 * 8,
                    '12h': 60 * 60000 * 60 * 12,
                    '1d': 60 * 60000 * 60 * 24,
                    '3d': 60 * 60000 * 60 * 24 * 3,
                    '1w': 60 * 60000 * 60 * 24 * 7,
                    '1M': 60 * 60000 * 60 * 24 * 31,  # ??? 31 day months ?????
                    }

    int_length = interval_dict[interval]

    out_frame = pandas.DataFrame()
    for i in range(num_intervals):
        print('Pulling Interval #', i)
        inter_frame = get_kline(symbol, interval, end=end)
        last = inter_frame['Open_Time'].iloc[-1]
        end = int(last - int_length)

        out_frame = inter_frame.append(out_frame, ignore_index=True)

#    out_frame['Human Time'] = out_frame['Close_Time'] - min(out_frame['Close_Time']
    out_frame.reset_index(inplace=True, drop=True)
    
    return out_frame


data = kline_combine(symbol='XRPETH', interval='5m', num_intervals=3)


#import asyncio
#import websockets
#
#async def hello():
#    async with websockets.connect('wss://stream.binance.com:9443/ws/ethbtc@depth') as websocket:
#
#
#        greeting = await websocket.recv()
#        print("< {}".format(greeting))
#
#asyncio.get_event_loop().run_until_complete(hello())
#
#
#import websockets
#
#with websockets.connect('wss://stream.binance.com:9443/ws/ethbtc@depth') as websocket:
#    resp = websocket.recv()