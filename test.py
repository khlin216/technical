import time
import datetime

import os
import sys

import hkfdb
import yfinance as yf

import pandas as pd
import numpy as np

import plotguy
import itertools

import technical.trendline


pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

client = hkfdb.Database('')

data_folder = 'data'
secondary_data_folder = 'secondary_data'
backtest_output_folder = 'backtest_output'
signal_output_folder = 'signal_output'

if not os.path.isdir(data_folder):
    os.mkdir(data_folder)
if not os.path.isdir(secondary_data_folder):
    os.mkdir(secondary_data_folder)
if not os.path.isdir(backtest_output_folder):
    os.mkdir(backtest_output_folder)
if not os.path.isdir(signal_output_folder):
    os.mkdir(signal_output_folder)

py_filename = os.path.basename(__file__).replace('.py', '')


def get_hist_data(code_list, start_date, end_date, freq,
                  data_folder, file_format, update_data,
                  market):

    start_date_int = int(start_date.replace('-', ''))
    end_date_int = int(end_date.replace('-', ''))

    df_dict = {}
    for code in code_list:

        file_path = os.path.join(data_folder, code + '_' + freq + '.' + file_format)

        if os.path.isfile(file_path) and not update_data:
            if file_format == 'csv':
                df = pd.read_csv(file_path)
                df['datetime'] = pd.to_datetime(df['datetime'], format='%Y-%m-%d')
                df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
                df = df.set_index('datetime')
            elif file_format == 'parquet':
                df = pd.read_parquet(file_path)
                df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')

            print(datetime.datetime.now(), 'successfully read data')
        else:
            if market == 'HK':
                df = client.get_hk_stock_ohlc(
                    code, start_date_int, end_date_int, freq, price_adj=True, vol_adj=True)
                df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')

            elif market == 'US':
                ticker = yf.Ticker(code)
                df = ticker.history(start=start_date, end=end_date)
                df = df[['Open', 'High', 'Low', 'Close', 'Volume']]
                df = df[df['Volume'] > 0]
                df.columns = map(str.lower, df.columns)
                df = df.rename_axis('datetime')
                df['date'] = df.index.date
                df = df[['date', 'open', 'high', 'low', 'close', 'volume']]

            time.sleep(1)
            if file_format == 'csv':
                df.to_csv(file_path)
            elif file_format == 'parquet':
                df.to_parquet(file_path)
            print(datetime.datetime.now(), 'successfully get data from data source')

        df['pct_change'] = df['close'].pct_change()

        df_dict[code] = df

    return df_dict


def get_secondary_data(df_dict):

    for code, df in df_dict.items():
        df['candle'] = df['close'] / df['open'] - 1
        df_dict[code] = df

    return df_dict


def get_sec_profile(code_list, market, sectype, initial_capital):

    sec_profile = {}
    lot_size_dict = {}

    if market == 'HK':
        if sectype == 'STK':
            info = client.get_basic_hk_stock_info()
            for code in code_list:
                lot_size = int(info[info['code'] == code]['lot_size'])
                lot_size_dict[code] = lot_size
        else:
            for code in code_list:
                lot_size_dict[code] = 1

    elif market == 'US':
        for code in code_list:
            lot_size_dict[code] = 1

    sec_profile['market'] = market
    sec_profile['sectype'] = sectype
    sec_profile['initial_capital'] = initial_capital
    sec_profile['lot_size_dict'] = lot_size_dict

    if sectype == 'STK':
        if market == 'HK':
            sec_profile['commission_rate'] = 0.03 * 0.01
            sec_profile['min_commission'] = 3
            sec_profile['platform_fee'] = 15
        if market == 'US':
            sec_profile['commission_each_stock'] = 0.0049
            sec_profile['min_commission'] = 0.99
            sec_profile['platform_fee_each_stock'] = 0.005
            sec_profile['min_platform_fee'] = 1

    return sec_profile


if __name__ == '__main__':

    start_date = '2022-01-01'
    end_date = '2022-12-31'
    freq = '1D'
    market = 'HK'
    sectype = 'STK'
    file_format = 'csv'

    initial_capital = 200000

    update_data = False
    run_mode = 'backtest'
    summary_mode = False
    read_only = False
    number_of_core = 60

    code_list = ['00388', '00939']
    para_dict = {
        'code': code_list,
        'candle_direction': ['positive', 'negative'],
        'candle_len': [2],
        'sma_len': [10, 20],
        'sma_direction': ['above', 'below', 'whatever'],
        'std_ratio_threshold': [0.5, 1],
        'profit_target': [3, 6],
        'stop_loss': [2, 4],
        'holding_day': [5],
    }

    df_dict = get_hist_data(code_list, start_date, end_date, freq,
                            data_folder, file_format, update_data,
                            market)


    # Adding stock code as a column in each dataframe and then concatenating them
    combined_df = pd.concat(
        [df.assign(stock_code='00388') for code, df in df_dict.items()],
        ignore_index=True
    )

    print(combined_df)


    # Displaying the combined dataframe
    print(combined_df.head())

    trend = technical.trendline.gentrends(combined_df)

    print(trend)

