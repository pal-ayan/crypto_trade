import asyncio
import concurrent.futures
import glob
import os

import numpy as np
from aiohttp import ClientSession
from pandas import json_normalize

import json


def computeRSI(data, time_window):
    diff = data.diff(1).dropna()
    up_chg = 0 * diff
    down_chg = 0 * diff
    up_chg[diff > 0] = diff[diff > 0]
    down_chg[diff < 0] = diff[diff < 0]
    up_chg_avg = up_chg.ewm(com=time_window - 1, min_periods=time_window).mean()
    down_chg_avg = down_chg.ewm(com=time_window - 1, min_periods=time_window).mean()
    rs = abs(up_chg_avg / down_chg_avg)
    rsi = 100 - 100 / (1 + rs)
    return rsi


def stochastic(data, k_window, d_window, window):
    min_val = data.rolling(window=window, center=False).min()
    max_val = data.rolling(window=window, center=False).max()
    stoch = ((data - min_val) / (max_val - min_val)) * 100
    K = stoch.rolling(window=k_window, center=False).mean()
    D = K.rolling(window=d_window, center=False).mean()
    return K, D


class capture:

    def __init__(self, m):
        self.l = m.l
        self.m = m
        self.markets_df = m.markets_df
        self.call = m.call
        self.slack = m.slack

    def clear_tmp(self):
        self.l.log_info('clearing previous ticker json')
        files = glob.glob(os.path.realpath('.') + '/ticker/*.csv')
        for f in files:
            try:
                os.remove(f)
            except OSError:
                self.l.log_warn('error while deleting -> ' + f)
                continue
        self.l.log_info('ticker json cleared')

    def get_unsold_pairs(self, ls_unsold):
        df = self.markets_df.loc[self.markets_df['target_currency_short_name'].isin(ls_unsold)]
        return df['pair'].tolist()

    async def run(self, ls_pairs, ls_unsold):
        tasks = []

        # Fetch all responses within one Client session,
        # keep connection alive for all requests.
        async with ClientSession() as session:
            for pair in ls_pairs:
                url = self.call.get_candle_url(pair, '5m')
                task = asyncio.ensure_future(self.fetch(pair, url, session))
                tasks.append(task)

            responses = await asyncio.gather(*tasks)
            # you now have all response bodies in this variable
            self.l.log_debug('Responses Gathered -> ' + str(len(responses)))
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = []
                try:
                    for response in responses:
                        p = response[0]
                        pair_json = response[1]
                        ticker = _tikcer(self.call, self.m)
                        futures.append(executor.submit(ticker.fetch, p, pair_json, p in ls_unsold))
                    for future in concurrent.futures.as_completed(futures):
                        future.result()
                except:
                    self.l.log_exception('Error Occured')
            self.l.log_debug('Candle data formatted')

    async def fetch(self, pair, url, session):
        self.l.log_debug(url)
        async with session.get(url) as response:
            return [pair, await response.read()]

    def do(self):
        loop = asyncio.get_event_loop()
        try:

            #self.clear_tmp()

            df = self.call.get_ticker()

            markets_df = self.markets_df

            coindcx_name_list = markets_df['coindcx_name'].tolist()

            df['pair_name'] = np.nan

            for idx in df.index:
                drop = False
                if df['market'][idx] not in coindcx_name_list:
                    drop = True
                else:
                    index = markets_df[markets_df['coindcx_name'] == df['market'][idx]].index
                    pair_name = markets_df['pair'][index].values[0]
                    min_notional = markets_df['min_notional'][index].values[0]
                    df.loc[idx, 'pair_name'] = pair_name
                    self.m.dict_min_notional[pair_name] = min_notional
                    if float(df['volume'][idx]) < min_notional * 10000:
                        self.l.log_info("insufficient volume -> " + pair_name)
                        drop = True
                if drop:
                    df.drop(idx, inplace=True)

            ls_pairs = df['pair_name'].tolist()

            ls_unsold = self.call.get_unsold()

            for base_curr in self.m.env.get_value('BASE_CURR_LIST').split(','):
                if base_curr in ls_unsold:
                    ls_unsold.remove(base_curr)

            ls_unsold_pairs = self.get_unsold_pairs(ls_unsold)

            ls_pairs.extend(ls_unsold_pairs)

            ls_pairs = set(ls_pairs)

            self.l.log_info(len(ls_pairs))

            future = asyncio.ensure_future(self.run(ls_pairs, ls_unsold_pairs))
            loop.run_until_complete(future)

            # self.slack.post_message("eligible pairs successfully captured")


        except:
            # self.slack.post_message("error occurred in generating eligible pairs")
            self.l.log_exception("Error Occurred")


class _tikcer():

    def __init__(self, call, m):
        self.call = call
        self.m = m

    def fetch(self, pair, json_str, is_unsold):
        add_ticker = False
        my_json = json_str.decode('utf-8').replace("'", '"')
        data = json.loads(my_json)
        df = json_normalize(data)
        df = df.sort_index(ascending=False)
        df['RSI'] = computeRSI(df['close'], 14)
        df['K'], df['D'] = stochastic(df['RSI'], 3, 3, 14)
        df['MACD'] = df['close'].ewm(span=12).mean() - df['close'].ewm(span=26).mean()
        df['MACD_Signal'] = df['MACD'].ewm(span=9).mean()
        df['MACD'] = df['MACD'].multiply(1000000)
        df['MACD_Signal'] = df['MACD_Signal'].multiply(1000000)
        sub_df = df.tail(6)
        sub_df_h = sub_df.head(1)
        sub_df_t = sub_df.tail(1)
        macd_slope = sub_df_t['MACD'].mean() < sub_df_h['MACD'].mean()
        if is_unsold:
            add_ticker = True
        if (sub_df['MACD'].mean() < 0) and (sub_df['MACD_Signal'].mean() < 0) and macd_slope:
            add_ticker = True
        if add_ticker:
            #df.to_csv(os.path.realpath('.') + "/ticker/" + pair + ".csv")
            df = df.iloc[[len(df) - 2]]
            self.m.store_ticker(pair, df)