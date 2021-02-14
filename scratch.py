import concurrent.futures
import glob
import os

import numpy as np
import pandas as pd


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

    def get_unsold(self):
        ls_unsold = []
        statement = "select distinct pair from trade where action = 'BUY' and related = -1"
        results = self.m.execute_sql(statement, False)
        for result in results:
            ls_unsold.append(result[0])
        return ls_unsold

    def do(self):

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
                    if float(df['volume'][idx]) < min_notional * 10000:
                        self.l.log_info("insufficient volume -> " + pair_name)
                        drop = True
                    else:
                        self.m.dict_min_notional[pair_name] = min_notional
                if drop:
                    df.drop(idx, inplace=True)

            ls_pairs = df['pair_name'].tolist()

            ls_unsold = self.get_unsold()

            ls_pairs.extend(ls_unsold)

            ls_pairs = set(ls_pairs)

            self.l.log_info(len(ls_pairs))

            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = []
                try:
                    for pair in ls_pairs:
                        ticker = _tikcer(self.call, self.m, ls_unsold)
                        futures.append(executor.submit(ticker.fetch, pair))
                    for future in concurrent.futures.as_completed(futures):
                        future.result()
                except Exception as e:
                    self.l.log_exception('Error Occured')

            # self.slack.post_message("eligible pairs successfully captured")

        except:
            self.slack.post_message("error occurred in generating eligible pairs")
            self.l.log_exception("Error Occurred")


class _tikcer():

    def __init__(self, call, m, ls_unsold):
        self.call = call
        self.m = m
        self.ls_unsold = ls_unsold

    def fetch(self, pair):
        add_ticker = False
        path = os.path.realpath('.') + "/ticker/" + pair + ".csv"
        stored_df = pd.read_csv(path)
        if len(stored_df.index) < 500:
            stored_df = self.call.get_candle_data(pair, '5m')

        df = self.call.get_candle_data(pair, '5m')
        df = df.sort_index(ascending=False)
        df = df.reset_index()
        df = df.drop(columns=['index'])
        original_df = df
        df['RSI'] = computeRSI(df['close'], 14)
        df['K'], df['D'] = stochastic(df['RSI'], 3, 3, 14)
        df['MACD'] = df['close'].ewm(span=12).mean() - df['close'].ewm(span=26).mean()
        df['MACD_Signal'] = df['MACD'].ewm(span=9).mean()
        df['MACD'] = df['MACD'].multiply(1000000)
        df['MACD_Signal'] = df['MACD_Signal'].multiply(1000000)
        df = df.dropna()
        df = df.reset_index()
        sub_df = df.tail(6)
        sub_df_h = sub_df.head(1)
        sub_df_t = sub_df.tail(1)
        macd_slope = sub_df_t['MACD'].mean() < sub_df_h['MACD'].mean()
        if pair in self.ls_unsold:
            add_ticker = True
        if (sub_df['MACD'].mean() < 0) and (sub_df['MACD_Signal'].mean() < 0) and macd_slope:
            add_ticker = True
        if add_ticker:
            self.m.store_ticker(pair, df.tail(1))
            original_df.to_csv(os.path.realpath('.') + "/ticker/" + pair + ".csv")
