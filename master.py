import io
import os
import sqlite3 as sql
import threading
from datetime import date, datetime, timedelta
from threading import RLock
from time import sleep

import numpy as np
import pandas as pd
from env.load_env import load_env
from pandas import json_normalize

import coindcx_api_caller as cdx
import json
from log import log
from slack_util import slack_util


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


def get_dataframe_info(df):
    buf = io.StringIO()
    df.info(buf=buf)
    return buf.getvalue()


def get_date_as_ms_string(days_delta=0):
    x = datetime.today() - timedelta(days=days_delta)
    dt = date(year=x.year, month=x.month, day=x.day)
    epoch = datetime.utcfromtimestamp(0)
    dt = datetime.combine(dt, datetime.min.time())
    dt = (dt - epoch).total_seconds() * 1000
    dt = str(dt)
    return dt[:dt.index('.')]


def store_json(df, path):
    df.reset_index(drop=True, inplace=True)
    df.to_json(path)


def get_start_of_day_ms():
    x = datetime.today()
    dt = date(year=x.year, month=x.month, day=x.day)
    epoch = datetime.utcfromtimestamp(0)
    dt = datetime.combine(dt, datetime.min.time())
    dt = (dt - epoch).total_seconds() * 1000
    dt = str(dt)
    return dt[:dt.index('.')]


def create_eligible_pairs_list(pair, json_str):
    add_ticker = False
    my_json = json_str.decode('utf-8').replace("'", '"')
    data = json.loads(my_json)
    df = json_normalize(data)
    df = df.sort_index(ascending=False)
    df['MACD'] = df['close'].ewm(span=12).mean() - df['close'].ewm(span=26).mean()
    df['MACD'] = df['MACD'].multiply(1000000)
    sub_df = df.tail(25).head(24)
    macd_arr = sub_df['MACD'].tolist()
    gradient = np.gradient(macd_arr)
    mean_gradient = np.mean(gradient)
    standard_deviation = np.std(macd_arr)
    macd_mean = sub_df['MACD'].mean()
    if (macd_mean < 0) and (mean_gradient > 0) and (abs(macd_mean) < abs(standard_deviation)):
        add_ticker = True
    if add_ticker:
        with open('eligible_pair.txt', 'a') as the_file:
            the_file.write(pair + '\n')


class master:

    def __init__(self, l):
        self.l = log() if l is None else l
        self.lock = RLock()
        self.env = load_env()

        self.slack = slack_util(self.l, self.env)

        self.call = cdx.call_api(self.l, self.env, self)

        self._created_threads = []
        self.markets_df = None
        self.t_markets_df = None
        self.dict_ticker_df = {}
        self.dict_min_notional = {}
        self.dict_step = {}
        self.l.log_info('MASTER INIT COMPLETE')

    def acquire_lock(self):
        self.l.log_debug('attempting to acquire lock')
        self.lock.acquire(blocking=True)
        self.l.log_debug('lock acquired')

    def release_lock(self):
        self.lock.release()
        self.l.log_debug('lock released')

    def join_threads(self):
        for thread in self._created_threads:
            if thread.is_alive():
                thread.join(timeout=5)
                self.l.log_debug('finishing thread ' + thread.name)
            else:
                self.l.log_debug('thread is already dead ' + thread.name)

    def run_thread(self, name, function, ls_args):
        t = threading.Thread(target=function, args=tuple(ls_args), name="Thread-" + name, daemon=True)
        t.start()
        self.l.log_debug('starting thread ' + t.name)
        self._created_threads.append(t)

    def init_markets_df(self):

        umd_path = os.path.realpath('.') + self.env.get_value('USABLE_MARKET_DETAILS_PATH')
        md_path = os.path.realpath('.') + self.env.get_value('MARKET_DETAILS_PATH')

        try:
            mtime = os.path.getmtime(umd_path)
            val = datetime.fromtimestamp(int(mtime))
            last_access_date = val.strftime("%Y-%m-%d")
        except FileNotFoundError as f:
            last_access_date = 'N/A'

        current_date = datetime.today().strftime('%Y-%m-%d')

        if current_date == last_access_date:
            self.markets_df = pd.read_json(umd_path)
            self.t_markets_df = pd.read_json(md_path)
            return self.markets_df
        else:
            df = self.call.get_active_market_details()
            complete_df = df.copy()
            self.run_thread("store_market_details", store_json, [df, md_path])

        base_currency_list = self.env.get_value('BASE_CURR_LIST').split(',')

        for idx in df.index:
            base_currency_short_name = df['base_currency_short_name'][idx]
            if base_currency_short_name not in base_currency_list:
                df.drop(idx, inplace=True)
        self.markets_df = df
        self.t_markets_df = complete_df
        self.run_thread("store_usable_market_details", store_json, [df, umd_path])
        return df

    def execute_sql(self, statement, commit=False, db=os.path.realpath('.') + '/db/trade.db'):
        ls_results = []
        retry_count = 0
        self.acquire_lock()
        conn = sql.connect(db, isolation_level='EXCLUSIVE')
        while True:
            try:
                self.l.log_debug(statement)
                results = conn.execute(statement)
                for result in results:
                    ls_results.append(result)
                break
            except sql.OperationalError as e:
                self.l.log_exception('Error Occured while executing statement -> ' + statement)
                if retry_count >= 10:
                    self.l.log_error('maximum retry reached for statement -> ' + statement)
                    self.release_lock()
                    raise Exception('maximum retry reached for statement -> ' + statement)
                sleep(0.5)
                retry_count += 1
                continue
        if commit:
            self.l.log_debug('committing')
            conn.commit()
        conn.close()
        self.release_lock()
        return ls_results

    def execute_sql_many(self, statement, args, db=os.path.realpath('.') + '/db/trade.db'):
        self.acquire_lock()
        conn = sql.connect(db, isolation_level='EXCLUSIVE')
        cur = conn.cursor()
        try:
            self.l.log_debug(statement)
            for arg in args:
                self.l.log_debug(arg)
            cur.executemany(statement, args)
        except sql.OperationalError:
            raise Exception('maximum retry reached for statement -> ' + statement)
        cur.close()
        self.l.log_debug('committing')
        conn.commit()
        conn.close()
        self.release_lock()

    def store_ticker(self, pair, df):
        self.dict_ticker_df[pair] = df

    def get_balance(self):
        ret_total = {}

        base_currency_list = self.env.get_value('BASE_CURR_LIST').split(',')

        for curr in base_currency_list:
            ret_total[curr] = 0.0

        statement = '''
            select t.base_currency as c, sum(t.final_price) as b
            FROM
            orders t
            where 
            t.side = 'buy'
            and t.related = -1
            and status = 'filled'
            group by t.base_currency
        '''
        results = self.execute_sql(statement.strip())
        for result in results:
            curr = result[0]
            if curr not in base_currency_list:
                continue
            val = ret_total[curr] if curr in ret_total else 0.0
            ret_total[curr] = val + float(result[1])

        df = self.call.get_user_balances()

        for key in ret_total:
            sub_df = df[df['currency'] == key]
            bal = 0.0
            locked_bal = 0.0
            val = ret_total[key]
            for idx in sub_df.index:
                bal = sub_df['balance'][idx]
                locked_bal = sub_df['locked_balance'][idx]
            ret_total[key] = val + bal + locked_bal
        return ret_total
