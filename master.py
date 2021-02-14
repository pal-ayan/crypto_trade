import io
import os
import sqlite3 as sql
import threading
from datetime import date, datetime, timedelta
from threading import RLock
from time import sleep

import pandas as pd

import coindcx_api_caller as cdx
from env.load_env import load_env
from log import log
from slack_util import slack_util


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


class master:

    def __init__(self, l):
        self.l = log() if l is None else l
        self.lock = RLock()
        self.env = load_env()

        self.slack = slack_util(self.l, self.env)

        self.call = cdx.call_api(self.l, self.env)

        self._created_threads = []
        self.markets_df = None
        self.dict_ticker_df = {}
        self.dict_min_notional = {}
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
            return self.markets_df
        else:
            df = self.call.get_active_market_details()
            self.run_thread("store_market_details", store_json, [df, md_path])

        base_currency_list = self.env.get_value('BASE_CURR_LIST').split(',')

        for idx in df.index:
            base_currency_short_name = df['base_currency_short_name'][idx]
            if base_currency_short_name not in base_currency_list:
                df.drop(idx, inplace=True)
            else:
                drop = False
                min_notional = float(df['min_notional'][idx])
                step = df['step'][idx]
                if step / min_notional > 100:
                    drop = True
                if drop:
                    df.drop(idx, inplace=True, errors="ignore")
        self.markets_df = df
        self.run_thread("store_usable_market_details", store_json, [df, umd_path])
        return df

    def execute_sql(self, statement, commit=False, db=os.path.realpath('.') + '/db/buy_sell_test.db'):
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

    def store_ticker(self, pair, df):
        self.dict_ticker_df[pair] = df

    def get_balance(self):
        ret_available = {}
        ret_total = {}
        statement = '''
            select combo.c as currency, sum(combo.b) as available_bal, sum(combo.tb) as total_bal 
            from (select currency as c, balance as b, 0 as tb from balances
                  union
                  select total_bal.c as c, 0 as b, sum(total_bal.b) as tb from (select t.currency as c, sum(t.total_price) as b
                    FROM
                    trade t
                    where 
                    t.action = 'BUY'
                    and t.related = -1
                    group by t.currency
                    UNION
                    select currency as c, balance as b from balances) as total_bal
                    group by total_bal.c) 
            as combo
            group by combo.c
        '''
        results = self.execute_sql(statement.strip())
        for result in results:
            ret_total[result[0]] = float(result[2])
            ret_available[result[0]] = float(result[1])

        return ret_available, ret_total
