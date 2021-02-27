import asyncio
import concurrent.futures
import glob
import os

from aiohttp import ClientSession
from pandas import json_normalize

import json
import master

'''
    Runs every 5 minutes
'''


class capture:

    def __init__(self, m):
        self.l = m.l
        self.m = m
        self.markets_df = m.markets_df
        self.t_markets_df = m.t_markets_df
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

    def remove_small_bal(self, balance_df, ls_unsold_pair):
        ticker_df = self.call.get_ticker()
        total_market_details = self.t_markets_df
        for idx in balance_df.index:
            curr = balance_df['currency'][idx]
            bal = balance_df['balance'][idx]
            df = total_market_details[total_market_details['target_currency_short_name'] == curr]
            for index in df.index:
                min_notional = df['min_notional'][index]
                market_name = df['coindcx_name'][index]
                pair = df['pair'][index]
                current_price = self.call.get_current_price(market_name,ticker_df)
                units = float(min_notional) / float(current_price)
                if bal < units and pair in ls_unsold_pair:
                    ls_unsold_pair.remove(pair)
        return ls_unsold_pair

    async def run(self, ls_pairs):
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
                        self.l.log_debug('calling - '+p)
                        futures.append(executor.submit(ticker.fetch, p, pair_json))
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

            ls_pairs = []

            bal_df, ls_unsold_curr = self.call.get_unsold()
            ls_unsold = []
            markets_df = self.t_markets_df
            for idx in markets_df.index:
                pair_name = markets_df['pair'][idx]
                min_notional = markets_df['min_notional'][idx]
                step = markets_df['step'][idx]
                self.m.dict_min_notional[pair_name] = min_notional
                self.m.dict_step[pair_name] = step
                if markets_df['target_currency_short_name'][idx] in ls_unsold_curr:
                    ls_unsold.append(pair_name)

            with open('eligible_pair.txt', 'r') as the_file:
                for line in the_file:
                    ls_pairs.append(line.strip())

            ls_unsold = self.remove_small_bal(bal_df, ls_unsold)

            ls_pairs.extend(ls_unsold)

            ls_pairs = set(ls_pairs)

            ls_pairs = list(filter(None, ls_pairs))

            self.l.log_info(len(ls_pairs))

            future = asyncio.ensure_future(self.run(ls_pairs))
            loop.run_until_complete(future)

            # self.slack.post_message("eligible pairs successfully captured")


        except:
            self.slack.post_message("error occurred in generating candle data")
            self.l.log_exception("Error Occurred")


class _tikcer():

    def __init__(self, call, m):
        self.call = call
        self.m = m

    def fetch(self, pair, json_str):
        try:
            my_json = json_str.decode('utf-8').replace("'", '"')
            data = json.loads(my_json)
            df = json_normalize(data)
            df = df.sort_index(ascending=False)
            df['RSI'] = master.computeRSI(df['close'], 14)
            df['K'], df['D'] = master.stochastic(df['RSI'], 3, 3, 14)
            df['MACD'] = df['close'].ewm(span=12).mean() - df['close'].ewm(span=26).mean()
            df['MACD_Signal'] = df['MACD'].ewm(span=9).mean()
            df['MACD'] = df['MACD'].multiply(1000000)
            df['MACD_Signal'] = df['MACD_Signal'].multiply(1000000)
            df = df.iloc[[len(df) - 2]]
            self.m.store_ticker(pair, df)
            #df.to_csv(os.path.realpath('.') + "/ticker/" + pair + ".csv")
        except:
            self.m.l.log_exception("error with - " + pair)
