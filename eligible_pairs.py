import asyncio
import concurrent.futures
import glob
import os

import numpy as np
from aiohttp import ClientSession

import master

'''
    runs once every hour, at the beginning
'''


class capture:

    def __init__(self, m):
        self.l = m.l
        self.m = m
        self.markets_df = m.markets_df
        self.call = m.call
        self.slack = m.slack

    def clear_tmp(self):
        self.l.log_info('clearing previous ticker json')
        files = glob.glob(os.path.realpath('.') + '/eligible_pair.txt')
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

    async def run(self, ls_pairs):
        tasks = []
        ls_pairs = list(filter(None, ls_pairs))
        # Fetch all responses within one Client session,
        # keep connection alive for all requests.
        async with ClientSession() as session:
            for pair in ls_pairs:
                url = self.call.get_candle_url(pair, '1h')
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
                        futures.append(executor.submit(master.create_eligible_pairs_list, p, pair_json))
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

            if os.path.exists("eligible_pair.txt"):
                os.remove("eligible_pair.txt")

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
                if drop:
                    df.drop(idx, inplace=True)

            ls_pairs = df['pair_name'].tolist()

            ls_pairs = set(ls_pairs)

            ls_pairs = list(filter(None, ls_pairs))

            self.l.log_info(len(ls_pairs))

            future = asyncio.ensure_future(self.run(ls_pairs))
            loop.run_until_complete(future)

            # self.slack.post_message("eligible pairs successfully captured")


        except:
            self.slack.post_message("error occurred in generating eligible pairs")
            self.l.log_exception("Error Occurred")
