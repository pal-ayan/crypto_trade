import hashlib
import hmac
import time
from datetime import datetime

import requests
from pandas import json_normalize

import json
import master


class call_api():
    def __init__(self, log, env):
        self._log = log
        self.env = env

        self.secret_bytes = bytes(self.env.get_value("COINDCX_SECRET"), encoding='utf-8')

        self._user_info_url = "https://api.coindcx.com/exchange/v1/users/info"
        self._user_balances_url = "https://api.coindcx.com/exchange/v1/users/balances"
        self._active_orders_url = "https://api.coindcx.com/exchange/v1/orders/active_orders"
        self._ticker_url = "https://api.coindcx.com/exchange/ticker"
        self._market_details = "https://api.coindcx.com/exchange/v1/markets_details"
        self._rss_feed_url = "https://status.coindcx.com/history.rss"
        self._candle_base_url = "https://public.coindcx.com/market_data/candles?"
        self._create_order = "https://api.coindcx.com/exchange/v1/orders/create"
        self._cancel_order = "https://api.coindcx.com/exchange/v1/orders/cancel"
        self._order_status = "https://api.coindcx.com/exchange/v1/orders/status"
        self._multiple_order_status = "https://api.coindcx.com/exchange/v1/orders/status_multiple"
        self._url_separator = "&"

    def _call(self, url, body=None):
        start_time = datetime.now()
        self._log.log_info("_call sending request")

        if body is None:
            time_stamp = int(round(time.time() * 1000))
            body = {
                "timestamp": time_stamp
            }

        json_body = json.dumps(body, separators=(',', ':'))

        signature = hmac.new(self.secret_bytes, json_body.encode(), hashlib.sha256).hexdigest()

        headers = {
            'Content-Type': 'application/json',
            'X-AUTH-APIKEY': self.env.get_value("COINDCX_KEY"),
            'X-AUTH-SIGNATURE': signature
        }

        time_diff = datetime.now() - start_time
        self._log.log_info('_call completion took -> %s' % time_diff)

        return requests.post(url, data=json_body, headers=headers)

    def get_user_info(self):
        start_time = datetime.now()
        self._log.log_info("get_user_info method execution started")

        response = self._call(self._user_info_url)
        data = response.json()

        time_diff = datetime.now() - start_time
        self._log.log_info('get_user_info completion took -> %s' % time_diff)

        return json_normalize(data)

    def get_user_balances(self):
        start_time = datetime.now()
        self._log.log_info("get_user_balances method execution started")

        response = self._call(self._user_balances_url)
        data = response.json()
        df = json_normalize(data)
        convert_dict = {
            "balance": float,
            "locked_balance": float
        }

        df = df.astype(convert_dict)

        index_names = df[(df['balance'] <= 0.0) & (df['locked_balance'] <= 0.0)].index
        df.drop(index_names, inplace=True)

        time_diff = datetime.now() - start_time

        self._log.log_debug(master.get_dataframe_info(df))

        self._log.log_info('get_user_balances completion took -> %s' % time_diff)

        return df

    def get_active_orders(self):
        start_time = datetime.now()
        self._log.log_info("get_active_orders method execution started")

        response = self._call(self._active_orders_url)
        data = response.json()
        time_diff = datetime.now() - start_time
        self._log.log_info('get_active_orders completion took -> %s' % time_diff)
        return json_normalize(data['orders'])

    def get_ticker(self):
        start_time = datetime.now()
        self._log.log_info("get_ticker method execution started")

        response = requests.get(self._ticker_url)
        data = response.json()
        time_diff = datetime.now() - start_time
        self._log.log_info('get_ticker completion took -> %s' % time_diff)
        return json_normalize(data)

    def get_current_price(self, market):
        df = self.get_ticker()
        df = df.loc[df['market'] == market]
        return df['last_price'].values[0]

    def get_all_market_details(self):
        start_time = datetime.now()
        self._log.log_info("get_all_market_details method execution started")

        response = requests.get(self._market_details)
        data = response.json()

        time_diff = datetime.now() - start_time
        self._log.log_info('get_all_market_details completion took -> %s' % time_diff)

        return json_normalize(data)

    def get_active_market_details(self):
        start_time = datetime.now()
        self._log.log_info(" get_active_market_details method execution started")

        df = self.get_all_market_details()
        drop_index_names = df[(df['status'] != 'active')].index
        df.drop(drop_index_names, inplace=True)

        time_diff = datetime.now() - start_time

        self._log.log_debug(master.get_dataframe_info(df))

        self._log.log_info('get_active_market_details completion took -> %s' % time_diff)

        return df

    def get_candle_data(self, pair_name, interval, limit=500):
        pair_url = "pair=" + pair_name
        interval_url = "interval=" + str(interval)
        limit_url = "limit=" + str(limit)
        full_url = self._candle_base_url + pair_url + self._url_separator + interval_url + self._url_separator + limit_url
        response = requests.get(full_url)
        data = response.json()
        return json_normalize(data)

    def get_candle_url(self, pair_name, interval, limit=500):
        pair_url = "pair=" + pair_name
        interval_url = "interval=" + str(interval)
        limit_url = "limit=" + str(limit)
        full_url = self._candle_base_url + pair_url + self._url_separator + interval_url + self._url_separator + limit_url
        return full_url

    def get_candle_data_by_time_frame(self, pair_name, interval, start, end):
        pair_url = "pair=" + pair_name
        interval_url = "interval=" + str(interval)
        start_url = "startTime=" + str(start)
        end_url = "endTime=" + str(end)
        full_url = self._candle_base_url + pair_url + self._url_separator + interval_url + self._url_separator + start_url + self._url_separator + end_url
        response = requests.get(full_url)
        data = response.json()
        return json_normalize(data)

    def create_order(self, side, order_type, market, price_per_unit, quantity):
        time_stamp = int(round(time.time() * 1000))
        if order_type == "limit_order":
            body = {
                "side": side,  # Toggle between 'buy' or 'sell'.
                "order_type": order_type,  # Toggle between a 'market_order' or 'limit_order'.
                "market": market,  # Replace 'SNTBTC' with your desired market pair.
                "price_per_unit": price_per_unit,  # This parameter is only required for a 'limit_order'
                "total_quantity": quantity,  # Replace this with the quantity you want
                "timestamp": time_stamp
            }
        else:
            body = {
                "side": side,  # Toggle between 'buy' or 'sell'.
                "order_type": order_type,  # Toggle between a 'market_order' or 'limit_order'.
                "market": market,  # Replace 'SNTBTC' with your desired market pair.
                "total_quantity": quantity,  # Replace this with the quantity you want
                "timestamp": time_stamp
            }
        response = self._call(self._create_order, body)
        data = response.json()
        df = json_normalize(data['orders'])
        return df

    def cancel_order(self, id):
        time_stamp = int(round(time.time() * 1000))
        body = {
            "id": id,  # Enter your Order ID here.
            "timestamp": time_stamp
        }
        response = self._call(self._cancel_order, body)
        data = response.json()
        df = json_normalize(data)
        pass

    def get_order_status(self, id):
        time_stamp = int(round(time.time() * 1000))
        body = {
            "id": id,  # Enter your Order ID here.
            "timestamp": time_stamp
        }
        response = self._call(self._order_status, body)
        data = response.json()
        df = json_normalize(data)
        pass

    def get_multiple_order_status(self, ls_ids):
        time_stamp = int(round(time.time() * 1000))
        body = {
            "id": ls_ids,  # Enter your Order ID here.
            "timestamp": time_stamp
        }
        response = self._call(self._multiple_order_status, body)
        data = response.json()
        df = json_normalize(data)
        pass
