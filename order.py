import threading
from threading import Lock
from time import sleep

from coindcx_api_caller import call_api
from log import log
from utility import util


class order:

    def __init__(self, markets_df):
        self._lock = Lock()
        self._active_threads = []
        self._l = log()
        self._call = call_api()
        self._u = util()
        self.markets_df = markets_df

    def __del__(self):
        for thread in self._active_threads:
            thread.join()
            self._l.log_debug('finishing thread ' + thread.name)

    def _store_order(self, df):
        substitution_dict = df.to_dict(orient='records')[0]
        statement = '''
            insert into orders (
                id,
                market,
                order_type,
                side,
                status,
                fee_amount,
                fee,
                total_quantity,
                remaining_quantity,
                avg_price,
                price_per_unit,
                create_dt,
                update_dt
            ) values (
                %(id)s,
                %(market)s,
                %(order_type)s,
                %(side)s,
                %(status)s,
                %(fee_amount)s,
                %(fee)s,
                %(total_quantity)s,
                %(remaining_quantity)s,
                %(avg_price)s,
                %(price_per_unit)s,
                %(create_dt)s,
                %(update_dt)s
            )
        ''' % substitution_dict
        self._u.execute_sql(statement.strip(), True, "./db/orders.db")

    def _run_thread(self, name, function, *args):
        t = threading.Thread(target=function, args=tuple(*args), name="Thread-"+name)
        t.start()
        self._l.log_debug('starting thread ' + t.name)
        self._active_threads.append(t)

    def _get_current_price(self, market):
        df = self._call.get_ticker()
        index = df[df['market'] == market].index
        return df['last_price'][index].values[0]

    def _get_order_type(self, market):
        index = self.markets_df[self.markets_df['coindcx_name'] == market].index
        order_types = self.markets_df['order_types'][index]
        """
        TODO: find out how the data frame stores order types
        """
        pass

    def place_buy_order(self, market, quantity):
        while self._lock.locked():
            self._l.log_debug("place_buy_order is locked")
            sleep(1)
        self._lock.acquire(blocking=True, timeout=2)
        order_types = self._get_order_type(market)
        if "market_order" in order_types:
            order_type = "market_order"
            price_per_unit = None
        elif "limit_order" in order_types:
            order_type = "limit_order"
            price_per_unit = self._get_current_price(market)
        df = self._call.create_order('buy', order_type, market, price_per_unit, quantity)
        self._lock.release()
        self._run_thread(market, self._store_order, df)
        return df
