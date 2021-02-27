import os
import time

from master import master


class order:

    def __init__(self, m):
        self.m = m
        self._l = m.l
        self._call = m.call
        self.markets_df = m.t_markets_df

    def store_buy_order(self, pair, df):
        substitution_dict = df.to_dict(orient='records')[0]
        substitution_dict['pair'] = pair
        s = pair.split("-")[1]
        substitution_dict['base_currency'] = s.split("_")[1]
        substitution_dict['target_currency'] = s.split("_")[0]
        substitution_dict['total_price'] = substitution_dict['price_per_unit'] * substitution_dict['total_quantity']
        statement = '''
            insert into orders (
                order_id,
                pair,
                market,
                base_currency,
                target_currency,
                price,
                units,
                total_price,
                created_time,
                updated_time,
                side,
                order_type,
                status
            ) values (
                '%(id)s',
                '%(pair)s',
                '%(market)s',
                '%(base_currency)s',
                '%(target_currency)s',
                %(price_per_unit)s,
                %(total_quantity)s,
                %(total_price)s,
                %(created_at)s,
                %(updated_at)s,
                '%(side)s',
                '%(order_type)s',
                '%(status)s'
            )
        ''' % substitution_dict
        self.m.execute_sql(statement.strip(), True)

    def store_sell_order(self, pair, df, ls_buy_ids):
        substitution_dict = df.to_dict(orient='records')[0]
        substitution_dict['pair'] = pair
        s = pair.split("-")[1]
        substitution_dict['base_currency'] = s.split("_")[1]
        substitution_dict['target_currency'] = s.split("_")[0]
        substitution_dict['total_price'] = substitution_dict['price_per_unit'] * substitution_dict['total_quantity']
        substitution_dict['ls_buy_ids'] = ','.join(ls_buy_ids)
        statement = '''
            insert into orders (
                order_id,
                pair,
                market,
                base_currency,
                target_currency,
                price,
                units,
                total_price,
                created_time,
                updated_time,
                side,
                order_type,
                status,
                inv_rel
            ) values (
                '%(id)s',
                '%(pair)s',
                '%(market)s',
                '%(base_currency)s',
                '%(target_currency)s',
                %(price_per_unit)s,
                %(total_quantity)s,
                %(total_price)s,
                %(created_at)s,
                %(updated_at)s,
                '%(side)s',
                '%(order_type)s',
                '%(status)s',
                %(ls_buy_ids)s
            )
        ''' % substitution_dict
        self.m.execute_sql(statement.strip(), True)

    def _get_current_price(self, market):
        df = self._call.get_ticker()
        index = df[df['market'] == market].index
        return df['last_price'][index].values[0]

    def _get_order_type(self, market):
        df = self.markets_df[self.markets_df['coindcx_name'] == market]
        sub_df = df['order_types']
        return sub_df.tolist()[0]

    def place_order(self, side, pair, market, quantity, current_price=None, ls_buy_ids=None):
        order_types = self._get_order_type(market)
        order_type = None
        price_per_unit = None
        if "market_order" in order_types:
            order_type = "market_order"
            price_per_unit = None
        elif "limit_order" in order_types:
            order_type = "limit_order"
            price_per_unit = self._call.get_current_price(market) if current_price is None else current_price
        df = self._call.create_order(side, order_type, market, price_per_unit, quantity)
        order_id = df['order_id'][0]
        self._l.log_debug("placed order " + order_id)
        if os.path.exists(os.path.realpath('.') + "/csv/orders.csv"):
            df.to_csv(os.path.realpath('.') + "/csv/orders.csv", mode='a', header=False, index=False)
        else:
            df.to_csv(os.path.realpath('.') + "/csv/orders.csv", index=False)
        if side == "buy":
            self.store_buy_order(pair, df)
        else:
            self.store_sell_order(pair, df, ls_buy_ids)

    def update_order_status(self):
        ls_ids = self.get_open_orders()
        for orderid in ls_ids:
            df = self._call.get_order_status(orderid)
            for idx in df.index:
                status = df['status'][idx]
                order_id = df['id'][idx]
                created_at = df['created_at'][idx]
                current_time = int(round(time.time() * 1000))

                if 'open' == status or 'init' == status:
                    self._l.log_warn("order " + order_id + " is still open")
                    if current_time - created_at >= 300000:
                        self.m.slack.post_message("order " + order_id + " is open for more than 5 minutes")
                    continue

                side = df['side'][idx]
                fee_amount = df['fee_amount'][idx]
                updated_at = df['updated_at'][idx]

                if 'filled' == status:
                    if side == 'buy':
                        subs_dict = {
                            "fee_amount": fee_amount,
                            "updated_time": updated_at,
                            "status": "'" + status + "'",
                            "order_id": "'" + order_id + "'"
                        }
                        statement = '''
                            update orders
                            set 
                            fee_amount = %(fee_amount)s,
                            updated_time = %(updated_time)s,
                            status = %(status)s,
                            final_price = (select total_price from orders where order_id = %(order_id)s) + %(fee_amount)s
                            where order_id = %(order_id)s
                        ''' % subs_dict
                        self.m.execute_sql(statement.strip(), True)

                    elif side == 'sell':
                        subs_dict = {
                            "fee_amount": fee_amount,
                            "updated_time": updated_at,
                            "status": "'" + status + "'",
                            "order_id": "'" + order_id + "'"
                        }
                        statement = '''
                            update orders
                            set 
                            fee_amount = %(fee_amount)s,
                            updated_time = %(updated_time)s,
                            status = %(status)s,
                            final_price = (select total_price from orders where order_id = %(order_id)s) - %(fee_amount)s
                            where order_id = %(order_id)s
                        ''' % subs_dict
                        self.m.execute_sql(statement.strip(), True)

                        sell_id = -1
                        id_csv = 'N/A'
                        total_sell_price = 0.0
                        total_buy_price = 0.0
                        statement = "select id, inv_rel, final_price from orders where order_id = '" + order_id + "'"
                        results = self.m.execute_sql(statement.strip())
                        for result in results:
                            sell_id = result[0]
                            id_csv = result[1]
                            total_sell_price = float(result[2])

                        statement = "select sum(final_price) from orders where id in (" + id_csv + ")"
                        results = self.m.execute_sql(statement.strip())
                        for result in results:
                            total_buy_price = result[0]
                        profit = ((total_sell_price - total_buy_price) / total_buy_price) * 100

                        statement = "update orders set profit = " + str(profit) + " where order_id = '" + order_id + "'"
                        self.m.execute_sql(statement.strip(), True)
                        statement = "update orders set related = " + str(sell_id) + " where id in (" + id_csv + ")"
                        self.m.execute_sql(statement.strip(), True)
                    self._l.log_info("order " + order_id + " is filled")

    def get_open_orders(self):
        ls_ret = []
        statement = '''
            select order_id
            from orders
            where
            status = 'open'
        '''
        results = self.m.execute_sql(statement.strip())
        for result in results:
            ls_ret.append(result[0])
        return ls_ret