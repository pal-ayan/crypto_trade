import concurrent.futures

import numpy as np

from order import order


def rsi_constantly_below_bottom_threshold_since_bought(df, current_index, last_buy_idx):
    if last_buy_idx is None:
        return False
    sub_df = df.loc[last_buy_idx:current_index]
    for idx in sub_df.index:
        rsi_below_bottom_threshold = (df['RSI'][idx] < 30) or (0 < (((df['RSI'][idx] - 30) / 30) * 100) < 5)
        if rsi_below_bottom_threshold:
            continue
        else:
            return False
    return True


class trade:

    def __init__(self, m):
        self.l = m.l
        self.m = m
        self.markets_df = m.t_markets_df
        self.call = m.call
        self.slack = m.slack
        self.dict_ticker_df = m.dict_ticker_df

    def get_already_bought(self):
        ret_dict = {}
        statement = "select pair, sum(final_price), ifnull(max(created_time), 0) from orders where side = 'buy' and status = 'filled' and related = -1 group by pair"
        results = self.m.execute_sql(statement)
        for result in results:
            pair = result[0]
            total_price = float(result[1])
            last_bought = int(result[2])
            ret_dict[pair] = [total_price, last_bought]
        return ret_dict

    def do(self):
        bought_dict = self.get_already_bought()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for key in self.dict_ticker_df:
                p = _perform(self.m, self.l, self.call, self.markets_df, self.slack)
                is_bought = False
                total_price = 0.0
                last_bought = 0
                if key in bought_dict:
                    is_bought = True
                    val = bought_dict[key]
                    total_price = val[0]
                    last_bought = val[1]
                futures.append(executor.submit(p.analyse, key, self.dict_ticker_df[key], is_bought, total_price, last_bought))
            for future in concurrent.futures.as_completed(futures):
                self.l.log_info(future.result())


class _perform:

    def __init__(self, m, l, call, markets_df, slack):
        self.l = l
        self.m = m
        self.call = call
        self.markets_df = markets_df
        self.order = order(m)
        self.slack = slack

    def get_balance(self, currency):
        open_bal = 0.0
        statement = '''
            select sum(t.final_price) as b
                    FROM
                    orders t
                    where 
                    t.side = 'buy'
                    and t.related = -1
                    and t.status = 'filled'
                    and currency = %(currency)s
        ''' % {"currency": "'" + currency + "'"}
        results = self.m.execute_sql(statement.strip())
        for result in results:
            open_bal = result[0]
        available_bal, locked_bal = self.call.get_user_balance(currency)
        return available_bal, locked_bal + open_bal

    def get_coindcx_name(self, pair):
        df = self.markets_df.loc[self.markets_df['pair'] == pair]
        return df['coindcx_name'].values[0]

    def get_units(self, buy_amount, price, pair):
        step = self.m.dict_step[pair]
        units = buy_amount / float(price)
        a = np.array([float(units) / step])
        val = np.ceil(a)[0]
        return round(val) * step

    def get_units_bought(self, ls_ids_to_sell):
        ids = ",".join(ls_ids_to_sell)
        total_units = 0.0
        statement = "select units from orders where id in (%(ids)s)" % {"ids": ids}
        results = self.m.execute_sql(statement)
        for result in results:
            total_units += float(result[0])
        return total_units

    def buy(self, pair, is_bought, total_buy_price):
        self.m.acquire_lock()
        try:
            currency = pair.split("_")[1]
            buy_amount = self.m.dict_min_notional[pair]
            available_bal, total_bal = self.get_balance(currency)
            ret = False
            if is_bought:
                if ((total_buy_price / total_bal) * 100) > 10:
                    self.l.log_info(pair + ' overbought')
                    ret = True
                if buy_amount >= available_bal / 2:
                    self.l.log_info(pair + ' insufficient balance')
                    ret = True
            else:
                if buy_amount >= (available_bal * 2) / 3:
                    self.l.log_info(pair + ' insufficient balance for new position')
                    ret = True
            if ret:
                return

            market = self.get_coindcx_name(pair)
            price = self.call.get_current_price(market)
            units = self.get_units(buy_amount, price, pair)

            self.order.place_order('buy', pair, market, units, price)
        except:
            self.l.log_exception('error while buying -> ' + pair)
            self.slack.post_message('error while buying -> ' + pair)
        finally:
            self.m.release_lock()

    def sell(self, pair):
        self.m.acquire_lock()
        try:
            market = self.get_coindcx_name(pair)
            price = self.call.get_current_price(self.get_coindcx_name(pair))
            min_profit_price = 0.99 * float(price)
            statement = "select id, (select count(order_id) from orders where pair = %(pair)s and side = 'buy' and status = 'filled' and related = -1) as total " \
                        "from orders where price < %(min_profit_price)s and pair = %(pair)s  and side = 'buy' and status = 'filled' and related = -1" \
                        % {"pair": "'" + pair + "'", "min_profit_price": min_profit_price}

            results = self.m.execute_sql(statement)
            ls_ids_to_sell = []

            if len(results) == 0:
                # this is a scenario where the current sell trigger price is definitley lower than all buy price
                # hence a definite loss scenario
                # if the position is open for less than 14 days -> HOLD
                statement = "SELECT ifnull(round(julianday('now') - julianday(min(created_time))) >= 14, 0) from trade where pair = %(pair)s and side = 'BUY' and status = 'filled' and related = -1" \
                            % {"pair": "'" + pair + "'"}
                results = self.m.execute_sql(statement)
                for result in results:
                    if int(result[0]) == 1:
                        self.l.log_info(pair + ' position has been open for MORE than 14 days, sell everything')
                    else:
                        self.l.log_info(pair + ' position has been open for LESS than 14 days')
                        return

            for result in results:
                ls_ids_to_sell.append(str(result[0]))

            total_units_bought = self.get_units_bought(ls_ids_to_sell)
            self.order.place_order('sell', pair, market, total_units_bought, price, ls_ids_to_sell)

        except:
            self.l.log_exception('error while selling -> ' + pair)
            self.slack.post_message('error while selling -> ' + pair)
        finally:
            self.m.release_lock()

    def analyse(self, pair, last_row, is_bought, total_buy_price=0.0, last_bought=0):
        try:
            self.l.log_info(pair + ' already bought -> ' + str(is_bought))
            for idx in last_row.index:

                rsi_below_bottom_threshold = (last_row['RSI'][idx] < 30) or (((abs(30 - last_row['RSI'][idx]) / 30) * 100) < 5)
                s_rsi_below_threshold_and_blue_below_red = (last_row['K'][idx] < 20) or ((abs(last_row['K'][idx] - 20) / 20) * 100) < 5
                macd_green_below_red = last_row['MACD'][idx] < last_row['MACD_Signal'][idx] and abs(((last_row['MACD_Signal'][idx] - last_row['MACD'][idx]) / last_row['MACD'][idx]) * 100) > 5

                if rsi_below_bottom_threshold and s_rsi_below_threshold_and_blue_below_red and macd_green_below_red:
                    if last_bought != 0 and ((last_row['time'][idx] - last_bought) <= 900000):
                        self.l.log_info(pair + ' has been bought in the last 15 mins')
                        continue

                    self.buy(pair, is_bought, total_buy_price)
                    continue

                if is_bought:
                    rsi_above_top_threshold = (last_row['RSI'][idx] > 70) and ((((last_row['RSI'][idx] - 70) / 70) * 100) > 5)
                    s_rsi_above_top_threshold = (last_row['K'][idx] > 80) and (((last_row['K'][idx] - 80) / 80) * 100) > 5
                    macd_green_above_red = last_row['MACD'][idx] > last_row['MACD_Signal'][idx] and abs(((last_row['MACD_Signal'][idx] - last_row['MACD'][idx]) / last_row['MACD'][idx]) * 100) > 5

                    if rsi_above_top_threshold and s_rsi_above_top_threshold and macd_green_above_red:
                        self.sell(pair)

            return "completed " + pair

        except:
            self.l.log_exception('Error Occurred ' + pair)
            self.slack.post_message('Error occured while analysing pair - '+ pair)