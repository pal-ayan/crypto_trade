import concurrent.futures
from datetime import datetime, timezone


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
        self.markets_df = m.markets_df
        self.call = m.call
        self.slack = m.slack
        self.dict_ticker_df = m.dict_ticker_df

    def get_already_bought(self):
        ret_dict = {}
        statement = "select pair, sum(total_price), ifnull(max(unixtime), 0) from trade where action = 'BUY' and related = -1 group by pair"
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
                p = _perform(self.m, self.l, self.call, self.markets_df)
                is_bought = False
                if key in bought_dict:
                    is_bought = True
                    val = bought_dict[key]
                    total_price = val[0]
                    last_bought = val[1]
                futures.append(executor.submit(p.analyse, key, self.dict_ticker_df[key], is_bought, total_price, last_bought))
            for future in concurrent.futures.as_completed(futures):
                self.l.log_info(future.result())


class _perform:

    def __init__(self, m, l, call, markets_df):
        self.l = l
        self.m = m
        self.call = call
        self.markets_df = markets_df

    def get_balance(self, currency):
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
            where combo.c = %(currency)s
        ''' % {"currency": "'" + currency + "'"}
        results = self.m.execute_sql(statement.strip())
        for result in results:
            return float(result[1]), float(result[2])

    def get_coindcx_name(self, pair):
        df = self.markets_df.loc[self.markets_df['pair'] == pair]
        return df['coindcx_name'].values[0]

    def buy(self, pair, currency, timestamp, status, action, buy_amount, str_time, is_bought, total_buy_price):
        self.m.acquire_lock()
        try:
            available_bal, total_bal = self.get_balance(currency)
            ret = False
            if is_bought:
                if ((total_buy_price / total_bal) * 100) > 10:
                    ret = True
                if buy_amount >= available_bal / 2:
                    ret = True
            else:
                if buy_amount >= (available_bal * 2) / 3:
                    ret = True
            if ret:
                self.l.log_info(pair + ' insufficient balance ' + str_time)
                return

            price = self.call.get_current_price(self.get_coindcx_name(pair))
            units = buy_amount / float(price)
            remaining_bal = available_bal - float(buy_amount)

            substitution_dict = {
                "pair": "'" + pair + "'",
                "price": price,
                "currency": "'" + currency + "'",
                "unixtime": timestamp,
                "timestamp": "'" + str_time + "'",
                "status": "'" + status + "'",
                "action": "'" + action + "'",
                "units": "'" + str(units) + "'",
                "total_price": "'" + str(buy_amount) + "'",
                "remaining_bal": "'" + str(remaining_bal) + "'"
            }

            statement = "update balances set balance = " + str(remaining_bal) + " where currency = %(currency)s" % substitution_dict
            self.m.execute_sql(statement, True)

            statement = "insert into trade (pair, price, units, total_price, currency, unixtime, timestamp, status, action, remaining_bal) " \
                        "values (%(pair)s, %(price)s, %(units)s, %(total_price)s, %(currency)s, %(unixtime)s, %(timestamp)s, %(status)s, %(action)s, %(remaining_bal)s)" \
                        % substitution_dict
            self.m.execute_sql(statement, True)
            self.l.log_info(pair + ' buying at ' + str_time)
        except:
            self.l.log_exception('error while buying -> ' + pair)
        finally:
            self.m.release_lock()

    def sell(self, pair, currency, timestamp, status, action, str_time):
        self.m.acquire_lock()
        try:
            price = self.call.get_current_price(self.get_coindcx_name(pair))
            min_profit_price = 0.99 * float(price)
            statement = "select id, (select count(id) from trade where pair = %(pair)s and action = 'BUY' and related = -1) as total " \
                        "from trade where price < %(min_profit_price)s and pair = %(pair)s  and action = 'BUY' and related = -1" \
                        % {"pair": "'" + pair + "'", "min_profit_price": min_profit_price}

            results = self.m.execute_sql(statement)
            ls_ids_to_sell = []

            if len(results) == 0:
                # this is a scenario where the current sell trigger price is definitley lower than all buy price
                # hence a definitel loss scenario
                # if the position is open for less than 7 days -> HOLD
                statement = "SELECT ifnull(round(julianday('now') - julianday(min(timestamp))) >= 7, 0) from trade where pair = %(pair)s and action = 'BUY' and related = -1" \
                            % {"pair": "'" + pair + "'"}
                results = self.m.execute_sql(statement)
                for result in results:
                    if int(result[0]) == 1:
                        self.l.log_info(pair + ' position has been open for MORE than 7 days')
                    else:
                        self.l.log_info(pair + ' position has been open for LESS than 7 days')
                        return

            for result in results:
                if int(result[1]) == len(results):
                    self.l.log_info(pair + ' all open positions need to be closed')
                    break
                else:
                    ls_ids_to_sell.append(str(result[0]))
            total_buy_price, total_sell_price, total_units, profit_percent = self.calculate_profit(price, pair, ls_ids_to_sell)

            available_bal, total_bal = self.get_balance(currency)
            remaining_bal = available_bal + float(total_sell_price)

            total_change = ((total_sell_price - total_buy_price) / total_bal) * 100

            substitution_dict = {
                "pair": "'" + pair + "'",
                "price": price,
                "currency": "'" + currency + "'",
                "unixtime": timestamp,
                "timestamp": "'" + str_time + "'",
                "status": "'" + status + "'",
                "action": "'" + action + "'",
                "units": "'" + str(total_units) + "'",
                "total_price": "'" + str(total_sell_price) + "'",
                "remaining_bal": "'" + str(remaining_bal) + "'",
                "total_change": "'" + str(total_change) + "'",
                "profit": "'" + str(profit_percent) + "'"
            }

            statement = "insert into trade (pair, price, units, total_price, currency, unixtime, timestamp, status, action, profit, remaining_bal, total_change) values " \
                        "(%(pair)s, %(price)s, %(units)s, %(total_price)s, %(currency)s, %(unixtime)s, %(timestamp)s, %(status)s, %(action)s, %(profit)s, %(remaining_bal)s, %(total_change)s)" \
                        % substitution_dict
            self.m.execute_sql(statement, True)

            statement = "update balances set balance = %(remaining_bal)s where currency = %(currency)s" % substitution_dict
            self.m.execute_sql(statement, True)
            if len(ls_ids_to_sell) > 0:
                substitution_dict = {
                    "pair": "'" + pair + "'",
                    "id_csv": ','.join(ls_ids_to_sell)
                }
                statement = "update trade set related = (select id from trade where pair = %(pair)s and action = 'SELL' order by unixtime desc limit 1)" \
                            " where id in (%(id_csv)s)" % substitution_dict
            else:
                substitution_dict = {
                    "pair": "'" + pair + "'"
                }
                statement = "update trade set related = (select id from trade where pair = %(pair)s and action = 'SELL' order by unixtime desc limit 1)" \
                            " where id in (select id from trade where pair = %(pair)s and action = 'BUY' and related = -1)" % substitution_dict
            self.m.execute_sql(statement, True)
            self.l.log_info(pair + ' sold at ' + str_time)
        except:
            self.l.log_exception('error while buying -> ' + pair)
        finally:
            self.m.release_lock()

    def calculate_profit(self, sold_at, pair, ls_ids_to_sell):
        total_buy_price = 0
        total_units = 0
        if len(ls_ids_to_sell) > 0:
            id_csv = ','.join(ls_ids_to_sell)
            statement = "select total_price, units from trade where id in (%(id_csv)s)" % {"id_csv": id_csv}
        else:
            statement = "select total_price, units from trade where pair = %(pair)s and action = 'BUY' and related = -1" % {"pair": "'" + pair + "'"}

        results = self.m.execute_sql(statement)

        for result in results:
            total_buy_price += float(result[0])
            total_units += float(result[1])
        total_sell_price = total_units * float(sold_at)
        return total_buy_price, total_sell_price, total_units, ((total_sell_price - total_buy_price) / total_buy_price) * 100

    def analyse(self, pair, df, is_bought, total_buy_price=0, last_bought=0):
        try:
            min_buy_amount = self.m.dict_min_notional[pair]
            self.l.log_info(pair + ' already bought -> ' + str(is_bought))
            last_row = df
            for idx in last_row.index:
                rsi_below_bottom_threshold = (last_row['RSI'][idx] < 30) or (((abs(30 - last_row['RSI'][idx]) / 30) * 100) < 5)
                s_rsi_below_threshold_and_blue_below_red = (last_row['K'][idx] < 20) or ((abs(last_row['K'][idx] - 20) / 20) * 100) < 5
                macd_green_below_red = last_row['MACD'][idx] < last_row['MACD_Signal'][idx] and abs(((last_row['MACD_Signal'][idx] - last_row['MACD'][idx]) / last_row['MACD'][idx]) * 100) > 5
                str_time = datetime.fromtimestamp(int(last_row['time'][idx] / 1000)).astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

                if rsi_below_bottom_threshold and s_rsi_below_threshold_and_blue_below_red and macd_green_below_red:
                    if last_bought != 0 and ((last_row['time'][idx] - last_bought) <= 900000):
                        self.l.log_info(pair + ' has been bought in the last 15 mins')
                        continue

                    self.buy(pair, pair.split("_")[1], last_row['time'][idx], "DONE", "BUY", min_buy_amount, str_time, is_bought, total_buy_price)
                    continue

                if is_bought:
                    rsi_above_top_threshold = (last_row['RSI'][idx] > 70) and ((((last_row['RSI'][idx] - 70) / 70) * 100) > 5)
                    s_rsi_above_top_threshold = (last_row['K'][idx] > 80) and (((last_row['K'][idx] - 80) / 80) * 100) > 5
                    macd_green_above_red = last_row['MACD'][idx] > last_row['MACD_Signal'][idx] and abs(((last_row['MACD_Signal'][idx] - last_row['MACD'][idx]) / last_row['MACD'][idx]) * 100) > 5

                    if rsi_above_top_threshold and s_rsi_above_top_threshold and macd_green_above_red:
                        self.sell(pair, pair.split("_")[1], last_row['time'][idx], 'DONE', 'SELL', str_time)

            return "completed " + pair

        except:
            self.l.log_exception('Error Occurred ' + pair)
