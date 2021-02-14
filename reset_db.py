from master import master


class reset:

    def __init__(self, m):
        self.u = m
        self.l = m.l
        self.slack = m.slack
        self.call = m.call
        self.l.log_info('starting reset')

    def do(self):
        statement = "delete from balances"
        self.u.execute_sql(statement, True)
        statement = "delete from trade"
        self.u.execute_sql(statement, True)
        statement = "insert into balances (currency, balance) values (%(c)s, %(b)s)" % {"c": "'" + 'ETH' + "'", "b": "'" + str(0.1) + "'"}
        self.u.execute_sql(statement, True)
        statement = "insert into balances (currency, balance) values (%(c)s, %(b)s)" % {"c": "'" + 'BTC' + "'", "b": "'" + str(0.004) + "'"}
        self.u.execute_sql(statement, True)
        statement = "insert into balances (currency, balance) values (%(c)s, %(b)s)" % {"c": "'" + 'USDT' + "'", "b": "'" + str(136) + "'"}
        self.u.execute_sql(statement, True)


if __name__ == "__main__":
    m = master(None)
    r = reset(m)
    r.do()
