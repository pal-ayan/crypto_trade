import glob
import os.path
import tarfile
from datetime import datetime
from os import listdir
from os.path import isfile, join

import master
from master import master as m


def make_tarfile():
    output_filename = os.path.realpath('.') + "/logs_archive/" + str_time + ".tar.bz2"
    onlyfiles = [f for f in listdir(os.path.realpath('.') + "/logs") if isfile(join(os.path.realpath('.') + "/logs", f))]
    tar = tarfile.open(output_filename, "w:bz2")
    for name in onlyfiles:
        tar.add(os.path.realpath('.') + "/logs/" + name, arcname=name)
    tar.close()

    files = glob.glob(os.path.realpath('.') + "/logs/*")
    for f in files:
        try:
            os.remove(f)
        except OSError:
            continue


def update_ledger(m):
    daily_change_dict = {}
    weekly_change_dict = {}
    monthly_change_dict = {}
    available_dict, total_dict = m.get_balance()
    current_day = master.get_date_as_ms_string()
    prev_day = master.get_date_as_ms_string(1)
    prev_week = master.get_date_as_ms_string(7)
    prev_month = master.get_date_as_ms_string(30)
    substitution_dict = {
        "prev_day": prev_day,
        "prev_week": prev_week,
        "prev_month": prev_month
    }
    statement = '''
        select currency, 1, balance from ledger where unixtime = %(prev_day)s
        union
        select currency, 7, balance from ledger where unixtime = %(prev_week)s
        union
        select currency, 30, balance from ledger where unixtime = %(prev_month)s
    ''' % substitution_dict
    results = m.execute_sql(statement)
    for result in results:
        currency = result[0]
        delta = int(result[1])
        bal = float(result[2])
        if delta == 1:
            daily_change_dict[currency] = bal
        elif delta == 7:
            weekly_change_dict[currency] = bal
        elif delta == 30:
            monthly_change_dict[currency] = bal

    for key in total_dict:
        daily_bal = daily_change_dict.get(key, 0)
        weekly_bal = weekly_change_dict.get(key, 0)
        monthly_bal = monthly_change_dict.get(key, 0)
        current_bal = total_dict[key]
        substitution_dict = {
            "currency": "'" + key + "'",
            "unixtime": current_day,
            "balance": current_bal,
            "daily_change_pct": 0 if daily_bal == 0 else (((current_bal - daily_bal) / daily_bal) * 100),
            "weekly_change_pct": 0 if weekly_bal == 0 else (((current_bal - weekly_bal) / weekly_bal) * 100),
            "monthly_change_pct": 0 if monthly_bal == 0 else (((current_bal - monthly_bal) / monthly_bal) * 100)

        }
        statement = "insert into ledger (currency, unixtime, balance, daily_change_pct, weekly_change_pct, monthly_change_pct) " \
                    "values (%(currency)s, %(unixtime)s, %(balance)s, %(daily_change_pct)s, %(weekly_change_pct)s, %(monthly_change_pct)s)" % substitution_dict
        m.execute_sql(statement, True)


if __name__ == "__main__":
    m = m(None)
    str_time = datetime.now().strftime("%Y%m%d")
    make_tarfile()
    update_ledger(m)
