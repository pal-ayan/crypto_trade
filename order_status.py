from datetime import datetime

from log import log
from master import master
from order import order

l = log()
l.log_info('***********START***********')

start_time = datetime.now()
m = master(l)
slack = m.slack

try:
    o = order(m)
    o.update_order_status()
except:
    l.log_exception("error occured with order status")
    slack.post_message("error occured with updating order status")

m.join_threads()

time_diff = datetime.now() - start_time
l.log_info('***********END***********  ' + 'script completion took -> %s' % time_diff)
