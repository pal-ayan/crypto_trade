from datetime import datetime

from eligible_pairs import capture
from log import log
from master import master
from trade import trade

l = log()
l.log_info('***********START***********')

start_time = datetime.now()

m = master(l)
m.init_markets_df()

ep = capture(m)
ep.do()

t = trade(m)
t.do()

time_diff = datetime.now() - start_time
l.log_info('processing completion took -> %s' % time_diff)

m.join_threads()

time_diff = datetime.now() - start_time
l.log_info('***********END***********  ' + 'script completion took -> %s' % time_diff)