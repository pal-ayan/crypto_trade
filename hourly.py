from datetime import datetime

from eligible_pairs import capture
from log import log
from master import master

l = log()
l.log_info('***********START***********')

start_time = datetime.now()

m = master(l)
m.init_markets_df()

ep = capture(m)
ep.do()

m.join_threads()

time_diff = datetime.now() - start_time
l.log_info('***********END***********  ' + 'script completion took -> %s' % time_diff)
