import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys

action_file = sys.argv[1]
#action_file = '../processed/epsilon-5en7-complete-all-volumes-2015-03-10-interventions.csv'
checks_file = sys.argv[2]
#checks_file = '../processed/epsilon-5en7-complete-all-volumes-2015-03-10-checks.csv'

actions = pd.io.parsers.read_csv(action_file)
checks = pd.io.parsers.read_csv(checks_file)

actions['action_id'] = actions['coin_name'] + map(lambda x: '-' + str(x), actions['intervention_num'])
checks['action_id'] = checks['coin_name'] + map(lambda x: '-' + str(x), checks['intervention_num'])
actions.index = actions['action_id']
checks.index = checks['action_id']
    
actions['start_time'] = actions['start_time'].apply(pd.to_datetime, errors='raise')
actions['end_time'] = actions['end_time'].apply(pd.to_datetime, errors='raise')
checks['last_trade_time'] = checks['last_trade_time'].apply(pd.to_datetime, errors='raise')

compare = checks[checks['monitor_num'] == 0]
sub = checks[checks['monitor_num'] != 0]

print 'Checking times...'

bad_times = 0
for i in range(len(sub)):
    if i % 10000 == 0:
        print '.'
    this_id = sub.iloc[i]['action_id']
    if compare.loc[this_id]['last_trade_time'] > sub.iloc[i]['last_trade_time']:
        bad_times += 1
        print 'warning', this_id, compare.loc[this_id]['last_trade_time'], sub.iloc[i]['last_trade_time']
    elif compare.loc[this_id]['last_trade_time'] == sub.iloc[i]['last_trade_time']:
        if compare.loc[this_id]['last_trade_id'] == sub.iloc[i]['last_trade_id']:
            bad_times += 1
            print 'error', this_id, actions.loc[this_id]['condition'], compare.loc[this_id]['last_trade_time'], sub.iloc[i]['last_trade_time']

print 'bad times (counting at monitor)', bad_times / float(len(sub))

bad_times = 0
for i in range(len(sub)):
    if i % 10000 == 0:
        print '.'
    this_id = sub.iloc[i]['action_id']
    if actions.loc[this_id]['start_time'] > sub.iloc[i]['last_trade_time']:
        bad_times += 1
        print 'warning', this_id, actions.loc[this_id]['condition'], actions.loc[this_id]['start_time'], sub.iloc[i]['last_trade_time']

print 'bad times (counting at start)', bad_times / float(len(sub))

bad_times = 0
for i in range(len(sub)):
    if i % 10000 == 0:
        print '.'        
    this_id = sub.iloc[i]['action_id']
    if actions.loc[this_id]['end_time'] > sub.iloc[i]['last_trade_time']:
        bad_times += 1
        print 'warning', this_id, actions.loc[this_id]['condition'], actions.loc[this_id]['end_time'], sub.iloc[i]['last_trade_time']

print 'bad times (counting at end)', bad_times / float(len(sub))
    
