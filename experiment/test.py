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

print
print 'num actions, num checks:', len(actions), len(checks)

print
print 'buy, sell, control proportions', np.mean(actions['condition'] == 'buy'), np.mean(actions['condition'] == 'sell'), np.mean(actions['condition'] == 'control')

print
states = {'up':1, 'down':1, 'stay':1}
total = 0
for s1 in states:
    for s2 in states:
        perc = np.mean(actions['buy_state'] +  actions['sell_state'] == s1 + s2)
        total += perc
        print 'state:', s1, s2, perc
assert abs(total - 1) < 1e-8

actions['action_id'] = actions['coin_name'] + map(lambda x: '-' + str(x), actions['intervention_num'])

print
print 'Checking unique names...'
if len(set(actions['action_id'])) == len(actions):
    print 'ok'
else:
    print 'fail'

print 'Checking coin ids...'
len(set(actions['coin_name'])) == len(set(actions['coin_id']))
print 'ok'

checks['action_id'] = checks['coin_name'] + map(lambda x: '-' + str(x), checks['intervention_num'])

print 'Checking unique names...'
if len(set(checks['action_id'] + map(str, checks['monitor_num']))) == len(checks):
    print 'ok'
else:
    print 'fail'

print 'Checking coin ids...'
if len(set(checks['coin_name'])) == len(set(checks['coin_id'])):
    print 'ok'
else:
    print 'fail'

print 'Checking times...'
    
actions['start_time'] = actions['start_time'].apply(pd.to_datetime, errors='raise')
actions['end_time'] = actions['end_time'].apply(pd.to_datetime, errors='raise')
checks['time'] = checks['time'].apply(pd.to_datetime, errors='raise')

if sum(actions['wait'] <= 60) == 0:
    print 'ok'
else:
    print 'fail'

if sum(actions['start_time'] >= actions['end_time']) == 0:
    print 'ok'
else:
    print 'fail'

wrong = 0
for coin in set(actions['coin_name']):
    sub = actions[actions['coin_name'] == coin]
    nums = sorted(sub['intervention_num'])
    last_time = None
    for i,num in enumerate(nums):
        if i == len(nums) - 1:
            break
        a = sub[sub['intervention_num'] == num]
        b = sub[sub['intervention_num'] == nums[i+1]]
        if list(a['start_time'] + timedelta(minutes = float(a['wait'])) >= b['start_time'])[0]:
            print 'warn:', a['end_time'].iloc[0] + timedelta(minutes = float(a['wait'])), 'vs', b['start_time'].iloc[0]
            wrong += 1

if wrong > 0:
    print wrong, 'wrong times of', len(actions)
else:
    'ok'

print 'Checking monitors...'
    
minutes = dict(zip(range(4), map(lambda x: timedelta(minutes = x), [0,15,30,60])))

def state(price_1, price_2):
    price_1, price_2 = (float(price_1), float(price_2))
    if price_1 < price_2:
        return 'up'
    elif price_1 > price_2:
        return 'down'
    else:
        return 'stay'
    
def check_equal_to_sum(val, val1, val2):
    if np.isnan(val):
        assert np.isnan(val1) and np.isnan(val2)
    else:
        assert abs(val - (val1 + val2)) < 1e-6

different_prices = 0
late_monitors = 0
failed_buy_interventions = 0
failed_sell_interventions = 0
for a in set(actions['action_id']):
    sub_a = actions[actions['action_id'] == a]
    sub_c = checks[checks['action_id'] == a]
    for i in range(len(sub_c)):
        assert sub_c.iloc[i]['monitor_num'] == i
        if i == 0:
            time = sub_c.iloc[i]['time']
            assert sub_c.iloc[i]['buy_state'] == state(sub_c.iloc[i]['last_trade_price'], sub_c.iloc[i]['lowest_sell_price'])
            assert sub_c.iloc[i]['sell_state'] == state(sub_c.iloc[i]['last_trade_price'], sub_c.iloc[i]['highest_buy_price'])
            check_equal_to_sum(sub_c.iloc[i]['volume_15'], sub_c.iloc[i]['sell_volume_15'], sub_c.iloc[i]['buy_volume_15'])
            check_equal_to_sum(sub_c.iloc[i]['volume_30'], sub_c.iloc[i]['sell_volume_30'], sub_c.iloc[i]['buy_volume_30'])
            check_equal_to_sum(sub_c.iloc[i]['volume_60'], sub_c.iloc[i]['sell_volume_60'], sub_c.iloc[i]['buy_volume_60'])
            check_equal_to_sum(sub_c.iloc[i]['total_15'], sub_c.iloc[i]['sell_total_15'], sub_c.iloc[i]['buy_total_15'])
            check_equal_to_sum(sub_c.iloc[i]['total_30'], sub_c.iloc[i]['sell_total_30'], sub_c.iloc[i]['buy_total_30'])
            check_equal_to_sum(sub_c.iloc[i]['total_60'], sub_c.iloc[i]['sell_total_60'], sub_c.iloc[i]['buy_total_60'])
            assert not np.isnan(sub_c.iloc[i]['volume_15'])
            assert not np.isnan(sub_c.iloc[i]['volume_30'])
            assert not np.isnan(sub_c.iloc[i]['volume_60'])
            assert not np.isnan(sub_c.iloc[i]['total_15'])
            assert not np.isnan(sub_c.iloc[i]['total_30'])
            assert not np.isnan(sub_c.iloc[i]['total_60'])
            assert np.isnan(sub_c.iloc[i]['volume'])
            assert np.isnan(sub_c.iloc[i]['total'])
            assert np.isnan(sub_c.iloc[i]['buy_volume'])
            assert np.isnan(sub_c.iloc[i]['sell_volume'])
            assert np.isnan(sub_c.iloc[i]['buy_total'])
            assert np.isnan(sub_c.iloc[i]['sell_total'])
            if not np.isnan(sub_c.iloc[i]['volume_15']):
                assert sub_c.iloc[i]['volume_30'] >= sub_c.iloc[i]['volume_15']
                assert sub_c.iloc[i]['total_30'] >= sub_c.iloc[i]['total_15']
            if not np.isnan(sub_c.iloc[i]['volume_30']):
                assert sub_c.iloc[i]['volume_60'] >= sub_c.iloc[i]['volume_30']
                assert sub_c.iloc[i]['total_60'] >= sub_c.iloc[i]['total_30']
            if sub_c.iloc[i]['condition'] == 'buy':
                if sub_c.iloc[i]['lowest_sell_price'] != sub_a['this_trade_price'].iloc[0]:
                    different_prices += 1
                if state(sub_c.iloc[i]['last_trade_price'], sub_a['this_trade_price']) != sub_c.iloc[i]['buy_state']:
                    failed_buy_interventions += 1    
            if sub_c.iloc[i]['condition'] == 'sell':                
                if sub_c.iloc[i]['highest_buy_price'] != sub_a['this_trade_price'].iloc[0]:
                    different_prices += 1
                if state(sub_c.iloc[i]['last_trade_price'], sub_a['this_trade_price']) != sub_c.iloc[i]['sell_state']:
                    failed_sell_interventions += 1
        else:
            assert sub_c.iloc[i]['time'] > time + minutes[i]
            check_equal_to_sum(sub_c.iloc[i]['volume'], sub_c.iloc[i]['sell_volume'], sub_c.iloc[i]['buy_volume'])
            check_equal_to_sum(sub_c.iloc[i]['total'], sub_c.iloc[i]['sell_total'], sub_c.iloc[i]['buy_total'])
            assert np.isnan(sub_c.iloc[i]['volume_15'])
            assert np.isnan(sub_c.iloc[i]['volume_30'])
            assert np.isnan(sub_c.iloc[i]['volume_60'])
            assert np.isnan(sub_c.iloc[i]['total_15'])
            assert np.isnan(sub_c.iloc[i]['total_30'])
            assert np.isnan(sub_c.iloc[i]['total_60'])
            assert np.isnan(sub_c.iloc[i]['buy_volume_15'])
            assert np.isnan(sub_c.iloc[i]['sell_volume_15'])
            assert np.isnan(sub_c.iloc[i]['buy_total_15'])
            assert np.isnan(sub_c.iloc[i]['sell_total_15'])
            assert np.isnan(sub_c.iloc[i]['buy_volume_30'])
            assert np.isnan(sub_c.iloc[i]['sell_volume_30'])
            assert np.isnan(sub_c.iloc[i]['buy_total_30'])
            assert np.isnan(sub_c.iloc[i]['sell_total_30'])
            assert np.isnan(sub_c.iloc[i]['buy_volume_60'])
            assert np.isnan(sub_c.iloc[i]['sell_volume_60'])
            assert np.isnan(sub_c.iloc[i]['buy_total_60'])
            assert np.isnan(sub_c.iloc[i]['sell_total_60'])
            if sub_c.iloc[i]['time'] > time + minutes[i] + timedelta(minutes = 5):
                late_monitors += 1
            if not np.isnan(sub_c.iloc[i]['volume']) and i + 1 < len(sub_c):
                if not np.isnan(sub_c.iloc[i+1]['volume']):
                    assert sub_c.iloc[i]['volume'] <= sub_c.iloc[i+1]['volume']


print 'failed buy interventions', failed_buy_interventions, 'out of', sum(actions['condition'] == 'buy')
print 'failed sell interventions', failed_sell_interventions, 'out of', sum(actions['condition'] == 'sell')
print 'unexpected prices', different_prices
print 'late monitors', late_monitors
