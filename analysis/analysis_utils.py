
import os

import matplotlib.pyplot as plt
import seaborn as sns

import numpy as np
import pandas as pd

def read_data(held_out):

    if held_out:
        group = 'held_out'
        group_name = 'confirmatory'
    else:
        group = 'kept'
        group_name = 'exploratory'

    checks = pd.read_csv(group + '_checks.csv')
    interventions = pd.read_csv(group + '_interventions.csv')
    
    checks = checks.set_index(checks['ids'])
    interventions = interventions.set_index(interventions['ids'])

    print(len(interventions), 'total', group_name,'trials')
    print(len(checks), 'total', group_name, 'observations')
    print(len(set(checks['coin_name'])), 'coins')
    print('start:', min(checks['time']))
    print('end:', max(checks['time']))    
    print('Coins traded:\n', ', '.join(sorted(set(checks['coin_name']))))
    
    return checks, interventions


def expand_columns(checks, interventions):
    
    checks = add_time_segments(checks)

    for trade_type in ['Buy','Sell']:
        checks = add_last_type(checks, trade_type)

    checks = add_last_nonnull(checks)

    checks['spread'] = checks['lowest_sell_price'] - checks['highest_buy_price']

    for side in ['buy','sell']:
        checks = add_last_total(checks, side)
        checks = add_recent_trade(checks, side)

    checks = add_last_percent(checks)
        
    checks = add_recent_nonnull(checks)        

    # mean_features = ['recent_last_trade_buy',
    #                  'last percent buy BTC volume',
    #                  'trivial']
    # log_features = ['total_60',
    #                 'last_trade_price',
    #                 'highest_buy_total',
    #                 'lowest_sell_total',
    #                 'spread']
    
    mean_features = []
    log_features = ['total_60']
    
    for feat in mean_features:
        checks = get_descriptive_column(checks, feat, False)

    for feat in log_features:
        checks = get_descriptive_column(checks, feat, True)
        
    init_feats = ['last_trade_buy','last_trade_sell',
                  'last percent buy BTC volume',
                  'last percent sell BTC volume',
                  'weekday','total_60','spread','last_trade_price']


    for feat in init_feats:
        checks = add_initial_columns(checks,feat)

    checks = add_intervention_info(checks,interventions) 
    
    return checks

def add_time_segments(checks):
    """
    >>> df = pd.DataFrame({'time':['2015-04-12 22:42:46','2015-10-19 05:09:09','2015-10-19 09:09:09']})    
    >>> df.index = ['a']*3
    >>> mdf = add_time_segments(df)
    >>> list(mdf['day'])
    ['2015-04-12', '2015-10-19', '2015-10-19']
    >>> list(mdf['month'])
    ['2015-04', '2015-10', '2015-10']
    >>> list(mdf['hour'])
    [22, 5, 9]
    >>> list(mdf['minute'])
    [22.699999999999999, 5.1500000000000004, 9.1500000000000004]
    >>> list(mdf['day_of_week'])
    [6, 0, 0]
    >>> list(mdf['weekday'])
    [False, True, True]
    >>> list(mdf['humantimes'])
    [False, False, True]
    """
    
    checks['day'] = checks['time'].str[0:10]
    checks['month'] = checks['time'].str[0:7]
    checks['hour'] = np.array(checks['time'].str[11:13],dtype=int)
    checks['minute'] = np.array(checks['time'].str[11:13],dtype=int) + np.array(checks['time'].str[14:16],dtype=float) / 60

    checks['day_of_week'] = checks['time'].apply(pd.to_datetime, errors='raise').map(lambda x: x.weekday())
    checks['weekday'] = checks['day_of_week'] < 5
    checks['humantimes'] = (checks['hour'] >= 9) & (checks['hour'] <= 16)
    
    return checks

def add_intervention_info(checks,interventions):
    """
    >>> df = pd.DataFrame({
    ... 'monitor_num':[0,1,2,3,0,1,2,3,0]})
    >>> df.index = ['a','a','a','a','b','b','b','b','c']
    >>> df2 = pd.DataFrame({
    ... 'this_trade_total':[-1,-2,np.nan]})
    >>> df2.index = ['a','b','c']
    >>> add_intervention_info(df, df2)
       monitor_num  this_trade_total
    a            0              -1.0
    a            1              -1.0
    a            2              -1.0
    a            3              -1.0
    b            0              -2.0
    b            1              -2.0
    b            2              -2.0
    b            3              -2.0
    c            0              -2.0
    """
    
    checks['this_trade_total'] = interventions.loc[checks.index, 'this_trade_total']

    # fill in controls
    checks['this_trade_total'] = checks['this_trade_total'].ffill()
        
    return checks

def add_initial_columns(checks,feat):
    """
    >>> df = pd.DataFrame({
    ... 'buy_total_60':[120] + [np.nan]*3 + [60] + [np.nan]*3,
    ... 'monitor_num':[0,1,2,3,0,1,2,3]})
    >>> df.index = ['a']*8
    >>> add_initial_columns(df,'buy_total_60')
       buy_total_60  monitor_num  initial_buy_total_60
    a         120.0            0                 120.0
    a           NaN            1                 120.0
    a           NaN            2                 120.0
    a           NaN            3                 120.0
    a          60.0            0                  60.0
    a           NaN            1                  60.0
    a           NaN            2                  60.0
    a           NaN            3                  60.0
    """
    
    sub = checks.loc[checks['monitor_num'] == 0,feat]

    assert sub.isnull().sum().sum() == 0
    
    init_feat = 'initial_' + feat
    checks[init_feat] = np.nan
    checks.loc[checks['monitor_num'] == 0,init_feat] = sub
    checks[init_feat] = checks[init_feat].ffill()
    
    return checks


def add_last_type(checks, trade_type):
    """
    >>> df = pd.DataFrame({'last_trade_type':['Buy','Sell',np.nan]})
    >>> mdf = add_last_type(df, 'Buy')
    >>> list(mdf['last_trade_buy'])
    [1.0, 0.0, nan]
    >>> mdf = add_last_type(df, 'Sell')
    >>> list(mdf['last_trade_sell'])
    [0.0, 1.0, nan]
    """
    
    z = np.zeros(len(checks))
    x = np.array(checks['last_trade_type'] == trade_type)
    z[x] = 1.0
    y = np.array(checks['last_trade_type'].isnull())
    z[y] = np.nan
    checks['last_trade_' + trade_type.lower()] = z
    
    return checks

def add_last_nonnull(checks):
    """
    >>> df = pd.DataFrame({'last_trade_type':['Buy','Sell',np.nan]})
    >>> mdf = add_last_nonnull(df)
    >>> list(mdf['last_trade_nonnull'])
    [1.0, 1.0, 0.0]
    """
    
    z = np.zeros(len(checks))
    y = np.array(checks['last_trade_type'].notnull())
    z[y] = 1.0
    checks['last_trade_nonnull'] = z
    
    return checks

def add_last_total(checks, side):
    """
    >>> df = pd.DataFrame({
    ... 'buy_total_60':[120,np.nan,np.nan,np.nan],
    ... 'sell_total_60':[60,np.nan,np.nan,np.nan],
    ... 'buy_total':[np.nan,120,180,180],
    ... 'sell_total':[np.nan,0,0,60],
    ... 'monitor_num':[0,1,2,3]})
    >>> df.index = ['a','a','a','a']
    >>> list(add_last_total(df, 'buy')['last_buy_total'])
    [2.0, 8.0, 4.0, 0.0]
    >>> list(add_last_total(df, 'sell')['last_sell_total'])
    [1.0, 0.0, 0.0, 2.0]
    """
    
    checks.loc[checks['monitor_num'] == 0, 'last_' + side + '_total'] = checks.loc[checks['monitor_num'] == 0, side + '_total_60'] / 60.0
    checks.loc[checks['monitor_num'] == 1, 'last_' + side + '_total'] = checks.loc[checks['monitor_num'] == 1, side + '_total'] / 15.0
    for i in [2,3]:
        recent = checks.loc[checks['monitor_num'] == i, side + '_total']
        base = checks.loc[checks['monitor_num'] == i - 1, side + '_total']
        checks.loc[checks['monitor_num'] == i, 'last_' + side + '_total'] = recent - base.loc[recent.index]
        if i == 2:
            checks.loc[checks['monitor_num'] == i, 'last_' + side + '_total'] /= 15.0
        if i == 3:
            checks.loc[checks['monitor_num'] == i, 'last_' + side + '_total'] /= 30.0

    return checks

def add_last_percent(checks):
    """
    >>> df = pd.DataFrame({
    ... 'buy_total_60':[120,np.nan,np.nan,np.nan],
    ... 'sell_total_60':[60,np.nan,np.nan,np.nan],
    ... 'buy_total':[np.nan,120,180,180],
    ... 'sell_total':[np.nan,0,0,60],
    ... 'monitor_num':[0,1,2,3]})
    >>> df.index = ['a','a','a','a']
    >>> df = add_last_total(df, 'buy')
    >>> df = add_last_total(df, 'sell')
    >>> mdf = add_last_percent(df)
    >>> list(mdf['last percent buy BTC volume'])
    [0.66666666666666663, 1.0, 1.0, 0.0]
    >>> list(mdf['last percent sell BTC volume'])
    [0.33333333333333331, 0.0, 0.0, 1.0]
    """
    
    norm = (checks['last_buy_total'] + checks['last_sell_total']).astype(float)
    for side in ['buy','sell']:
        
        checks['last percent ' + side + ' BTC volume'] = checks['last_' + side + '_total'] / norm
    
    return checks
    
def add_recent_trade(checks, side):
    """
    >>> df = pd.DataFrame({'last_trade_type':['Buy',np.nan,'Buy','Buy', 'Sell','Sell','Buy','Sell'],
    ... 'last_trade_id':[0,np.nan,1,1, 2,2,3,4],
    ... 'monitor_num':[0,1,2,3, 0,1,2,3]})
    >>> df.index = ['a','a','a','a', 'b','b','b','b']
    >>> df = add_last_type(df, 'Buy')
    >>> df = add_last_type(df, 'Sell')
    >>> list(add_recent_trade(df, 'buy')['recent_last_trade_buy'])
    [1.0, nan, 1.0, nan, 0.0, nan, 1.0, 0.0]
    >>> list(add_recent_trade(df, 'sell')['recent_last_trade_sell'])
    [0.0, nan, 0.0, nan, 1.0, nan, 0.0, 1.0]
    """
    
    checks.loc[checks['monitor_num'] == 0, 'recent_last_trade_' + side] = checks.loc[checks['monitor_num'] == 0, 'last_trade_' + side].copy()
    for i in [1,2,3]:
        recent = checks.loc[checks['monitor_num'] == i, 'last_trade_id']
        base = checks.loc[checks['monitor_num'] == i - 1, 'last_trade_id']
        checks.loc[checks['monitor_num'] == i, 'recent_last_trade_' + side] = checks.loc[checks['monitor_num'] == i, 'last_trade_' + side].copy()
        checks.loc[(checks['monitor_num'] == i)&(recent == base.loc[recent.index]), 'recent_last_trade_' + side] = np.nan

    return checks
                
def add_recent_nonnull(checks):
    """
    >>> df = pd.DataFrame({'last_trade_type':['Buy',np.nan,'Buy','Buy', 'Sell','Sell','Buy','Sell'],
    ... 'last_trade_id':[0,np.nan,1,1, 2,2,3,4],
    ... 'monitor_num':[0,1,2,3, 0,1,2,3]})
    >>> df.index = ['a','a','a','a', 'b','b','b','b']
    >>> df = add_last_type(df, 'Buy')
    >>> df = add_recent_trade(df, 'buy')
    >>> list(add_recent_nonnull(df)['recent_last_trade_nonnull'])
    [1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0]
    """
    
    z = np.zeros(len(checks))
    y = np.array(checks['recent_last_trade_buy'].notnull())
    z[y] = 1.0
    checks['recent_last_trade_nonnull'] = z
    
    return checks


def group_bootstrap(func, data, units, samples = 100):
    """
    >>> d = pd.DataFrame({'a':[0,0,1,1,0,0,1,1],'b':[0,0,0,0,1,1,1,1],'c':[0,0,1,1,0,0,1,1]})
    >>> np.nanmean(group_bootstrap(lambda x: diff_in_means(x, 'a', 'c', 0, 1), d, 'b', samples = 1000))
    1.0
    >>> d = pd.DataFrame({'a':[0,0,1,1,0,0,2,2],'b':[0,0,0,0,1,1,1,1],'c':[0,0,1,1,0,0,1,1]})
    >>> assert abs(np.nanmean(group_bootstrap(lambda x: diff_in_means(x, 'a', 'c', 0, 1), d, 'b', samples = 1000)) - 1.5) < 0.1
    >>> d = pd.DataFrame({'a':[0,0,1,1,0,0,2],'b':[0,0,0,0,1,1,1],'c':[0,0,1,1,0,0,1]})
    >>> assert np.nanmean(group_bootstrap(lambda x: diff_in_means(x, 'a', 'c', 0, 1), d, 'b', samples = 1000)) < 1.5
    """

    data = data.copy()
    data.index = range(len(data))
    
    values = list(set(data[units]))
    indices = np.zeros((samples, len(data)), dtype = int)
    
    for v in values:
        sub = data.loc[data[units] == v]
        n = len(sub)
        resample = np.random.choice(n, size = (samples, n))
        indices[:,sub.index] = sub.index[resample]
    
    return iter_index(func, data, indices)
    
def iter_index(func, data, indices):
    """
    >>> d = pd.DataFrame({'a':[0,1,1,1],'b':[0,0,1,1]})
    >>> iter_index(lambda x: diff_in_means(x, 'a', 'b', 0, 1), d, np.array([[1,1,2,3],[0,1,2,3]]))
    array([ 0. ,  0.5])
    """
    return np.array(list(map(lambda x: func(data.iloc[x]), indices)))

def diff_in_means(data, dep_var, group_col, group_0, group_1):
    """
    >>> d = pd.DataFrame({'a':[0,0,1,1,0,0,2,2],'b':[0,0,0,0,1,1,1,1],'c':[0,0,1,1,0,0,1,1]})
    >>> diff_in_means(d, 'a', 'c', 0, 1)
    1.5
    """

    sub_0 = data.loc[data[group_col] == group_0, dep_var]
    sub_1 = data.loc[data[group_col] == group_1, dep_var]
    
    if len(sub_0) > 0 and len(sub_1) > 0:
        mean_0 = np.nanmean(sub_0)
        mean_1 = np.nanmean(sub_1)

        return mean_1 - mean_0
    else:
        return np.nan

def get_data(df, action, monitor_num):
    """
    >>> df = pd.DataFrame({
    ... 'sell_total_60':[0,1,2,3, 4,5,6,7, 8,9,10,11],
    ... 'condition':['buy']*4 + ['sell']*4 + ['control']*4,
    ... 'monitor_num':[0,1,2,3, 0,1,2,3, 0,1,2,3]})
    >>> get_data(df, "buy", 1)
      condition  monitor_num  sell_total_60
    1       buy            1              1
    9   control            1              9
    >>> get_data(df, "sell", 3)
       condition  monitor_num  sell_total_60
    7       sell            3              7
    11   control            3             11
    """
    
    keep = np.array([True]*len(df), dtype = bool)
    
    keep = keep & ((df['condition'] == action) | (df['condition'] == 'control'))
    
    keep = keep & (df['monitor_num'] == monitor_num)
    
    data = df.loc[keep].copy()
    
    assert len(data) > 0
    
    return data

def get_descriptive_column(checks, feat, log_trans):
    """
    >>> df = pd.DataFrame({
    ... 'feat':[0,1,2,3, 4,5,6,7, 8,9,10,11],
    ... 'coin_name':['a']*4 + ['a']*4 + ['b']*4,
    ... 'ids':['a','b','c','d'] + ['e']*8,
    ... 'monitor_num':[0,1,2,3, 0,1,2,3, 0,1,2,3]})
    >>> df.index = ['a','b','c','d'] + ['e']*8
    >>> get_descriptive_column(df, 'feat', False)
        coin_name  feat ids  monitor_num  coin_feat
    ids                                            
    a           a     0   a            0          2
    b           a     1   b            1          2
    c           a     2   c            2          2
    d           a     3   d            3          2
    e           a     4   e            0          2
    e           a     5   e            1          2
    e           a     6   e            2          2
    e           a     7   e            3          2
    e           b     8   e            0          8
    e           b     9   e            1          8
    e           b    10   e            2          8
    e           b    11   e            3          8
    >>> get_descriptive_column(df, 'feat', True)
        coin_name  feat ids  monitor_num  coin_feat
    ids                                            
    a           a     0   a            0   0.693147
    b           a     1   b            1   0.693147
    c           a     2   c            2   0.693147
    d           a     3   d            3   0.693147
    e           a     4   e            0   0.693147
    e           a     5   e            1   0.693147
    e           a     6   e            2   0.693147
    e           a     7   e            3   0.693147
    e           b     8   e            0   2.079442
    e           b     9   e            1   2.079442
    e           b    10   e            2   2.079442
    e           b    11   e            3   2.079442
    """
    
    by_coin = checks.groupby(['coin_name','monitor_num'], as_index=False)
    
    if log_trans:
        descriptives = by_coin[feat].mean()
        descriptives[feat] = np.log(descriptives[feat])
    else:
        descriptives = by_coin[feat].mean()
    
    descriptives = descriptives[descriptives['monitor_num'] == 0]
    
    coin_feat = 'coin_' + feat

    descriptives.columns = list(descriptives.columns[:2]) + [coin_feat]

    del descriptives['monitor_num']
    
    checks = checks.merge(descriptives,on = 'coin_name')

    checks = checks.set_index(checks['ids'])
    
    return checks


def get_daily_volume(checks):
    """
    >>> df = pd.DataFrame({
    ... 'day':['1','1','1','1', '1','1','1','1', '0','0','0','0'],
    ... 'total_60':[0]+[np.nan]*3 + [1]+[np.nan]*3 + [2]+[np.nan]*3,
    ... 'total':[np.nan,0,1,2, np.nan,3,4,5, np.nan,6,7,8],
    ... 'monitor_num':[0,1,2,3, 0,1,2,3, 0,1,2,3]})
    >>> get_daily_volume(df)
    day
    0    10.0
    1     8.0
    dtype: float64
    """
    
    before_action = checks.loc[checks['monitor_num'] == 0].groupby('day')['total_60'].sum()
    after_action = checks.loc[checks['monitor_num'] == 3].groupby('day')['total'].sum()
    
    return before_action + after_action
    

def get_coin_data(checks):
    """
    >>> df = pd.DataFrame({
    ... 'coin_name':['1','1','1','1', '1','1','1','1', '0','0','0','0', '0','0','0','0'],
    ... 'condition':['buy']*4 + ['control']*4 + ['control']*4 + ['control']*4,
    ... 'last_trade_total':[np.nan,1,1,0, np.nan,1,1,1, np.nan,1,1,2, np.nan,1,1,4],
    ... 'total':[np.nan,1,1,4, np.nan,1,1,6, np.nan,1,1,8, np.nan,1,1,10],
    ... 'monitor_num':[0,1,2,3, 0,1,2,3, 0,1,2,3, 0,1,2,3]})
    >>> get_coin_data(df)
    {'tot': [9.0, 6.0], 'size': [3.0, 1.0], 'counts': array([ 3.,  6.])}
    """
    
    coins = sorted(set(checks['coin_name']))
    
    cdata = {}
    cdata['tot'] = []
    cdata['size'] = []
    
    for c in coins:
        check_sub = checks.loc[(checks['coin_name'] == c)&(checks['monitor_num'] == 3)&(checks['condition'] == 'control')]
        cdata['tot'] += [np.nanmean(check_sub['total'])]
        cdata['size'] += [np.nanmean(check_sub['last_trade_total'])]

    cdata['counts'] = np.array(cdata['tot'])/np.array(cdata['size'])
    
    return cdata


if __name__ == "__main__":
    import doctest
    doctest.testmod()
