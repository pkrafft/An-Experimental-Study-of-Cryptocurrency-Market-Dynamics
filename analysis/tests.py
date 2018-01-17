
import seaborn as sns
import matplotlib.pyplot as plt

import numpy as np
import pandas as pd
import scipy.stats as stats

from decimal import Decimal

import analysis_utils as utils
import importlib
importlib.reload(utils)

import os

# Experiment parameters:
btc_conversion = 200 # approximate BTC/USD price at time of experiment
max_intervention_size = 5e-6 # BTC

def print_descriptives(checks, daily_volume):
    
    min_intervention_size = max_intervention_size/10
    
    trade_sizes = checks.loc[checks['monitor_num'] == 0]['last_trade_total']
    
    print('Mean daily BTC trading volume:', np.mean(daily_volume))
    print('Minimum intervention size percentile:',
          (1 - np.mean(min_intervention_size < trade_sizes))*100)
    print('Maximum intervention size percentile:',
          (1 - np.mean(max_intervention_size < trade_sizes))*100)

    
def run_ttests(checks):

    mons = [1,2,3]
    treats = ['buy','sell']
    deps = lambda a: ['recent_last_trade_' + a,'last percent ' + a + ' BTC volume','recent_last_trade_nonnull']

    ntests = len(mons)*len(treats)*len(deps('buy'))

    print('Number of tests:', ntests)

    print("\\begin{tabular}{|r|r|r|r|r|r|r|r|r|}")
    print("\\hline")
    print("\\textbf{Time} & \\textbf{Condition} & \\textbf{Dependent Var.} & \\textbf{$n$ Control} & \\textbf{$n$ Treat} & \\textbf{Control Mean} & \\textbf{Mean Effect} & \\textbf{$t$-stat} & \\textbf{$p$-value}\\\\\\hline")
    minutes = {0:'0',1:'15',2:'30',3:'60'}
    actions = {'buy':'Buy','sell':"Sell"}
    dep_vars = {'recent_last_trade_buy':'Buy Prob.',
                'recent_last_trade_sell':'Sell Prob.',
                'last percent buy BTC volume':'\% Buy Vol.',
                'last percent sell BTC volume':'\% Sell Vol.',
                'recent_last_trade_nonnull':'Trade Prob.'}
    for m in mons:
        for a in treats:
            for dep in deps(a):
                x = checks.loc[(checks['condition'] == a)&(checks['monitor_num'] == m)][dep].dropna()
                y = checks.loc[(checks['condition'] == 'control')&(checks['monitor_num'] == m)][dep].dropna()
                results = stats.ttest_ind(x,y)
                print(' & '.join(map(str, [minutes[m] + ' Min.',
                                           actions[a],
                                           dep_vars[dep],
                                           len(y),
                                           len(x),
                                           "{:.3f}".format(round(np.mean(y),3)),
                                           "{:.3f}".format(round(np.mean(x) - np.mean(y),3)),
                                           "{:.2f}".format(round(results.statistic,2)),
                                           '%.2e' % Decimal(min(ntests*results.pvalue,1))])) + '\\\\\\hline')

    print('\\end{tabular}')


def get_bootstrap_results(checks):
    
    dep_var = {}
    
    dep_var['buy'] = {'trade':'recent_last_trade_buy',
                      'perc':'last percent buy BTC volume',
                      'null':'recent_last_trade_nonnull'}

    dep_var['sell'] = {'trade':'recent_last_trade_sell',
                       'perc':'last percent sell BTC volume',
                       'null':'recent_last_trade_nonnull'}
    
    results = {'trade':{},'perc':{},'null':{}}
    
    for d in results:
        results[d] = {}
        for a in ['buy','sell']:
            results[d][a] = {}
            for t in [1,2,3]:
                results[d][a][t] = get_unconditional_results(checks, a, t, dep_var[a][d])
    
    return results

    
def get_unconditional_results(df, action, monitor_num, dep_var):
    
    data = utils.get_data(df, action, monitor_num)
    
    results = utils.group_bootstrap(
        lambda x: utils.diff_in_means(x, dep_var, 'condition', 'control', action), data, 'coin_name', samples = 1000)
        
    return results


def violin(results, groups, xlabel, ylabel, title = None, filebase = None):

    x = []
    y = []
    for g in groups:
        x += [groups[g]]*len(results[g])
        y += list(results[g])
    
    sns.set(font_scale = 2.75,
            font = 'serif',
            style = 'white',
            palette = 'colorblind',
            context = 'paper')
    
    sns.violinplot(xlabel, ylabel, data = pd.DataFrame({xlabel:x,ylabel:y}), order = sorted(list(set(x))))
    if title is not None:
        sns.plt.title(title)
    plt.axhline(linewidth=6, linestyle = 'dashed', color='black')
    plt.xticks(rotation=10)
    if filebase is not None:
        plt.savefig(filebase + '.jpg', bbox_inches = 'tight')
    plt.show()
    plt.close()

    
def run_regressions(checks):
    
    reg_features = ['coin_name','monitor_num','buy_state','sell_state','buy_eats_block', 'sell_eats_block','condition',
                    'recent_last_trade_buy','recent_last_trade_sell','recent_last_trade_nonnull','last percent buy BTC volume',
                    'last percent sell BTC volume',
                    'this_trade_total','initial_total_60','initial_last_trade_buy','initial_last_trade_sell',
                    'initial_last percent buy BTC volume','initial_last percent sell BTC volume',
                    'initial_weekday','coin_total_60']
    
    checks[reg_features].to_csv('tmp_regression_features.csv',index = False)
    
    os.system('Rscript regression_analysis.R > tmp')
    with open('tmp') as f:
        for l in f.readlines():
            print(l)

            
def get_total_effect_size(checks, interventions):

    x = checks.loc[(checks['condition'] == 'buy')&(checks['monitor_num'] == 1),'buy_total']

    mae = np.mean(np.abs(x - np.mean(x))) 
    mse = np.mean((x - np.mean(x))**2)
    
    print('MAE/MSE ratio:', mae / mse)
    
    sub = checks.loc[(checks['monitor_num'] == 1)&(~checks['buy_total'].isnull())]
    
    Control = sub.loc[(sub['condition'] == 'control'),'buy_total']
    Buy = sub.loc[(sub['condition'] == 'buy'),'buy_total']
    Sell = sub.loc[(sub['condition'] == 'sell'),'buy_total']

    n_ctrl = float(len(Control))
    n_buy = float(len(Buy))
    n_sell = float(len(Sell))

    mean_ctrl = np.mean(Control)
    mean_buy = np.mean(Buy)
    mean_sell = np.mean(Sell)
    
    sum_ctrl = sum(Control)
    sum_buy = sum(Buy)
    sum_sell = sum(Sell)
    
    print('Buy vs. Control test')    
    print (stats.mannwhitneyu(Buy, Control))

    print('Buy vs. Sell test')
    print (stats.mannwhitneyu(Buy, Sell))

    print()
    print ('Number of sell-side measurements', n_sell)
    print ('Number of control measurements', n_ctrl)
    print ('Number of buy measurements', n_buy)
    print ('Difference in number between buy v. control', n_buy - n_ctrl)
    print ('Proportional difference between buy v. control', (n_buy - n_ctrl)/n_ctrl)
    print ('Difference in number between buy v. sell', n_buy - n_sell)
    print ('Proportional difference between buy v. sell', (n_buy - n_sell)/n_sell)


    print()
    print ('Mean of sell-side measurements', mean_sell)
    print ('Mean of control measurements', mean_ctrl)
    print ('Mean of buy measurements', mean_buy)
    print ('Difference in mean between buy v. control', mean_buy - mean_ctrl)
    print ('Proportional difference between buy v. control', (mean_buy - mean_ctrl)/mean_ctrl)
    print ('Difference in mean between buy v. sell', mean_buy - mean_sell)
    print ('Proportional difference between buy v. sell', (mean_buy - mean_sell)/mean_sell)
    
    print()
    print ('Sum of sell-side measurements', sum_sell)
    print ('Sum of control measurements', sum_ctrl)
    print ('Sum of buy measurements', sum_buy)
    print ('Difference in sum between buy v. control', sum_buy - sum_ctrl)
    print ('Proportional difference between buy v. control', (sum_buy - sum_ctrl)/sum_ctrl)
    print ('Difference in sum between buy v. sell', sum_buy - sum_sell)
    print ('Proportional difference between buy v. sell', (sum_buy - sum_sell)/sum_sell)

    total_spent = sum(interventions.loc[interventions['condition'] == 'buy','this_trade_total'])
    
    print()
    print ('Total BTC spent on buys:', total_spent)
    print('Total USD spent on buys', total_spent * btc_conversion)
    print ('Percent multiplier effect', (sum_buy - sum_ctrl) / total_spent )

    
def get_state_fractions(interventions):

    print('Fraction of buys that increase price:', np.mean(interventions['buy_state'] == 'up'))
    print('Fraction of sell that decrease price:', np.mean(interventions['sell_state'] == 'down'))

def bot_check(checks):

    variables = ['condition','recent_last_trade_buy','last percent buy BTC volume','recent_last_trade_nonnull','humantimes','weekday']
    
    checks[variables].loc[(checks['monitor_num'] == 1)&(checks['condition'] != 'control')].to_csv('tmp_bot_check.csv')
    
    os.system('Rscript bot_check.R > tmp')
    with open('tmp') as f:
        for l in f.readlines():
            print(l)
