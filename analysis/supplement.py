
import pandas as pd
import numpy as np

def ppoints(vector):
    '''
    Mimics R's function 'ppoints'.
    http://stackoverflow.com/questions/20292216/imitating-ppoints-r-function-in-python
    '''

    m_range = int(vector[0]) if len(vector)==1 else len(vector)
    
    n = vector[0] if len(vector)==1 else len(vector)
    a = 3./8. if n <= 10 else 1./2
    
    m_value =  n if len(vector)==1 else m_range
    pp_list = [((m+1)-a)/(m_value+(1-a)-a) for m in range(m_range)]
    
    return pp_list

def add_trivialness(checks, trivial_method):

    coin_df = pd.read_csv('trivial-coins.csv')
    coin_attr = dict(zip(coin_df['coin'], coin_df[trivial_method]))
    missing = set(checks['coin_name']).difference(set(coin_attr.keys()))
    print('missing coins:', missing)
    for c in missing:
        coin_attr[c] = np.nan
    checks['trivial'] = checks['coin_name'].replace(coin_attr)

    return checks
