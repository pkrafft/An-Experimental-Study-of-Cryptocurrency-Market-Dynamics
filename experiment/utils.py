
import datetime
import pytz
import csv
import json
import urllib2
import sys
sys.path.append('../')

# import exchange API from a hidden file
from exchange import Api

# import public and private API keys from a hidden file
import keys
PUB_KEY = keys.pub_key()
PRIV_KEY = keys.priv_key()

def get_markets():
    
    api = Api(PUB_KEY, PRIV_KEY)
    
    all_markets = api.markets()['return']
    markets = []
    
    for item in all_markets:
        if item['secondary_currency_code'] == 'BTC':
            markets += [(item['primary_currency_code'], item['marketid'])]
    
    return markets

def parse_time(time):
    return datetime.datetime.strptime(time, '%Y-%m-%d %H:%M:%S')

def str_time(time):
    return time.strftime('%Y-%m-%d %H:%M:%S')

def now(string = False):
    time = datetime.datetime.now(pytz.timezone('US/Eastern')).replace(tzinfo=None)
    if string:
        time = time.strftime('%Y-%m-%d %H:%M:%S')
    return time

def get_count(counter_file):
    try:
        f = open(counter_file)
        num = int(f.readline().strip())
        f.close()
    except:
        f = open(counter_file, 'w')
        f.write('0\n')
        f.close()
        num = 0
    return num

def increment_count(counter_file):
    num = get_count(counter_file)
    f = open(counter_file, 'w')
    f.write(str(num + 1) + '\n')
    f.close()
    return num + 1

def write(out, filename):
    f = open(filename, 'a')
    writer = csv.writer(f)
    writer.writerow(out)
    f.close()    

def warn(message, out_file, critical = False):
    message = 'Warning (' + now(True) + '): ' + message
    if critical:
        message = 'CRITICAL ' + message
    f = open(out_file, 'a')
    f.write(message + '\n')
    f.close()

def message(message, out_file):
    message = 'Status (' + now(True) + '): ' + message
    f = open(out_file, 'a')
    f.write(message + '\n')
    f.close()

class Monitor():

    def __init__(self):
        self.api = Api(PUB_KEY, PRIV_KEY)
        self.ids = dict(get_markets())
    
    def clear_orders(self):
        orders = self.api.my_orders()['return']
        for order in orders:
            self.api.cancel_order(order['orderid'])
        bank = self.api.info()['return']['balances_available']
        for coin in bank:
            if float(bank[coin]) > 0 and coin != 'BTC':
                price = utils.get_highest_buy_order(self.ids[coin], coin)
                print('Selling ' + bank[coin] + ' ' + coin + 
                      '(' + m_id + ') for ' + str(price))
                print self.api.sell(self.ids[coin], bank[coin], price)

    def check_bank(self):
        bank = self.api.info()['return']['balances_available']
        for coin in bank:
            if float(bank[coin]) > 0:
                print coin, bank[coin]

    def check_orders(self):
        return self.api.my_orders()

if __name__ == "__main__":
    import doctest
    doctest.testmod()

