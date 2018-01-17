
import utils
import os, heapq, time, pickle, datetime, requests, sys
import numpy.random as random
import traceback
sys.path.append('../')

# import exchange API from a hidden file
from exchange import Api

# import public and private API keys from a hidden file
import keys
PUB_KEY = keys.pub_key()
PRIV_KEY = keys.priv_key()

def run(m_name, m_id, epsilon, delay, intervention_file, monitor_file,
        out_file, error_file, status_file, my_counter_file,
        global_counter_file, end_date, lock_local, seed):
    
    global lock
    lock = lock_local

    random.seed(seed)

    if os.path.exists(out_file):
        utils.message('Restarting ' + m_name + ' experiment...', out_file)
    else:
        utils.message('Starting new ' + m_name + ' experiment...', out_file)

    pid = os.getpid()
    utils.message(m_name + ' PID: ' + str(pid), out_file)
        
    exprmnt = Experiment(m_name, m_id, epsilon, delay,
                         intervention_file, monitor_file, out_file,
                         error_file, status_file, my_counter_file,
                         global_counter_file, end_date)
        
    exprmnt.run()

class Experiment():
    
    def __init__(self, m_name, m_id, epsilon, delay,
                 intervention_file, monitor_file, out_file,
                 error_file, status_file, my_counter_file,
                 global_counter_file, end_date):
        
        self.api = Api(PUB_KEY, PRIV_KEY)
        
        self.m_name = m_name
        self.m_id = int(m_id)
        
        self.epsilon = epsilon
        self.delay = delay
        
        self.intervention_file = intervention_file
        self.monitor_file = monitor_file
        self.out_file = out_file
        self.error_file = error_file
        self.my_counter_file = my_counter_file
        self.status_file = status_file
        self.global_counter_file = global_counter_file
        
        self.intervention_num = utils.get_count(my_counter_file)
        self.end_date = end_date
        
        self.set_status('ok') 

        self.monitor_num = -1
        self.init_queue()
        self.control_trade_id = None
        
        self.cancel_trades()

    def done(self):
        return utils.now() >= self.end_date or self.canceled()
        
    def init_queue(self):
        self.queue = []
        if not self.done():
            t = utils.now() + datetime.timedelta(minutes = self.random_wait())
            heapq.heappush(self.queue, (t, 'intervene'))
        else:
            self.warn(0)
    
    def random_wait(self):
        return self.delay + random.random()*self.delay
    
    def run(self):
        
        while len(self.queue) > 0:
            
            (next_time, next_action) = heapq.heappop(self.queue)
            delay = (next_time - utils.now()).total_seconds()
            if delay > 0:
                time.sleep(delay)
            self.do(next_action)
        
        if not self.done():
            self.warn(1)
        else:
            if self.canceled():
                self.warn(8)
            else:
                self.log(0)
    
    def do(self, action):
        
        lock.acquire()
        
        get_trade_success = False
        while not get_trade_success:
            get_trade_success, trades = self.get_my_trades()
            if not get_trade_success:
                lock.release()
                time.sleep(10)
                lock.acquire()
        
        self.my_trade_ids = set([trades[i]['tradeid'] for i in range(len(trades))])
            
        if action == 'intervene':
            self.log(1)
            self.try_to_intervene()
        else:
            assert action == 'monitor'
            self.log(2)
            self.monitor()
        
        lock.release()
    
    def try_to_intervene(self):

        balance_success, btc, this_coin = self.get_balances()
        
        max_btc = 10*self.epsilon
        amount_btc = random.uniform(self.epsilon, max_btc)
        
        prices = {}
        
        intervention_start = utils.now()

        trade_success, trades = self.get_trade_data()
        if trade_success:
            t = intervention_start - datetime.timedelta(minutes = self.delay)
            volume, total = self.volume_since(t, trades)
            volume_success = volume > 0
            if not volume_success:
                self.warn(10)
        
        order_success, lowest_sell, highest_buy = self.get_order_data()

        success = balance_success and order_success and trade_success and volume_success
        
        if success:
            
            self.log(8)
            
            last_trade = trades[0]
            prices['buy'] = float(lowest_sell['sellprice'])
            prices['sell'] = float(highest_buy['buyprice'])
            prices['last'] = float(last_trade['tradeprice'])
            
            have_enough_to_buy = btc > max_btc
            have_enough_to_sell = this_coin > max_btc/prices['sell']
            if not have_enough_to_buy and not have_enough_to_sell:
                #self.set_status('cancel')
                self.warn(7, (btc, max_btc, this_coin, max_btc/prices['sell']))
                success = False
            else:                
                success = True
            
            if success:

                if have_enough_to_buy and have_enough_to_sell:
                    self.condition = random.choice(['buy','sell','buy-control','sell-control'])
                elif have_enough_to_buy:
                    self.condition = random.choice(['buy','buy-control'])
                else:
                    assert have_enough_to_sell
                    self.condition = random.choice(['sell','sell-control'])
                
                self.log(7)
                
                self.success, this_trade = self.intervene(amount_btc, prices)
                
                self.buy_eats_block = float(lowest_sell['total']) <= amount_btc
                self.sell_eats_block = float(highest_buy['total']) <= amount_btc
                
                self.buy_state, self.sell_state = self.get_state(prices)
                
                self.intervention_num = utils.increment_count(self.my_counter_file)
                utils.increment_count(self.global_counter_file)
                
                self.monitor_num = -1
                self.monitor(lowest_sell, highest_buy, trades, intervention_start)
                
                if self.condition not in ['buy','sell']:
                    self.control_trade_id = last_trade['tradeid']
                
                self.intervention_end = utils.now()
                inter_intervention_wait = self.random_wait()
                self.write_intervention(intervention_start, self.intervention_end, inter_intervention_wait, last_trade, this_trade)
                
                for frac in [0.25, 0.5, 1]:
                    t = self.intervention_end + datetime.timedelta(minutes = self.delay * frac)
                    heapq.heappush(self.queue, (t, 'monitor'))
                
                if not self.done():
                    t = self.intervention_end + datetime.timedelta(minutes = inter_intervention_wait)
                    heapq.heappush(self.queue, (t, 'intervene'))
        else:
            success = False
        
        if not success:
            if not self.done():
                t = utils.now() + datetime.timedelta( minutes = self.delay/8.0*(1 + random.random()) )
                heapq.heappush(self.queue, (t, 'intervene'))            
    
    def intervene(self, amount, prices):
        
        this_trade = self.empty_trade()
        
        if self.condition in ['buy','sell']:
            price = prices[self.condition]
            quantity = amount/price
            execute_success = self.execute(quantity, price)
        else:
            execute_success = True
        
        lock.release()
        time.sleep(10)
        lock.acquire()

        if execute_success:
            
            if self.condition in ['buy','sell']:
                get_trade_success, trades = self.get_my_trades(delay = 10, get_new_trade = True)
            else:
                get_trade_success, trades = self.get_my_trades(delay = 10, get_new_trade = False)
            
            cancel_success = self.cancel_trades()
            
            if get_trade_success and cancel_success:
                lock.release()
                time.sleep(10)
                lock.acquire()
                get_trade_success, trades = self.get_my_trades()
                if get_trade_success and self.condition in ['buy','sell']:
                    this_trade = trades[0]
        else:
            
            self.cancel_trades()
        
        success = execute_success and get_trade_success and cancel_success
        
        return success, this_trade
    
    def execute(self, quantity, price):
        
        success = False
        try:
            if self.condition == 'buy':
                reply = self.api.buy(self.m_id, quantity, price)
            else:
                reply = self.api.sell(self.m_id, quantity, price)
            self.log(5, self.condition)
            assert reply['success'] == '1'
            success = True
        except:
            self.error()
            self.warn(9, (self.condition, quantity * price))
            try:
                self.warn(2, (self.condition, reply['error']))
            except:
                self.error()
                self.warn(3, self.condition)
        
        return success
    
    def cancel_trades(self):
        success = False
        try:
            self.log(4, 'cancel')
            reply = self.api.cancel_all_market_orders(self.m_id)
            self.log(5, 'cancel')
            assert reply['success'] == '1'
            assert 'return' not in reply
            success = True
        except:
            self.error()
            self.warn(4)
        return success
        
    def get_my_trades(self, delay = 0, get_new_trade = False):
        
        success = False
        trades = None
        
        for i in range(20):
            try:
                self.log(4, 'get trade')
                reply = self.api.my_trades(self.m_id)
                self.log(5, 'last trade')
                trades = reply['return']
                if len(trades) > 0:
                    this_trade = trades[0]
                else:
                    this_trade = self.empty_trade()
                if get_new_trade:
                    assert this_trade['tradeid'] != ''
                    assert this_trade['tradeid'] not in self.my_trade_ids
                success = True
            except:
                self.warn(5)
                self.error()
            if success:
                break
            else:
                lock.release()
                time.sleep(delay)
                lock.acquire()
        
        if not success:
            self.warn(5)
        
        return success, trades

    def get_state(self, prices):
        
        if prices['buy'] > prices['last']:
            buy_state = 'up' 
        elif prices['buy'] < prices['last']:
            buy_state = 'down' 
        else:
            buy_state = 'stay' 

        if prices['sell'] > prices['last']:
            sell_state = 'up' 
        elif prices['sell'] < prices['last']:
            sell_state = 'down' 
        else:
            sell_state = 'stay'
        
        return buy_state, sell_state
    
    def monitor(self, sell_order = None, buy_order = None, trades = None, now = None):
        
        self.monitor_num += 1

        if now == None:
            now = utils.now()
            order_success, sell_order, buy_order = self.get_order_data()
            trade_success, trades = self.get_trade_data()
            last_trade = trades[0]
            
            trade_id = last_trade['tradeid']
            if trade_id == self.control_trade_id or trade_id in self.my_trade_ids:
                last_trade = self.empty_trade()
            time = None
        else:
            last_trade = trades[0]
            time = now
        
        volume, total, past_volumes, past_totals = ({}, {}, {}, {})
        volume['buy'], total['buy'], past_volumes['buy'], past_totals['buy'] = self.compute_volumes(trades, time, 'Buy')
        volume['sell'], total['sell'], past_volumes['sell'], past_totals['sell'] = self.compute_volumes(trades, time, 'Sell')
        
        self.write_monitor(now, past_volumes, past_totals, volume, total, last_trade, buy_order, sell_order)

    def get_balances(self):
        
        success = False
        btc = None
        this_coin = None
        
        try:
            self.log(4, 'get balances')
            balances = self.api.info()['return']['balances_available']
            self.log(5, 'balance')
            btc = float(balances['BTC'])
            this_coin = float(balances[self.m_name])
            success = True
        except:
            self.error()
            self.warn(6, 'balance')
        
        return success, btc, this_coin
        
    def get_order_data(self):
        
        success = False
        sell_order = self.empty_order()
        buy_order = self.empty_order()
        
        try:
            self.log(4, 'get orders')
            reply = self.api.market_orders(self.m_id)
            self.log(5, 'order')
            sell_order = reply['return']['sellorders'][0] 
            buy_order = reply['return']['buyorders'][0]
            success = True
        except:
            self.error()
            self.warn(6, 'order')
        
        return success, sell_order, buy_order

    def get_trade_data(self):
        
        success = False
        
        try:
            self.log(4, 'get recent')
            reply = self.api.market_trades(self.m_id)
            self.log(5, 'recent trade')
            trades = reply['return']
            assert len(trades) > 0
            success = True
        except:
            self.error()
            self.warn(6, 'trade')
            trades = [self.empty_trade()]
        
        return success, trades

    def compute_volumes(self, trades, time, side):

        volume = ''
        total = ''
        past_volumes = ['','','']
        past_totals = ['','','']
        if trades[0] != self.empty_trade():            
            if time == None:
                volume, total = self.volume_since(self.intervention_end, trades, side)
            else:
                fracs = [0.25, 0.5, 1]
                for i,frac in enumerate(fracs):
                    t = time - datetime.timedelta(minutes = self.delay * frac)
                    past_volumes[i], past_totals[i] = self.volume_since(t, trades, side)
        
        return volume, total, past_volumes, past_totals
            
    
    def volume_since(self, time, trades, side = None):
        
        volume = 0
        total = 0
        i = 0
        recent = utils.parse_time(trades[i]['datetime']) > time
        not_mine = trades[i]['tradeid'] not in self.my_trade_ids
        not_control = trades[i]['tradeid'] != self.control_trade_id
        while recent and not_mine and not_control:
            if side == None or trades[i]['initiate_ordertype'] == side:
                volume += float(trades[i]['quantity'])
                total += float(trades[i]['total'])
            i += 1
            if i >= len(trades):
                break
            recent = utils.parse_time(trades[i]['datetime']) > time
            not_mine = trades[i]['tradeid'] not in self.my_trade_ids
            not_control = trades[i]['tradeid'] != self.control_trade_id

        return volume, total
    
    def empty_trade(self):
        return {'order_id':'', 
                'tradeid':'', 
                'datetime':'', 
                'quantity':'', 
                'tradeprice':'', 
                'total':'',
                'initiate_ordertype':''}
    
    def empty_order(self):
        return {'total':'',
                'quantity':'',
                'sellprice':'',
                'buyprice':''}
    
    def canceled(self):
        
        status = False
        
        try:
            f = open(self.status_file)
            status = f.readline().strip() != 'ok'
            f.close()
        except:
            f = open(self.status_file, 'w')
            f.write('ok\n')
            f.close()
        
        return status
    
    def set_status(self, status):
        f = open(self.status_file, 'w')
        f.write(status + '\n')
        f.close()
    
    def write_intervention(self, start, end, wait, last_trade, this_trade):
        utils.write([self.m_name, 
                     self.m_id, 
                     self.intervention_num,
                     self.buy_state,
                     self.sell_state,
                     self.buy_eats_block,
                     self.sell_eats_block,
                     self.condition,
                     self.success,
                     utils.str_time(start),
                     utils.str_time(end),
                     wait,
                     last_trade['initiate_ordertype'], 
                     last_trade['tradeid'], 
                     last_trade['datetime'], 
                     last_trade['quantity'], 
                     last_trade['tradeprice'], 
                     last_trade['total'], 
                     this_trade['order_id'],
                     this_trade['tradeid'], 
                     this_trade['datetime'], 
                     this_trade['quantity'], 
                     this_trade['tradeprice'], 
                     this_trade['total'],
                 ], 
                    self.intervention_file)
    
    def write_monitor(self, now, past_volumes, past_totals, volume, total, last_trade, buy_order, sell_order):
        utils.write([self.m_name, 
                     self.m_id, 
                     self.intervention_num,
                     self.monitor_num,
                     self.buy_state,
                     self.sell_state,
                     self.buy_eats_block,
                     self.sell_eats_block,
                     self.condition,
                     self.success,
                     utils.str_time(now),
                     past_volumes['buy'][0] + past_volumes['sell'][0],
                     past_volumes['buy'][1] + past_volumes['sell'][1],
                     past_volumes['buy'][2] + past_volumes['sell'][2],
                     past_totals['buy'][0] + past_totals['sell'][0],
                     past_totals['buy'][1] + past_totals['sell'][1],
                     past_totals['buy'][2] + past_totals['sell'][2],
                     volume['buy'] + volume['sell'],
                     total['buy'] + total['sell'],
                     past_volumes['buy'][0],
                     past_volumes['buy'][1],
                     past_volumes['buy'][2],
                     past_totals['buy'][0],
                     past_totals['buy'][1],
                     past_totals['buy'][2],
                     volume['buy'],
                     total['buy'],
                     past_volumes['sell'][0],
                     past_volumes['sell'][1],
                     past_volumes['sell'][2],
                     past_totals['sell'][0],
                     past_totals['sell'][1],
                     past_totals['sell'][2],
                     volume['sell'],
                     total['sell'],
                     last_trade['initiate_ordertype'], 
                     last_trade['tradeid'], 
                     last_trade['datetime'], 
                     last_trade['quantity'], 
                     last_trade['tradeprice'], 
                     last_trade['total'], 
                     buy_order['buyprice'],
                     buy_order['total'],
                     buy_order['quantity'],
                     sell_order['sellprice'],
                     sell_order['total'],
                     sell_order['quantity'],
                 ],
                    self.monitor_file)

    def warn(self, message, data = None):
        
        if message == 0:
            utils.warn('Experiment already complete! ' + self.m_name, self.out_file)
        
        elif message == 1:
            utils.warn('Ran out of things to do! ' + self.m_name, self.out_file, True)

        elif message == 2:
            utils.warn('Failed to confirm ' + data[0] + ' order for ' + self.m_name + 
                       '.  Error message: ' + data[1], self.out_file)
        
        elif message == 3:
            utils.warn('Bad response from exchange. Could not verify'
                       ' whether or not ' + data + ' order for ' + self.m_name + 
                       ' was submitted.', self.out_file)
        
        elif message == 4:
            utils.warn('Failed to cancel ' + self.m_name + ' orders.', self.out_file)

        elif message == 5:
            utils.warn('Failed to get record of ' + self.m_name +
                       ' transaction.', self.out_file)

        elif message == 6:
            utils.warn(self.m_name + ' ' + data + ' data not found.', self.out_file)

        elif message == 7:
            utils.warn('Insufficient funds (BTC, BTC price, coin, coin price): ' +
                       str(data) + ' ' + self.m_name, self.out_file)

        elif message == 8:
            utils.warn('Process terminated. ' + self.m_name, self.out_file)

        elif message == 9:
            utils.warn('Execute ' + data[0] + ' failed with BTC amount ' + str(data[1]) + ' ' + self.m_name, self.out_file)

        elif message == 10:
            utils.warn(self.m_name + ' has no recent trades.', self.out_file)
        
        else:
            utils.warn('Unknown warning ID: ' + message, self.out_file)
    
    def log(self, message, data = None):
        
        if message == 0:
            utils.message(self.m_name + ' completed successfully.', self.out_file)
        
        elif message == 1:
            utils.message('Looking to intervene ' + self.m_name + '...', self.out_file)
        
        elif message == 2:
            utils.message('Checking on ' + self.m_name + '...', self.out_file)
                
        elif message == 4:
            utils.message(self.m_name + ' trying to ' + data + '...', self.out_file)
        
        elif message == 5:
            utils.message(self.m_name + ' got reply to ' + data + ' request.', self.out_file)

        elif message == 6:
            utils.message('Balances (BTC, coin): ' + str(data) + ' ' + self.m_name + '.', self.out_file)

        elif message == 7:
            utils.message(self.m_name + ' state: ' + self.condition + '.', self.out_file)

        elif message == 8:
            utils.message('Trying to intervene ' + self.m_name + '...', self.out_file)

        else:
            utils.warn('Unknown message ' + message, self.out_file)

    def error(self):
        utils.warn('Runtime error in ' + self.m_name, self.error_file)
        traceback.print_exc(file = open(self.error_file,'a'))
