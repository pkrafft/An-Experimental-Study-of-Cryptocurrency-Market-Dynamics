import experiment, utils
import sys, os, datetime, numpy, time
import traceback
import numpy.random as random
from multiprocessing import Process, Lock

i = 1
OUT_DIR = i; i += 1
EPS = i; i += 1
STATE = i; i += 1

base_dir = sys.argv[OUT_DIR] + '/'

assert sys.argv[EPS] in set(['5e-7','1e-6'])
assert sys.argv[STATE] in set(['debug','run'])

epsilon = float(sys.argv[EPS])
debug = sys.argv[STATE] == 'debug'
if debug:
    delay = 1
    end_date = utils.now() + datetime.timedelta(minutes = 10)
else:
    delay = 60
    end_date = utils.now() + datetime.timedelta(days = 600)

try:
    os.mkdir(base_dir)
except OSError:
    pass

intervention_file_base = 'interventions.csv'
monitor_file_base = 'checks.csv'
out_file_base = 'messages.out'
error_file_base = 'errors.out'
counter_file_base = 'count.out'
status_file_base = 'status.out'

out_file = base_dir + out_file_base
error_file = base_dir + error_file_base
global_counter_file = base_dir + counter_file_base

pid = os.getpid()
utils.message('Starting main script...', out_file)
utils.message('Main PID: ' + str(pid), out_file)

lock = Lock()

running = {}

success = False
while not success:
    try:
        utils.message('Getting active markets...', out_file)
        markets = utils.get_markets()
        utils.message('Got active markets.', out_file)
        success = True
    except:
        traceback.print_exc(file = open(error_file,'a'))
        utils.warn('Could not get markets in main script.', out_file)
        time.sleep(5)        

if debug:
    #random.shuffle(markets)
    #markets = markets[0:10]
    markets = markets[0:10]

for m_name, m_id in markets:

    # Points is not a coin. AERO "will be closing soon". XPY too high minimum.
    if m_name == 'Points' or m_name == 'AERO' or m_name == 'XPY':
        continue
    
    try:
        os.mkdir(base_dir + m_name)
    except OSError:
        pass

    intervention_file = base_dir + m_name + '/' + intervention_file_base
    monitor_file = base_dir + m_name + '/' + monitor_file_base
    coin_out_file = base_dir + m_name + '/' + out_file_base
    coin_error_file = base_dir + m_name + '/' + error_file_base
    coin_counter_file = base_dir + m_name + '/' + counter_file_base
    coin_status_file = base_dir + m_name + '/' + status_file_base
    seed = random.randint(4294967295)

    m_args = (m_name, 
              m_id, 
              epsilon, 
              delay,
              intervention_file, 
              monitor_file, 
              coin_out_file,
              coin_error_file,
              coin_status_file,
              coin_counter_file,
              global_counter_file,
              end_date,
              lock,
              seed)

    running[m_name] = Process(target = experiment.run, 
                              args = m_args, 
                              name = m_name)
    utils.message('Starting ' + m_name + ' worker...', out_file)
    running[m_name].start()
        
for m_name in running:
    running[m_name].join()

utils.message('Main script completed successfully.', out_file)
