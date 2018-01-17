
import os
import pandas as pd
import random

in_dir = '../larger-processed/'
out_dir = '../larger-processed/'

try:
    os.make_dirs(out_dir)
except:
    pass

checks = pd.read_csv(open(in_dir + 'large-interventions-2015-12-20-checks.csv'))
interventions = pd.read_csv(open(in_dir + 'large-interventions-2015-12-20-interventions.csv'))

num = len(str(max(checks['intervention_num'])))

ids = checks['coin_name'].map(str)+'-'+checks['intervention_num'].map(lambda x: str(x).zfill(num))
checks['ids'] = ids

ids = interventions['coin_name'].map(str)+'-'+interventions['intervention_num'].map(lambda x: str(x).zfill(num))
interventions['ids'] = ids

ids = list(set(checks['ids']))

random.shuffle(ids)

hold_out_inds = sorted(ids[:(len(ids)/2)])
keep_inds = sorted(ids[(len(ids)/2):])

hold_out_checks = checks[map(lambda x: x in hold_out_inds, checks['ids'])]
hold_out_inter = interventions[map(lambda x: x in hold_out_inds, interventions['ids'])]

keep_checks = checks[map(lambda x: x in keep_inds, checks['ids'])]
keep_inter = interventions[map(lambda x: x in keep_inds, interventions['ids'])]

hold_out_checks.to_csv(out_dir + 'held_out_checks.csv', index = False)
hold_out_inter.to_csv(out_dir + 'held_out_interventions.csv', index = False)
keep_checks.to_csv(out_dir + 'kept_checks.csv', index = False)
keep_inter.to_csv(out_dir + 'kept_interventions.csv', index = False)
