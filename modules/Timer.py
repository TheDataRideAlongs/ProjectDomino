import time

import logging
logger = logging.getLogger('Timer')

class Timer:
    def __init__(self):
        self.counters = {}
        
    def tic(self, name, n=20, print_freq=0):
        if not (name in self.counters):
            self.counters[name] = {
                'tic': time.time(),
                'n': n,
                'k': 0,
                'print_freq': print_freq,
                'lastN': [0 for i in range(0, n)],
                'lastN_val': [0 for i in range(0, n)],
                'rolling_val_sum': 0
            }
        else:
            self.counters[name]['tic'] = time.time()
        
    def toc(self, name, val = None):
        toc = time.time()
        counter = self.counters[name]
        k = counter['k']
        n = counter['n']
        k_mod_n = k % n
        counter['lastN'][ k_mod_n ] = toc - counter['tic']
        if not (val is None):
            counter['lastN_val'][ k_mod_n ] = val
        self.maybe_emit(name, not (val is None))
        counter['k'] = k + 1
            
    def maybe_emit(self, name, show_val_per_second):
        counter = self.counters[name]
        k = counter['k']
        n = counter['n']
        if show_val_per_second:
            lastN = counter['lastN']
            lastN_val = counter['lastN_val']
            sum_s = 0.0 + sum([lastN[i % n ] for i in range(max(k - n,0), k)])
            sum_val = 0.0 + sum([lastN_val[i % n ] for i in range(max(k - n,0), k)])
            counter['rolling_val_sum'] = counter['rolling_val_sum'] + sum_val
            if counter['print_freq'] > 0 and (k % counter['print_freq'] == 0):
                if sum_s > 0:
                    logger.info('%s : %s / s (%s total)' % (name, sum_val / sum_s, counter['rolling_val_sum']))
        else:
            if counter['print_freq'] > 0 and (k % counter['print_freq'] == 0):
                lastN = counter['lastN']
                sum_s = 0.0 + sum([lastN[i % n ] for i in range(max(k - n,0), k)])
                logger.info('%s : %ss' % (name, sum_s / min(n, k + 1)))

        
