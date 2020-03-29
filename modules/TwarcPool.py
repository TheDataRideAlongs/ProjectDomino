class TwarcPool:

    def __init__(self, pool):
        self.pool = pool
        self.last_idx = 0
        
    def next_twarc(self):
        idx = (self.last_idx + 1) % len(self.pool)
        self.last_idx = idx
        t = self.pool[ idx ]
        return t