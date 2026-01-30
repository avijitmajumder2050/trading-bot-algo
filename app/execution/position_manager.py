class PositionManager:
    def __init__(self, entry, sl, qty, side, rr=1.5):
        self.entry = entry
        self.sl = sl
        self.qty = qty
        self.side = side.upper()
        self.rr = rr

        self.risk = abs(entry - sl)
        self.partial_done = False

        # Pre-calc targets
        if self.side == "BUY":
            self.one_r = self.entry + self.risk
            self.target = self.entry + (self.rr * self.risk)
        else:
            self.one_r = self.entry - self.risk
            self.target = self.entry - (self.rr * self.risk)

    def get_target_price(self):
        """Used for Super Order / logging"""
        return round(self.target, 2)

    def process_ltp(self, ltp):
        """
        Returns:
        - PARTIAL_BOOK at 1R
        - TRAIL_SL at RR (default 1.5R)
        """

        # 1R Partial Book
        if not self.partial_done:
            if (self.side == "BUY" and ltp >= self.one_r) or \
               (self.side == "SELL" and ltp <= self.one_r):
                self.partial_done = True
                return "PARTIAL_BOOK"

        # 1.5R Trail SL / Exit logic
        if (self.side == "BUY" and ltp >= self.target) or \
           (self.side == "SELL" and ltp <= self.target):
            return "TRAIL_SL"

        return None
