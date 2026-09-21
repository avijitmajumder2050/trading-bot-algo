class PositionManager:
    def __init__(self, entry, sl, qty, side, rr=5):
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

    def get_trailing_jump(self, multiplier=0.5):
        """Rupee jump Dhan's server uses to keep trailing the SL leg on
        its own after TRAIL_SL fires at 1R — same multiplier
        dhan_super_client.place_trade() uses for the initial order's
        own trailingJump, so the trail step size stays consistent
        across entry and the post-breakeven trail."""
        return round(self.risk * multiplier, 2)

    def process_ltp(self, ltp):
        """
        Returns:
        - TRAIL_SL once at 1R (move SL to entry + enable Dhan-native
          trailing from there — see get_trailing_jump())
        - EXIT_TRADE at RR (default 5R) as a market-order backstop, in
          case the Super Order's own TARGET_LEG hasn't already closed
          the trade by the time this ltp poll notices target's hit
        """

        # 1R breakeven + start trailing
        if not self.partial_done:
            if (self.side == "BUY" and ltp >= self.one_r) or \
               (self.side == "SELL" and ltp <= self.one_r):
                self.partial_done = True
                return "TRAIL_SL"

        # RR target reached (default 5R)
        if (self.side == "BUY" and ltp >= self.target) or \
           (self.side == "SELL" and ltp <= self.target):
            return "EXIT_TRADE"

        return None
