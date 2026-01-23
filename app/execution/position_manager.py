#app/execution/position_manager.py
class PositionManager:
    def __init__(self, entry, sl, qty):
        self.entry = entry
        self.sl = sl
        self.qty = qty
        self.risk = abs(entry - sl)

        self.partial_done = False
        self.sl_order_id = None

    def process_ltp(self, ltp):
        """
        Returns action based on LTP:
        - PARTIAL_BOOK at 1R
        - TRAIL_SL at 1.5R
        """
        # Partial book at 1R
        if not self.partial_done and ltp >= self.entry + self.risk:
            self.partial_done = True
            return "PARTIAL_BOOK"

        # Trail SL after 1.5R
        if ltp >= self.entry + (1.5 * self.risk):
            return "TRAIL_SL"

        return None
