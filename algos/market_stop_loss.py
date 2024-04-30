def place_market_order(init_prx: float, cur_prx: float, side: str, pct: float):
    """
    This function will place market order based on the threshold in percentage
    """
    if side == "long" and cur_prx - init_prx / init_prx <= pct / 100:
        # place short order
        pass
    elif side == "short" and cur_prx - init_prx / init_prx >= pct / 100:
        # place long order
        pass
