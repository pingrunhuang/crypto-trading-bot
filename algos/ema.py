import datetime


def sma(prices: list[float], days: int):
    return sum(prices)/days


def fetch_prices(dt: datetime.datetime, days: int):
    """
    fetch dt's previous days close prices
    to implement
    """
    return [1 for _ in range(days)]


def _ema(prices: list[float], days: int, smooth_factor: float = 2):
    """
    prices is ordered by oldest to nearest
    """
    if len(prices) == days:
        return sma(prices, days)
    prx = prices.pop()
    # this raise will cause lately price matter more
    multiplier = smooth_factor/(days+1)
    return prx*multiplier + _ema(prices, days, smooth_factor=smooth_factor)*(1-multiplier)


def ema(prx: float, days: int):
    """
    prx: today's price
    days: number of days
    """
    today = datetime.datetime.today()
    prices = fetch_prices(today, 2*days)
    assert len(prices) == 2*days

    return _ema(prices, days)
