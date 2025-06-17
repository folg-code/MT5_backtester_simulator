import MetaTrader5 as mt5
import pandas as pd
import time

def get_data(symbol, timeframe, start_date, end_date):
    if not mt5.initialize():
        raise RuntimeError(f"MT5 initialize failed: {mt5.last_error()}")

    if not mt5.symbol_select(symbol, True):
        mt5.symbol_select(symbol, False)  # Deselect
        time.sleep(0.5)
        if not mt5.symbol_select(symbol, True):
            raise RuntimeError(f"Still can't select symbol: {symbol}")

    rates = mt5.copy_rates_range(symbol, timeframe, start_date, end_date)
    if rates is None or len(rates) == 0:
        raise ValueError("Brak danych dla podanego zakresu dat.")

    df = pd.DataFrame(rates)
    df['time'] = pd.to_datetime(df['time'], unit='s').dt.tz_localize('UTC')
    return df