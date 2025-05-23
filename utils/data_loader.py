import MetaTrader5 as mt5
import pandas as pd

def get_data(symbol, timeframe, start_date, end_date):
    if not mt5.initialize():
        raise RuntimeError(f"MT5 initialize failed: {mt5.last_error()}")

    if not mt5.symbol_select(symbol, True):
        raise RuntimeError(f"Symbol select failed: {symbol}, Error: {mt5.last_error()}")

    rates = mt5.copy_rates_range(symbol, timeframe, start_date, end_date)
    if rates is None or len(rates) == 0:
        raise ValueError("Brak danych dla podanego zakresu dat.")

    df = pd.DataFrame(rates)
    df['time'] = pd.to_datetime(df['time'], unit='s')
    return df