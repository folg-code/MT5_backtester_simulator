import MetaTrader5 as mt5
import pandas as pd
import matplotlib.pyplot as plt
from strategy import MovingAverageCrossoverStrategy
from backtest import Backtest

symbol = "EURUSD"

# Inicjalizacja
if not mt5.initialize():
    print("MT5 init error:", mt5.last_error())
    quit()

mt5.symbol_select(symbol, True)

# Pobranie danych
rates = mt5.copy_rates_from_pos(symbol, mt5.TIMEFRAME_M1, 0, 100)
df = pd.DataFrame(rates)
df['time'] = pd.to_datetime(df['time'], unit='s')

# Inicjalizacja strategii
strategy = MovingAverageCrossoverStrategy(df)
strategy.populate_indicators()

# Sprawdzenie sygnału wejścia
entry_signal = strategy.populate_entry_trend()

if entry_signal:
    print("Sygnał wejścia:", entry_signal)
    # Wysyłanie zlecenia można dodać tu

# Można też testować wyjście, np. jeśli już masz otwartą pozycję 'buy':
exit_signal = strategy.populate_exit_trend(direction='buy')
if exit_signal:
    print("Sygnał wyjścia:", exit_signal)

def open_position(symbol, lot, buy=True):
    symbol_info = mt5.symbol_info(symbol)
    if symbol_info is None:
        print(f"Symbol {symbol} not found")
        return False

    if not symbol_info.visible:
        if not mt5.symbol_select(symbol, True):
            print(f"Failed to select {symbol}")
            return False

    price = mt5.symbol_info_tick(symbol).ask if buy else mt5.symbol_info_tick(symbol).bid

    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": symbol,
        "volume": lot,
        "type": mt5.ORDER_TYPE_BUY if buy else mt5.ORDER_TYPE_SELL,
        "price": price,
        "deviation": 10,
        "magic": 234000,
        "comment": "Python script open",
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_RETURN,
    }

    result = mt5.order_send(request)

    if result.retcode != mt5.TRADE_RETCODE_DONE:
        print(f"Order failed, retcode={result.retcode}")
        return False
    print(f"Order successful: {result}")
    return True


def get_data(symbol, timeframe, bars=1000):
    if not mt5.initialize():
        print("MT5 initialize failed")
        return None
    mt5.symbol_select(symbol, True)
    rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, bars)
    df = pd.DataFrame(rates)
    df['time'] = pd.to_datetime(df['time'], unit='s')
    return df

def plot_equity(trades_df):
    trades_df = trades_df.dropna(subset=['capital'])
    plt.plot(trades_df['timestamp'], trades_df['capital'])
    plt.title("Equity Curve")
    plt.xlabel("Time")
    plt.ylabel("Capital")
    plt.grid(True)
    plt.show()

if __name__ == "__main__":
    symbol = "EURUSD"
    timeframe = mt5.TIMEFRAME_M5
    df = get_data(symbol, timeframe)

    if df is not None:
        backtest = Backtest(df, MovingAverageCrossoverStrategy)
        backtest.run()
        trades_df = backtest.report()
        plot_equity(trades_df)

# Zamknięcie połączenia
mt5.shutdown()