import MetaTrader5 as mt5


# ====== Strategia =====
strategy = "POI"

# === Parametry rynku ===
SYMBOLS = [ 'EURUSD', 'USDJPY', 'XAUUSD', 'AUDNZD']
TIMEFRAME = 'M5'
TIMERANGE = {
    'start': '2025-02-12',
    'end': '2025-06-12'
}
# === Kapitał początkowy ===
INITIAL_BALANCE = 100_000.0  # USD
# === Parametry strategii ===
SLIPPAGE = 0.000
SL_PCT = 0.005    # SL = 0.1%
TP_PCT = 0.025    # TP = 0.2%s
INITIAL_SIZE = 1.0 * INITIAL_BALANCE
MAX_SIZE = 3.0
SINGLE_POSIOTION_MODE = True

# Czy używać niestandardowych SL/TP (np. na bazie ATR)
USE_CUSTOM_SL_TP = True
# === Inne opcje ===
PLOT_TRADES = True
PLOT_EACH_SYMBOL = True
SAVE_TRADES_CSV = True

##long ={"PA__MAD_" : "PA__C__MAD_", "PA__PA_H1__C__MAD_"}
#not_long = {"PA_H1_MAU"}

TICK_VALUE = 10  # Dla EURUSD 1 lot = $10/pips
TIMEFRAME_MAP = {
    "M1": mt5.TIMEFRAME_M1,
    "M5": mt5.TIMEFRAME_M5,
    "M15": mt5.TIMEFRAME_M15,
    "M30": mt5.TIMEFRAME_M30,
    "H1": mt5.TIMEFRAME_H1,
    "H4": mt5.TIMEFRAME_H4,
    "D1": mt5.TIMEFRAME_D1,
    "W1": mt5.TIMEFRAME_W1,
    "MN1": mt5.TIMEFRAME_MN1
}
