import MetaTrader5 as mt5


# ====== Strategia =====
strategy = "POI"

# === Parametry rynku ===
SYMBOLS = ['USDJPY', 'EURUSD','GBPUSD']
TIMEFRAME = 'M5'
TIMERANGE = {
    'start': '2025-05-11',
    'end': '2025-05-31'
}
# === Kapitał początkowy ===
INITIAL_BALANCE = 100_000.0  # USD
# === Parametry strategii ===
SLIPPAGE = 0.000
SL_PCT = 0.0015     # SL = 0.1%
TP_PCT = 0.003    # TP = 0.2%
INITIAL_SIZE = 1.0 * INITIAL_BALANCE
MAX_SIZE = 3.0

# Czy używać niestandardowych SL/TP (np. na bazie ATR)
USE_CUSTOM_SL_TP = False
# === Inne opcje ===
PLOT_TRADES = True
PLOT_EACH_SYMBOL = True
SAVE_TRADES_CSV = True

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
