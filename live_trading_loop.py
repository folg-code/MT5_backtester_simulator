import MetaTrader5 as mt5
import time
from datetime import datetime, timedelta
import pandas as pd
import config
from utils.data_loader import get_data
from utils.strategy_loader import load_strategy


def initialize_mt5():
    if not mt5.initialize():
        print("❌ MT5 init error:", mt5.last_error())
        return False
    print("✅ MetaTrader5 zainicjalizowany.")
    return True


def shutdown_mt5():
    mt5.shutdown()
    print("📴 Połączenie z MetaTrader5 zostało zamknięte.")


def prepare_initial_data():
    data = {}
    for symbol in config.SYMBOLS:
        df = get_data(
            symbol,
            config.TIMEFRAME_MAP[config.TIMEFRAME],
            datetime.strptime(config.TIMERANGE['start'], "%Y-%m-%d"),
            datetime.strptime(config.TIMERANGE['end'], "%Y-%m-%d")
        )
        if df is not None and not df.empty:
            data[symbol] = df
        else:
            print(f"⚠️ Brak danych początkowych dla: {symbol}")
    return data


def fetch_and_update_data(strategy, symbol):
    df = strategy.df
    last_time = df['time'].max()

    try:
        df_new = get_data(
            symbol,
            config.TIMEFRAME_MAP[config.TIMEFRAME],
            last_time + timedelta(seconds=1),
            datetime.now()
        )
        if df_new is not None and not df_new.empty:
            df_updated = pd.concat([df, df_new]).drop_duplicates(subset=['time']).reset_index(drop=True)
            strategy.df = df_updated
            print(f"🔄 Zaktualizowano dane dla {symbol} ({len(df_new)} nowych świec)")
            return True
        else:
            print(f"⏳ Brak nowych danych dla {symbol}")
            return False
    except Exception as e:
        print(f"❌ Błąd podczas pobierania danych dla {symbol}: {e}")
        return False


def run_live_loop():
    if not initialize_mt5():
        return

    all_data = prepare_initial_data()
    strategies = {
        symbol: load_strategy(config.strategy, df, symbol, config.TIMEFRAME_MAP[config.TIMEFRAME])
        for symbol, df in all_data.items()
    }

    try:
        print("🚀 Rozpoczęcie pętli live tradingowej...")
        while True:
            loop_start = datetime.now()
            print(f"\n🕒 Cykl rozpoczęty: {loop_start.strftime('%Y-%m-%d %H:%M:%S')}")

            for symbol, strategy in strategies.items():
                updated = fetch_and_update_data(strategy, symbol)
                if updated:
                    try:
                        strategy.run_live()
                    except Exception as e:
                        print(f"❌ Błąd w run_live() strategii {symbol}: {e}")

            print(f"⏸ Oczekiwanie {config.LIVE_LOOP_DELAY} sekund do kolejnego cyklu...")
            time.sleep(config.LIVE_LOOP_DELAY)

    except KeyboardInterrupt:
        print("\n🛑 Live trading przerwany przez użytkownika.")

    finally:
        shutdown_mt5()


if __name__ == "__main__":
    run_live_loop()