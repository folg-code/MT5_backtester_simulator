import MetaTrader5 as mt5
import backtest
import plot
import raport
from datetime import datetime
import config
from utils.data_loader import get_data
from utils.strategy_loader import load_strategy
import pandas as pd
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
import traceback


def prepare_data_for_all_symbols():
    data_dict = {}
    for symbol in config.SYMBOLS:
        df = get_data(symbol, config.TIMEFRAME_MAP[config.TIMEFRAME],
                      datetime.strptime(config.TIMERANGE['start'], "%Y-%m-%d"),
                      datetime.strptime(config.TIMERANGE['end'], "%Y-%m-%d"))
        data_dict[symbol] = df
    return data_dict

def run_strategy_single(symbol_df_tuple):
    symbol, df = symbol_df_tuple
    strategy = load_strategy(config.strategy, df, symbol, 600)
    df_bt = strategy.run()
    df_bt["symbol"] = symbol
    print(df_bt)
    return df_bt, strategy
# Inicjalizacja
if not mt5.initialize():
    print("MT5 init error:", mt5.last_error())
    quit()




if __name__ == "__main__":
    print("✅ Pobieranie danych z MT5...")
    all_data = prepare_data_for_all_symbols()
    mt5.shutdown()  # Zamyka MT5, żeby uniknąć problemów przy multiprocessing

    print("✅ Uruchamianie strategii równolegle...")
    all_signals = []
    all_strategies = []

    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = [executor.submit(run_strategy_single, (symbol, df)) for symbol, df in all_data.items()]
        
        for future in as_completed(futures):
            try:
                df_bt, strategy = future.result()
                all_signals.append(df_bt)

                all_strategies.append(strategy)
                
            except Exception as e:
                print(f"❌ Error: {e}")

    df_all_signals = pd.concat(all_signals).sort_values(by=["time", "symbol"])

    print("✅ Uruchamianie backtestu...")
    trades_all = backtest.vectorized_backtest(
        df_all_signals,
        None,
        config.SLIPPAGE,
        config.SL_PCT,
        config.TP_PCT,
        config.INITIAL_SIZE,
        config.MAX_SIZE,
        config.SINGLE_POSIOTION_MODE,
    )

    if not trades_all.empty:
        trades_all = raport.compute_equity(trades_all)
        plot.plot_equity(trades_all)
        raport.print_backtest_report(trades_all, df_all_signals)

        plots_folder = "trades_plots/plots"
        os.makedirs(plots_folder, exist_ok=True)

        for strategy in all_strategies:
            symbol = strategy.symbol
            trades_symbol = trades_all[trades_all['symbol'] == symbol]

            if not trades_symbol.empty:
                plot_path = f"{plots_folder}/{symbol}"
                plot.plot_trades_with_indicators(
                    df=strategy.df_plot,
                    trades=trades_symbol,
                    bullish_zones=strategy.get_bullish_zones(),
                    bearish_zones=strategy.get_bearish_zones(),
                    extra_series=strategy.get_extra_values_to_plot(),
                    bool_series=strategy.bool_series(),
                    save_path=plot_path
                )

        if config.SAVE_TRADES_CSV:
            output_folder = "trades_plots/trades"
            os.makedirs(output_folder, exist_ok=True)
            trades_all.to_csv(os.path.join(output_folder, "trades_ALL.csv"), index=False)

    print("🏁 Zakończono.")