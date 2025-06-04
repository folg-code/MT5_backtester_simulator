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

# Inicjalizacja
if not mt5.initialize():
    print("MT5 init error:", mt5.last_error())
    quit()



# Upewnij się, że trades_df zawiera kolumny: ['symbol', 'enter_tag', 'exit_tag', 'entry_time', 'exit_time', 'entry_price', 'exit_price', 'pnl']



if __name__ == "__main__":
    all_signals = []
    all_strategies = []

    for symbol in config.SYMBOLS:
        df = get_data(symbol, config.TIMEFRAME_MAP[config.TIMEFRAME],
                      datetime.strptime(config.TIMERANGE['start'], "%Y-%m-%d"),
                      datetime.strptime(config.TIMERANGE['end'], "%Y-%m-%d"))

        strategy = load_strategy(config.strategy, df, symbol, 600)
        df_bt = strategy.run()
        df_bt["symbol"] = symbol
        all_signals.append(df_bt)
        all_strategies.append(strategy)  # zapamiętujemy strategy do późniejszego rysowania

    df_all_signals = pd.concat(all_signals).sort_values(by=["time", "symbol"])

    trades_all = backtest.vectorized_backtest(
        df_all_signals,
        None,  # symbol=None --> wielosymbolowy backtest
        config.SLIPPAGE,
        config.SL_PCT,
        config.TP_PCT,
        config.INITIAL_SIZE,
        config.MAX_SIZE
    )

    pd.set_option('display.max_rows', None)  # Pokaż wszystkie wiersze
    pd.set_option('display.max_columns', None)  # Pokaż wszystkie kolumny
    pd.set_option('display.width', None)  # Dopasuj szerokość do konsoli (bez łamania linii)

    if not trades_all.empty:
        trades_all = raport.compute_equity(trades_all)
        plot.plot_equity(trades_all)
        raport.print_backtest_report(trades_all, df_all_signals)

        # Utwórz folder na wykresy jeśli nie istnieje
        plots_folder = "trades_plots/plots"
        os.makedirs(plots_folder, exist_ok=True)

        for strategy in all_strategies:
            symbol = strategy.symbol
            trades_symbol = trades_all[trades_all['symbol'] == symbol]

            if not trades_symbol.empty:
                plot_path = f"trades_plots/plots/{symbol}"
                plot.plot_trades_with_indicators(
                    df=strategy.df_plot,
                    trades=trades_symbol,
                    bullish_zones=strategy.get_bullish_zones(),
                    bearish_zones=strategy.get_bearish_zones(),
                    extra_series=strategy.get_extra_values_to_plot(),
                    save_path=plot_path
                )

        if config.SAVE_TRADES_CSV:
            output_folder = "trades_plots/trades"
            os.makedirs(output_folder, exist_ok=True)
            trades_all.to_csv(os.path.join(output_folder, "trades_ALL.csv"), index=False)

    mt5.shutdown()