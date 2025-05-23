import MetaTrader5 as mt5
import strategies
import backtest
from datetime import datetime
import config
from utils.data_loader import get_data
from utils.strategy_loader import load_strategy

# Inicjalizacja
if not mt5.initialize():
    print("MT5 init error:", mt5.last_error())
    quit()

mt5.symbol_select(config.SYMBOL, True)



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






# Upewnij się, że trades_df zawiera kolumny: ['symbol', 'enter_tag', 'exit_tag', 'entry_time', 'exit_time', 'entry_price', 'exit_price', 'pnl']



if __name__ == "__main__":






    df = get_data(config.SYMBOL, config.TIMEFRAME_MAP[config.TIMEFRAME], datetime.strptime(config.TIMERANGE['start'], "%Y-%m-%d"), datetime.strptime(config.TIMERANGE['end'], "%Y-%m-%d"))
    strategy = load_strategy(config.strategy, df)
    df_bt = strategy.run()
    df_plot = strategy.df_plot
    trades = backtest.vectorized_backtest(df_bt, config.SYMBOL, config.SLIPPAGE, config.SL_PCT, config.TP_PCT, config.INITIAL_SIZE, config.MAX_SIZE)
    backtest.print_trade_report(trades)
    if not trades.empty:
        trades = backtest.compute_equity(trades)
        backtest.plot_equity(trades)
        backtest.print_backtest_report(trades, df_bt)
        backtest.plot_trades_on_chart(df_plot, trades)

    else:
        print("⚠️ Brak wygenerowanych tradów – nic do raportu.")
        backtest.plot_trades_on_chart(df_plot, trades)


    if config.SAVE_TRADES_CSV:
        trades.to_csv(f"trades_{config.SYMBOL}_{config.TIMEFRAME}.csv", index=False)



# Zamknięcie połączenia
mt5.shutdown()