import MetaTrader5 as mt5
import pandas as pd
import matplotlib.pyplot as plt
from strategy import MovingAverageCrossoverStrategy
from backtest import vectorized_backtest, calculate_metrics
from datetime import timedelta
from datetime import datetime
from rich.console import Console
from rich.table import Table
import config
import plotly.graph_objects as go
import numpy as np


symbol = "EURUSD"

# Inicjalizacja
if not mt5.initialize():
    print("MT5 init error:", mt5.last_error())
    quit()

mt5.symbol_select(symbol, True)



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

def compute_equity(trades_df, starting_capital =config.INITIAL_BALANCE):
    trades = trades_df.copy()
    trades['pnl'] = np.where(
        trades['direction'] == 'long',
        trades['exit_price'] - trades['entry_price'],
        trades['entry_price'] - trades['exit_price']
    )





    # PNL w punktach
    trades['pnl_points'] = trades['pnl']

    # PNL w USD: tick value * lot size * punkty / pip_size
    pip_size = 0.0001  # dla EURUSD
    tick_value = config.TICK_VALUE

    equity = starting_capital
    capital_list = []

    for i, row in trades.iterrows():
        capital_list.append(equity)
        pnl_usd = (row['pnl_points'] / pip_size) * tick_value
        equity += pnl_usd

    trades['capital'] = capital_list
    trades['pnl_usd'] = (trades['pnl_points'] / pip_size) * tick_value

    # Aktualizacja kapitału
    trades['capital'] = config.INITIAL_BALANCE + trades['pnl_usd'].cumsum()

    return trades

def plot_equity(trades_df):
    """
    Rysuje krzywą kapitału na podstawie kolumny 'capital' i 'timestamp'.
    """
    if 'capital' not in trades_df.columns:
        raise ValueError("Brakuje kolumny 'capital' – oblicz ją najpierw (np. przez compute_equity()).")

    trades_df = trades_df.dropna(subset=['capital'])

    plt.figure(figsize=(12, 5))
    plt.plot(trades_df['entry_time'], trades_df['capital'], label='Equity Curve', color='blue')
    plt.title("Equity Curve")
    plt.xlabel("Time")
    plt.ylabel("Capital")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.show()


def plot_trades_on_chart(df, trades_df):
    """
    Rysuje wykres cen z zaznaczonymi transakcjami.

    df: DataFrame z danymi OHLC (wymaga kolumn 'time' i 'close')
    trades_df: DataFrame z transakcjami (wymaga 'timestamp', 'price', 'type' ('buy'/'sell'), 'action' ('entry'/'exit'))
    """

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df['time'],
        y=df['close'],
        mode='lines',
        name='Close Price',
        line=dict(color='lightgray')
    ))

    # Flagi do kontroli unikalnych legend
    shown_legend = {
        "Entry": False,
        "custom_SL": False,
        "custom_TP": False,
        "manual_exit": False
    }

    for _, trade in trades.iterrows():
        # ENTRY
        fig.add_trace(go.Scatter(
            x=[trade['entry_time']],
            y=[trade['entry_price']],
            mode='markers',
            name='Entry',
            showlegend=not shown_legend["Entry"],
            marker=dict(color='black', symbol='circle', size=8),
            hovertemplate=
            f"Enter Tag: {trade['enter_tag']}<br>" +
            f"Exit Tag: {trade['exit_tag']}<br>" +
            f"PnL: {trade['pnl']:.5f} " +
            f"Profit: {trade['pnl_usd']:.5f} " +
            f"Price: {trade['entry_price']:.5f}<br>" +
            f"Time: {trade['entry_time']}<extra></extra>"
        ))
        shown_legend["Entry"] = True

        # EXIT
        exit_tag = trade.get('exit_tag', 'manual_exit')

        # Dobór symbolu i koloru
        if 'TP' in exit_tag:
            color = 'blue'
            symbol = 'triangle-down'
            legend_key = 'custom_TP'
        elif 'SL' in exit_tag:
            color = 'orange'
            symbol = 'triangle-up'
            legend_key = 'custom_SL'
        else:
            color = 'gray'
            symbol = 'x'
            legend_key = 'manual_exit'

        fig.add_trace(go.Scatter(
            x=[trade['exit_time']],
            y=[trade['exit_price']],
            mode='markers',
            name=legend_key,
            showlegend=not shown_legend[legend_key],
            marker=dict(color=color, symbol=symbol, size=10),
            hovertemplate=
            f"Enter Tag: {trade['enter_tag']}<br>" +
            f"Exit Tag: {trade['exit_tag']}<br>" +
            f"PnL: {trade['pnl']:.5f} " +
            f"Profit: {trade['pnl_usd']:.5f} " +
            f"Price: {trade['exit_price']:.5f}<br>" +
            f"Time: {trade['exit_time']}<extra></extra>"
        ))
        shown_legend[legend_key] = True

        # Linia łącząca
        fig.add_trace(go.Scatter(
            x=[trade['entry_time'], trade['exit_time']],
            y=[trade['entry_price'], trade['exit_price']],
            mode='lines',
            line=dict(dash='dot', color='gray'),
            showlegend=False
        ))

    fig.update_layout(
        title="Trades on Chart",
        xaxis_title="Time",
        yaxis_title="Price",
        hovermode='closest',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=700
    )

    fig.show()


def format_duration(seconds):
    return str(timedelta(seconds=round(seconds)))

def summarize_group(df, group_name="TOTAL"):
    total_trades = len(df)
    win_trades = df[df["pnl"] > 0]
    loss_trades = df[df["pnl"] < 0]
    draw_trades = df[df["pnl"] == 0]

    avg_profit_pct = df["returns"].mean() * 100
    total_profit = df["pnl"].sum()
    total_return_pct = df["returns"].sum() * 100
    avg_duration = df["duration_sec"].mean()
    win_pct = len(win_trades) / total_trades * 100 if total_trades else 0

    return [
        group_name,
        total_trades,
        round(avg_profit_pct, 2),
        round(total_profit, 3),
        round(total_return_pct, 2),
        format_duration(avg_duration),
        len(win_trades), len(draw_trades), len(loss_trades), round(win_pct, 1)
    ]

def print_group_stats(df, group_col, title):
    console = Console()  # <- dodaj instancję
    table = Table(title=title)

    headers = [group_col.capitalize(), "Trades", "Avg Profit %", "Tot Profit USDT", "Tot Profit %", "Avg Duration", "Win", "Draw", "Loss", "Win%"]
    for header in headers:
        table.add_column(header, justify="center")

    grouped = df.groupby(group_col)
    for name, group in grouped:
        stats = summarize_group(group, str(name))
        table.add_row(*map(str, stats))

    total_stats = summarize_group(df)
    table.add_row(*map(str, total_stats), style="bold")

    console.print(table)  # <- użyj instancji

def print_mixed_tag_stats(df):
    df["mix_tag"] = list(zip(df["enter_tag"], df["exit_tag"]))
    print_group_stats(df, "mix_tag", "MIXED TAG STATS")

def print_backtest_report(trades_df, processed_df):
    trades_df = trades_df.copy()

    trades_df["returns"] = trades_df["pnl"] / trades_df["entry_price"]
    trades_df["duration_sec"] = (trades_df["exit_time"] - trades_df["entry_time"]).dt.total_seconds()

    start = df['time'].min()
    end = df['time'].max()

    start_balance = trades_df['capital'].iloc[0]
    end_balance = trades_df['capital'].iloc[-1]

    print_group_stats(trades_df, "symbol", f"BACKTESTING REPORT {start} to {end} ")
    print_group_stats(trades_df, "enter_tag", "ENTER TAG STATS")
    print_group_stats(trades_df, "exit_tag", "EXIT REASON STATS")
    print(f"starting balance {start_balance} ending balance{end_balance}")

# Upewnij się, że trades_df zawiera kolumny: ['symbol', 'enter_tag', 'exit_tag', 'entry_time', 'exit_time', 'entry_price', 'exit_price', 'pnl']



if __name__ == "__main__":
    symbol = config.SYMBOL
    timeframe = config.TIMEFRAME_MAP[config.TIMEFRAME]
    start_date = datetime.strptime(config.TIMERANGE['start'], "%Y-%m-%d")
    end_date = datetime.strptime(config.TIMERANGE['end'], "%Y-%m-%d")
    df = get_data(symbol, timeframe, start_date, end_date)
    strategy = MovingAverageCrossoverStrategy(df)
    processed_df = strategy.run()



    print("Rozpoczynam Backtest")
    trades = vectorized_backtest(processed_df, config.SYMBOL, config.SLIPPAGE, config.SL_PCT, config.TP_PCT, config.INITIAL_SIZE, config.MAX_SIZE)
    trades = compute_equity(trades)
    plot_equity(trades)
    plot_trades_on_chart(processed_df, trades)
    print_backtest_report(trades, processed_df)
    if config.SAVE_TRADES_CSV:
        trades.to_csv(f"trades_{config.SYMBOL}_{config.TIMEFRAME}.csv", index=False)

# Zamknięcie połączenia
mt5.shutdown()