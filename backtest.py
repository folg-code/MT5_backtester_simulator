import pandas as pd
import numpy as np
from pandas import DataFrame
import config
import matplotlib.pyplot as plt
import plotly.graph_objects as go
from rich.console import Console
from rich.table import Table
from datetime import timedelta
from datetime import datetime

def vectorized_backtest(df: DataFrame, symbol: str, slippage: float,
                        sl_pct: float, tp_pct: float, initial_size: float, max_size: float) -> DataFrame:
    df = df.copy()
    trades = []

    for direction in ['long', 'short']:
        tag_entry_col = 'enter_tag'
        entries = df[df['signal'].apply(lambda x: isinstance(x, tuple) and x[0] == 'enter' and x[1] == direction)].index
        exits = df[df['signal'].apply(lambda x: isinstance(x, tuple) and x[0] == 'exit' and x[1] == direction)].index

        last_exit = -1

        for entry_idx in entries:
            if entry_idx <= last_exit:
                continue

            signal = df.loc[entry_idx, 'signal']

            # Wstępne wartości
            if len(signal) >= 5:
                _, _, enter_tag, custom_sl, custom_tp = signal[:5]
                sl_exit_tag = signal[5] if len(signal) > 5 else "custom_SL"
                tp_exit_tag = signal[6] if len(signal) > 6 else "custom_TP"
            else:
                _, _, = signal
                enter_tag = ''
                custom_sl = None
                custom_tp = None
                sl_exit_tag = "SL"
                tp_exit_tag = "TP"

            entry_price = df.loc[entry_idx, 'close'] * (1 + slippage) if direction == 'long' else df.loc[entry_idx, 'close'] * (1 - slippage)

            if config.USE_CUSTOM_SL_TP and custom_sl is not None and custom_tp is not None:
                sl = custom_sl
                tp = custom_tp
            else:
                sl = entry_price * (1 - sl_pct if direction == 'long' else 1 + sl_pct)
                tp = entry_price * (1 + tp_pct if direction == 'long' else 1 - tp_pct)
                sl_exit_tag = "SL"
                tp_exit_tag = "TP"

            position_size = initial_size
            avg_entry_price = entry_price
            entry_time = df.loc[entry_idx, 'time']
            exit_price = None
            exit_time = None
            exit_reason = None
            pnl_total = 0

            for i in range(entry_idx + 1, len(df)):
                row = df.iloc[i]
                price_close = row['close']
                price_high = row['high']
                price_low = row['low']
                time = row['time']

                scale = row.get('scaling', None)
                scale_signal = 0
                if isinstance(scale, tuple) and scale[0] == "scale" and scale[1] == direction:
                    scale_signal = scale[2]

                # --- Skalowanie: ZWIĘKSZ ---
                if scale_signal == 1 and position_size < max_size:
                    add_size = min(initial_size, max_size - position_size)
                    if add_size > 0:
                        add_price = price_close * (1 + slippage) if direction == 'long' else price_close * (1 - slippage)
                        avg_entry_price = (avg_entry_price * position_size + add_price * add_size) / (position_size + add_size)
                        position_size += add_size
                        sl = avg_entry_price * (1 - sl_pct) if direction == 'long' else avg_entry_price * (1 + sl_pct)
                        tp = avg_entry_price * (1 + tp_pct) if direction == 'long' else avg_entry_price * (1 - tp_pct)

                # --- Skalowanie: ZMNIEJSZ ---
                if scale_signal == -1 and position_size > initial_size:
                    reduce_size = min(initial_size, position_size)
                    exit_price_part = price_close * (1 - slippage) if direction == 'long' else price_close * (1 + slippage)
                    pnl_part = (exit_price_part - avg_entry_price) * reduce_size if direction == 'long' else (avg_entry_price - exit_price_part) * reduce_size
                    position_size -= reduce_size
                    pnl_total += pnl_part

                # --- Warunki wyjścia ---
                if direction == 'long' and price_low <= sl:
                    exit_price = sl
                    exit_time = time
                    exit_reason = sl_exit_tag
                    break
                elif direction == 'short' and price_high >= sl:
                    exit_price = sl
                    exit_time = time
                    exit_reason = sl_exit_tag
                    break

                if direction == 'long' and price_high >= tp:
                    exit_price = tp
                    exit_time = time
                    exit_reason = tp_exit_tag
                    break
                elif direction == 'short' and price_low <= tp:
                    exit_price = tp
                    exit_time = time
                    exit_reason = tp_exit_tag
                    break

                if i in exits:
                    exit_price = price_close * (1 - slippage) if direction == 'long' else price_close * (1 + slippage)
                    exit_time = time
                    exit_reason = 'manual_exit'
                    break

            if exit_price is None:
                exit_price = df.iloc[-1]['close'] * (1 - slippage) if direction == 'long' else df.iloc[-1]['close'] * (1 + slippage)
                exit_time = df.iloc[-1]['time']
                exit_reason = 'end_of_data'

            pnl_final = (exit_price - avg_entry_price) * position_size if direction == 'long' else (avg_entry_price - exit_price) * position_size
            pnl_total += pnl_final

            trades.append({
                'symbol': symbol,
                'direction': direction,
                'entry_time': entry_time,
                'exit_time': exit_time,
                'entry_price': avg_entry_price,
                'exit_price': exit_price,
                'position_size': position_size,
                'pnl': pnl_total,
                'exit_reason': exit_reason,
                'enter_tag': enter_tag,
                'exit_tag': exit_reason,
            })

            last_exit = i

    return pd.DataFrame(trades)

def compute_equity(trades_df, starting_capital =config.INITIAL_BALANCE):
    trades = trades_df.copy()
    trades['pnl'] = np.where(
        trades['direction'] == 'long',
        trades['exit_price'] - trades['entry_price'],
        trades['entry_price'] - trades['exit_price']
    )

    trades['pnl_points'] = trades['pnl']

    # PnL USD na podstawie wielkości pozycji w dolarach
    trades['pnl_usd'] = trades['pnl_points'] * (trades['position_size'] / trades['entry_price'])

    # Aktualizacja kapitału
    trades['capital'] = starting_capital + trades['pnl_usd'].cumsum()

    print(trades)

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


def plot_trades_on_chart(df, trades):
    """
    Rysuje wykres cen z zaznaczonymi transakcjami.

    df: DataFrame z danymi OHLC (wymaga kolumn 'time' i 'close')
    trades_df: DataFrame z transakcjami (wymaga 'timestamp', 'price', 'type' ('buy'/'sell'), 'action' ('entry'/'exit'))
    """

    fig = go.Figure()

    fig.add_trace(go.Candlestick(
        x=df['time'],
        open=df['open'],
        high=df['high'],
        low=df['low'],
        close=df['close'],
        name='Candles'
    ))

    # Dodaj linię ASIA HIGH
    fig.add_trace(go.Scatter(
        x=df['time'],
        y=df['asia_high'],
        mode='lines',
        line=dict(color='green', dash='dash'),
        name='Asia High'
    ))

    # Dodaj linię ASIA LOW
    fig.add_trace(go.Scatter(
        x=df['time'],
        y=df['asia_low'],
        mode='lines',
        line=dict(color='red', dash='dash'),
        name='Asia Low'
    ))

    # Linie pionowe o 9:00 i 11:00
    for current_date in df['time'].dt.date.unique():
        for hour in [9, 11]:
            time_point = pd.Timestamp(f"{current_date} {hour:02d}:00:00")
            if time_point in df['time'].values:
                fig.add_shape(
                    type='line',
                    x0=time_point, x1=time_point,
                    y0=df['low'].min(), y1=df['high'].max(),
                    line=dict(color="blue", dash="dash"),
                    xref='x', yref='y'
                )

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
    total_profit = df["pnl_usd"].sum()
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

def print_final_raport(df, group_col, title):
    console = Console()  # <- dodaj instancję
    table = Table(title=title)

    headers = [group_col.capitalize(), "Trades", "Avg Profit %", "Tot Profit USDT", "Tot Profit %", "Avg Duration", "Win", "Draw", "Loss", "Win%","Max Balance","Min Balance", "Drowdown" ]
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

    start = processed_df['time'].min()
    end = processed_df['time'].max()

    start_balance = trades_df['capital'].iloc[0]
    end_balance = trades_df['capital'].iloc[-1]


    print_group_stats(trades_df, "enter_tag", "ENTER TAG STATS")
    print_group_stats(trades_df, "exit_tag", "EXIT REASON STATS")
    print_final_raport(trades_df, "symbol", f"BACKTESTING REPORT {start} to {end} ")
    print(f"starting balance {start_balance} ending balance{end_balance}")

def calculate_metrics(trades_df):
    trades_df['cum_pnl'] = trades_df['pnl'].cumsum()
    trades_df['returns'] = trades_df['pnl'] / trades_df['entry_price']
    sharpe = trades_df['returns'].mean() / trades_df['returns'].std(ddof=0) * np.sqrt(252) if len(trades_df) > 1 else 0
    drawdown = trades_df['cum_pnl'].cummax() - trades_df['cum_pnl']
    max_drawdown = drawdown.max()
    return {
        'total_trades': len(trades_df),
        'total_pnl': trades_df['pnl'].sum(),
        'sharpe': round(sharpe, 2),
        'max_drawdown': round(max_drawdown, 2)
    }


def print_trade_report(df, leverage=30):
    console = Console()
    table = Table(title="Trade Report")

    columns = ["symbol", "entry_time", "exit_time", "duration", "pnl_usd", "capital"]
    for col in columns:
        table.add_column(col.capitalize(), justify="center")

    for _, row in df.iterrows():
        entry_time = pd.to_datetime(row["entry_time"])
        exit_time = pd.to_datetime(row["exit_time"])
        duration = str(exit_time - entry_time)
        profit_usd = round(row["pnl_usd"], 2)
        balance = round(row["capital"], 2)

        table.add_row(
            row["symbol"],
            str(entry_time),
            str(exit_time),
            duration,
            f"${profit_usd}",
            f"${balance}"
        )

    console.print(table)