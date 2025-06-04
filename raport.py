import config
import numpy as np
from datetime import timedelta
from rich.console import Console
from rich.table import Table


def compute_equity(trades_df, starting_capital=config.INITIAL_BALANCE):
    trades = trades_df.copy()
    trades['pnl'] = np.where(
        trades['direction'] == 'long',
        trades['exit_price'] - trades['entry_price'],
        trades['entry_price'] - trades['exit_price']
    )
    trades['pnl_points'] = trades['pnl']
    trades['pnl_usd'] = trades['pnl_points'] * (trades['position_size'] / trades['entry_price'])
    trades['capital'] = starting_capital + trades['pnl_usd'].cumsum()
    return trades

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

    if 'capital' in df.columns and df['capital'].notna().any():
        max_capital = df['capital'].max()
        min_capital = df['capital'].min()
        drawdown = max_capital - min_capital
    else:
        max_capital = min_capital = drawdown = ''

    return [
        group_name,
        total_trades,
        round(avg_profit_pct, 2),
        round(total_profit, 3),
        round(total_return_pct, 2),
        format_duration(avg_duration),
        len(win_trades),
        len(draw_trades),
        len(loss_trades),
        round(win_pct, 1),
        round(max_capital, 2) if max_capital != '' else '',
        round(min_capital, 2) if min_capital != '' else '',
        round(drawdown, 2) if drawdown != '' else '',
    ]

def print_group_stats(df, group_col, title):
    console = Console()
    table = Table(title=title)

    headers = [group_col.capitalize(), "Trades", "Avg Profit %", "Tot Profit USD", "Tot Profit %", "Avg Duration", "Win", "Draw", "Loss", "Win%"]
    for header in headers:
        table.add_column(header, justify="center")

    grouped = df.groupby(group_col)
    for name, group in grouped:
        stats = summarize_group(group, str(name))
        table.add_row(*map(str, stats))

    total_stats = summarize_group(df)
    table.add_row(*map(str, total_stats), style="bold")

    console.print(table)

def print_final_raport(df, group_col, title):
    console = Console()
    table = Table(title=title)

    headers = [group_col.capitalize(), "Trades", "Avg Profit %", "Tot Profit USD", "Tot Profit %", "Avg Duration", "Win", "Draw", "Loss", "Win%", "Max Balance", "Min Balance", "Drowdown"]
    for header in headers:
        table.add_column(header, justify="center")

    grouped = df.groupby(group_col)
    for name, group in grouped:
        stats = summarize_group(group, str(name))
        table.add_row(*map(str, stats))

    total_stats = summarize_group(df)
    table.add_row(*map(str, total_stats), style="bold")
    console.print(table)

def print_summary_metrics(df, initial_balance=3000):
    console = Console()
    df = df.sort_values("exit_time").copy()
    df["balance"] = df["pnl_usd"].cumsum() + initial_balance


    start_date = df["exit_time"].min()
    end_date = df["exit_time"].max()
    n_days = (end_date - start_date).days or 1
    total_trades = len(df)
    final_balance = df["balance"].iloc[-1]
    absolute_profit = final_balance - initial_balance
    total_profit_pct = (absolute_profit / initial_balance) * 100
    cagr = ((final_balance / initial_balance) ** (365 / n_days) - 1) * 100

    daily_returns = df.groupby(df["exit_time"].dt.date)["returns"].sum()
    sharpe = sortino = 0
    if not daily_returns.empty:
        std_daily = daily_returns.std()
        mean_daily = daily_returns.mean()
        sharpe = mean_daily / std_daily * np.sqrt(365) if std_daily else 0
        sortino = mean_daily / daily_returns[daily_returns < 0].std() * np.sqrt(365) if (daily_returns < 0).any() else 0

    max_balance = df["balance"].max()
    min_balance = df["balance"].min()
    drawdown_abs = max_balance - min_balance
    drawdown_pct = drawdown_abs / max_balance * 100 if max_balance else 0
    calmar = cagr / drawdown_pct if drawdown_pct else 0

    avg_stake = df["position_size"].mean() if "position_size" in df.columns else np.nan
    total_volume = df["position_size"].sum() if "position_size" in df.columns else np.nan
    best_trade = df[df['returns'] == df['returns'].max()].iloc[0]
    worst_trade = df[df['returns'] == df['returns'].min()].iloc[0]

    # Największa dzienna strata w procentach
    worst_day_loss_pct = daily_returns.min() * 100


    table = Table(title="SUMMARY METRICS")
    table.add_column("Metric", justify="left")
    table.add_column("Value", justify="right")

    table.add_row("Backtesting from", str(start_date))
    table.add_row("Backtesting to", str(end_date))
    table.add_row("Total/Daily Avg Trades", f"{total_trades} / {round(total_trades / n_days, 2)}")
    table.add_row("Starting balance", f"{round(initial_balance,0)} USD")
    table.add_row("Final balance", f"{final_balance:.2f} USD")
    table.add_row("Absolute profit", f"{absolute_profit:.2f} USD")
    table.add_row("Total profit %", f"{total_profit_pct:.2f}%")
    table.add_row("CAGR %", f"{cagr:.2f}%")
    table.add_row("Sortino", f"{sortino:.2f}")
    table.add_row("Sharpe", f"{sharpe:.2f}")
    table.add_row("Calmar", f"{calmar:.2f}")
    table.add_row("Profit factor", f"{-df[df['pnl_usd'] > 0]['pnl_usd'].sum() / df[df['pnl_usd'] < 0]['pnl_usd'].sum():.2f}" if (df["pnl_usd"] < 0).any() else "∞")
    table.add_row("Expectancy (Ratio)", f"{df['pnl_usd'].mean():.2f} ({(df['pnl_usd'] > 0).mean():.2f})")
    table.add_row("Avg. daily profit %", f"{(df['returns'].sum() / n_days * 100):.2f}%")
    table.add_row("Avg. stake amount", f"{avg_stake:.2f} USD" if not np.isnan(avg_stake) else "-")
    table.add_row("Total trade volume", f"{total_volume:.2f} USD" if not np.isnan(total_volume) else "-")
    table.add_row("Best trade", f"{str(best_trade.get('pair', '-'))}: {best_trade['returns']*100:.2f}%")
    table.add_row("Worst trade", f"{worst_trade.get('pair', '-')}: {worst_trade['returns']*100:.2f}%")
    table.add_row("Min balance", f"{min_balance:.2f} USD")
    table.add_row("Max balance", f"{max_balance:.2f} USD")
    table.add_row("Absolute Drawdown", f"{drawdown_abs:.2f} USD")
    table.add_row("Max % underwater", f"{drawdown_pct:.2f}%")
    table.add_row("Max daily loss %", f"{worst_day_loss_pct:.2f}%")

    console.print(table)

def print_backtest_report(trades_df, processed_df):
    trades_df = trades_df.copy()
    trades_df["returns"] = trades_df["pnl"] / trades_df["entry_price"]
    trades_df["duration_sec"] = (trades_df["exit_time"] - trades_df["entry_time"]).dt.total_seconds()

    print_group_stats(trades_df, "enter_tag", "ENTER TAG STATS")
    print_group_stats(trades_df, "exit_tag", "EXIT REASON STATS")
    print_final_raport(trades_df, "symbol", f"BACKTESTING REPORT {processed_df['time'].min()} to {processed_df['time'].max()}")

    print_summary_metrics(trades_df, initial_balance=trades_df['capital'].iloc[0])