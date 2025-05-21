import pandas as pd
import numpy as np
from pandas import DataFrame
import config


def vectorized_backtest(df: DataFrame, symbol: str, slippage: float,
                        sl_pct: float, tp_pct: float, initial_size: float, max_size: float) -> DataFrame:
    df = df.copy()
    trades = []

    for direction in ['long', 'short']:
        entry_col = f'enter_{direction}'
        exit_col = f'exit_{direction}'
        scaling_col = f'scaling_{direction}'  # Teraz poprawnie używamy scaling_long / scaling_short

        tag_entry_col = 'enter_tag'
        tag_exit_col = 'exit_tag'

        entries = df[df[entry_col]].index
        exits = df[df[exit_col]].index

        last_exit = -1

        for entry_idx in entries:
            if entry_idx <= last_exit:
                continue

            entry_price = df.loc[entry_idx, 'close'] * (1 + slippage) if direction == 'long' else df.loc[entry_idx, 'close'] * (1 - slippage)

            # Ustal SL i TP
            if config.USE_CUSTOM_SL_TP:
                sl = df.loc[entry_idx, f'custom_sl_{direction}']
                tp = df.loc[entry_idx, f'custom_tp_{direction}']
            else:
                entry_price = df.loc[entry_idx, 'close'] * (1 + slippage if direction == 'long' else 1 - slippage)
                sl = entry_price * (1 - sl_pct if direction == 'long' else 1 + sl_pct)
                tp = entry_price * (1 + tp_pct if direction == 'long' else 1 - tp_pct)

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
                scale_signal = row.get(scaling_col, 0)

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
                # Stop Loss
                if direction == 'long' and price_low <= sl:
                    exit_price = sl
                    exit_time = time
                    exit_reason = 'custom_SL' if config.USE_CUSTOM_SL_TP else 'SL'
                    break
                elif direction == 'short' and price_high >= sl:
                    exit_price = sl
                    exit_time = time
                    exit_reason = 'custom_SL' if config.USE_CUSTOM_SL_TP else 'SL'
                    break

                # Take Profit
                if direction == 'long' and price_high >= tp:
                    exit_price = tp
                    exit_time = time
                    exit_reason = 'custom_TP' if config.USE_CUSTOM_SL_TP else 'TP'
                    break
                elif direction == 'short' and price_low <= tp:
                    exit_price = tp
                    exit_time = time
                    exit_reason = 'custom_TP' if config.USE_CUSTOM_SL_TP else 'TP'
                    break

                # Ręczne wyjście
                if i in exits:
                    exit_price = price_close * (1 - slippage) if direction == 'long' else price_close * (1 + slippage)
                    exit_time = time
                    exit_reason = 'manual_exit'
                    break

            # --- Domknięcie na końcu danych, jeśli nie było wyjścia ---
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
                'enter_tag': df.loc[entry_idx, tag_entry_col] if tag_entry_col in df.columns else '',
                'exit_tag': exit_reason,
            })

            last_exit = i

    return pd.DataFrame(trades)

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