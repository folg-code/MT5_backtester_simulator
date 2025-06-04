import pandas as pd
from pandas import DataFrame
import config


def vectorized_backtest(df: pd.DataFrame, symbol: str, slippage: float,
                        sl_pct: float, tp_pct: float, initial_size: float, max_size: float) -> pd.DataFrame:
    # Jeśli symbol jest podany, robimy tylko dla jednego symbolu (oryginalne działanie)
    if symbol is not None:
        return _vectorized_backtest_single_symbol(df, symbol, slippage, sl_pct, tp_pct, initial_size, max_size)

    # Jeśli symbol == None, oczekujemy kolumny 'symbol' w df i robimy backtest dla każdego osobno
    all_trades = []
    for sym, group_df in df.groupby('symbol'):
        trades = _vectorized_backtest_single_symbol(group_df, sym, slippage, sl_pct, tp_pct, initial_size, max_size)
        all_trades.append(trades)
    if all_trades:
        return pd.concat(all_trades).sort_values(by='exit_time')
    else:
        return pd.DataFrame()  # pusty df jeśli brak danych


def _vectorized_backtest_single_symbol(df: pd.DataFrame, symbol: str, slippage: float,
                                       sl_pct: float, tp_pct: float, initial_size: float,
                                       max_size: float) -> pd.DataFrame:
    df = df.copy()
    trades = []

    for direction in ['long', 'short']:
        entries = df[df['signal'].apply(lambda x: isinstance(x, tuple) and x[0] == 'enter' and x[1] == direction)].index
        exits = df[df['signal'].apply(lambda x: isinstance(x, tuple) and x[0] == 'exit' and x[1] == direction)].index

        last_exit = -1

        for entry_idx in entries:
            if entry_idx <= last_exit:
                continue

            signal = df.loc[entry_idx, 'signal']

            if len(signal) >= 5:
                _, _, enter_tag, custom_sl, custom_tp = signal[:5]
                sl_exit_tag = signal[5] if len(signal) > 5 else "custom_SL"
                tp_exit_tag = signal[6] if len(signal) > 6 else "custom_TP"
            else:
                _, _, enter_tag = signal if len(signal) >= 3 else ('', '', '')
                custom_sl = None
                custom_tp = None
                sl_exit_tag = "SL"
                tp_exit_tag = "TP"

            entry_price = df.loc[entry_idx, 'close'] * (1 + slippage) if direction == 'long' else df.loc[
                                                                                                      entry_idx, 'close'] * (
                                                                                                              1 - slippage)

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

                if scale_signal == 1 and position_size < max_size:
                    add_size = min(initial_size, max_size - position_size)
                    if add_size > 0:
                        add_price = price_close * (1 + slippage) if direction == 'long' else price_close * (
                                    1 - slippage)
                        avg_entry_price = (avg_entry_price * position_size + add_price * add_size) / (
                                    position_size + add_size)
                        position_size += add_size
                        sl = avg_entry_price * (1 - sl_pct) if direction == 'long' else avg_entry_price * (1 + sl_pct)
                        tp = avg_entry_price * (1 + tp_pct) if direction == 'long' else avg_entry_price * (1 - tp_pct)

                if scale_signal == -1 and position_size > initial_size:
                    reduce_size = min(initial_size, position_size)
                    exit_price_part = price_close * (1 - slippage) if direction == 'long' else price_close * (
                                1 + slippage)
                    pnl_part = (exit_price_part - avg_entry_price) * reduce_size if direction == 'long' else (
                                                                                                                         avg_entry_price - exit_price_part) * reduce_size
                    position_size -= reduce_size
                    pnl_total += pnl_part

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
                exit_price = df.iloc[-1]['close'] * (1 - slippage) if direction == 'long' else df.iloc[-1]['close'] * (
                            1 + slippage)
                exit_time = df.iloc[-1]['time']
                exit_reason = 'end_of_data'

            pnl_final = (exit_price - avg_entry_price) * position_size if direction == 'long' else (
                                                                                                               avg_entry_price - exit_price) * position_size
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

    trades_df = pd.DataFrame(trades)
    trades_df = trades_df.sort_values(by='exit_time')

    return trades_df
