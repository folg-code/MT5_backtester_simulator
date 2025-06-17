import pandas as pd
from pandas import DataFrame
import config
from concurrent.futures import ProcessPoolExecutor, as_completed
import os


def vectorized_backtest(df: pd.DataFrame, symbol: str, slippage: float,
                        sl_pct: float, tp_pct: float, initial_size: float,
                        max_size: float, SINGLE_POSIOTION_MODE: bool) -> pd.DataFrame:
    if symbol is not None:
        return _vectorized_backtest_single_symbol(df, symbol, slippage, sl_pct, tp_pct, initial_size, max_size, SINGLE_POSIOTION_MODE)

    all_trades = []
    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = []
        for sym, group_df in df.groupby('symbol'):
            # Przekazujemy wszystkie argumenty
            futures.append(executor.submit(
                _vectorized_backtest_single_symbol,
                group_df.copy(), sym,
                slippage, sl_pct, tp_pct,
                initial_size, max_size, SINGLE_POSIOTION_MODE
            ))

        for future in as_completed(futures):
            try:
                trades = future.result()
                all_trades.append(trades)
            except Exception as e:
                print(f"❌ Błąd w backteście: {e}")

    return pd.concat(all_trades).sort_values(by='exit_time') if all_trades else pd.DataFrame()

def _vectorized_backtest_single_symbol(df: pd.DataFrame, symbol: str, slippage: float,
                                       sl_pct: float, tp_pct: float, initial_size: float,
                                       max_size: float, SINGLE_POSIOTION_MODE: bool) -> pd.DataFrame:
    df = df.copy()
    trades = []
    df = df.reset_index(drop=False)



    print("initiate _vectorized_backtest_single_symbol")


    for direction in ['long', 'short']:
        # Wyszukiwanie pozycji wejściowych i wyjściowych
        entries_idx = df[df['signal_entry'].apply(lambda x: isinstance(x, tuple) and x[0] == direction)]['index'].tolist()
        exits_idx = df[df['signal_exit'].apply(lambda x: isinstance(x, tuple) and x[0] == direction)]['index'].tolist()

        entries_pos = [df.index[df['index'] == idx][0] for idx in entries_idx]
        exits_pos = [df.index[df['index'] == idx][0] for idx in exits_idx]


        executed_trades = []

        for entry_pos in entries_pos:
            entry_signal = df.loc[entry_pos, 'signal_entry']
            levels = df.loc[entry_pos, 'levels']



            if not (isinstance(entry_signal, tuple) and isinstance(levels, tuple) and len(levels) == 3):
                continue

            enter_tag = entry_signal[1]  # Zmieniono z sygnału 'enter'

            sl = levels[0][1]
            tp1 = levels[1][1]
            tp2 = levels[2][1]

            sl_exit_tag = levels[0][2]
            tp1_exit_tag = levels[1][2]
            tp2_exit_tag = levels[2][2]

            entry_time = df.loc[entry_pos, 'time']

            if any(t['enter_tag'] == enter_tag and t['exit_time'] > entry_time for t in executed_trades):
                continue

            entry_price = df.loc[entry_pos, 'close'] * (1 + slippage) if direction == 'long' else df.loc[entry_pos, 'close'] * (1 - slippage)
            position_size = initial_size
            avg_entry_price = entry_price
            current_sl = sl

            exit_price = None
            exit_time = None
            exit_reason = None
            tp1_price = None
            tp1_time = None
            tp1_executed = False
            tp1_exit_reason = None
            pnl_total = 0
            update_sl_next_bar = False
            tp1_pnl = None

            for i in range(entry_pos + 1, len(df)):
                row = df.iloc[i]
                price_high = row['high']
                price_low = row[['close', 'open']].min()
                price_close = row['close']
                time = row['time']

                candle_range = row['high'] - row['low']
                lower_shadow = row[['close', 'open']].min() - row['high'] 
                upper_shadow = row['high'] - row[['close', 'open']].max()
                is_green = row['close'] > row['open']
                is_red = row['close'] < row['open']
                small_upper_shadow = (upper_shadow / candle_range) < 0.2 if candle_range != 0 else False
                small_lower_shadow = (lower_shadow / candle_range) < 0.2 if candle_range != 0 else False

                no_exit_long = is_green & small_upper_shadow
                no_exit_short = is_red & small_lower_shadow

                if update_sl_next_bar:
                    current_sl = entry_price
                    update_sl_next_bar = False

                if direction == 'long':
                    if not tp1_executed and price_high >= tp1 and not no_exit_long:
                        tp1_price = price_close
                        tp1_time = time
                        tp1_exit_reason = tp1_exit_tag
                        tp1_pnl = (tp1_price - avg_entry_price) * (position_size * 0.5)
                        position_size *= 0.5
                        tp1_executed = True
                        sl_exit_tag = tp1_exit_tag
                        update_sl_next_bar = True

                    if price_low <= current_sl:
                        exit_price = current_sl
                        exit_reason = sl_exit_tag
                        exit_time = time
                        break

                    if price_high >= tp2 and not no_exit_long:
                        exit_price = price_close
                        exit_reason = tp2_exit_tag
                        exit_time = time
                        break

                elif direction == 'short':
                    if not tp1_executed and price_low <= tp1 and not no_exit_short:
                        tp1_price = price_close
                        tp1_time = time
                        tp1_exit_reason = tp1_exit_tag
                        tp1_pnl = (avg_entry_price - tp1_price) * (position_size * 0.5)
                        position_size *= 0.5
                        tp1_executed = True
                        sl_exit_tag = tp1_exit_tag
                        update_sl_next_bar = True

                    if price_high >= current_sl:
                        exit_price = price_close
                        exit_reason = sl_exit_tag
                        exit_time = time
                        break

                    if price_low <= tp2:
                        exit_price = price_close
                        exit_reason = tp2_exit_tag
                        exit_time = time
                        break

                # Nowy format sygnału wyjścia
                sig_exit = row.get("signal_exit")
                if isinstance(sig_exit, tuple) and sig_exit[0] == direction and enter_tag in sig_exit[1]:
                    exit_price = price_close
                    exit_time = time
                    exit_reason = sig_exit[2]
                    break

                if df.loc[i, 'index'] in exits_idx:
                    exit_price = price_close * (1 - slippage) if direction == 'long' else price_close * (1 + slippage)
                    exit_time = time
                    exit_reason = sig_exit[2]
                    break

            if exit_price is None:
                exit_price = df.iloc[-1]['close'] * (1 - slippage) if direction == 'long' else df.iloc[-1]['close'] * (1 + slippage)
                exit_time = df.iloc[-1]['time']
                exit_reason = 'end_of_data'

            if direction == 'long':
                if tp1_executed:
                    pnl_total = (((tp1_price - avg_entry_price) * 0.5) + ((exit_price - avg_entry_price) *0.5 ))
                else:
                    pnl_total = exit_price - avg_entry_price
            else:
                if tp1_executed:
                    pnl_total = (((avg_entry_price - tp1_price) * 0.5) + ((avg_entry_price - exit_price) * 0.5))
                else:
                    pnl_total = (avg_entry_price - exit_price)

            trades.append({
                'symbol': symbol,
                'direction': direction,
                'entry_time': entry_time,
                'exit_time': exit_time,
                'entry_price': avg_entry_price,
                'exit_price': exit_price,
                'position_size': initial_size,
                'pnl': pnl_total,
                'exit_reason': exit_reason,
                'enter_tag': enter_tag,
                'exit_tag': exit_reason,
                'tp1_price': tp1_price,
                'tp1_time': tp1_time,
                'tp1_exit_reason': tp1_exit_reason,
                'tp2_price': tp2 if exit_reason == tp2_exit_tag else None,
                'tp2_time': exit_time if exit_reason == tp2_exit_tag else None,
                'tp1_pnl': tp1_pnl
            })
            
            executed_trades.append({
                'enter_tag': enter_tag,
                'exit_time': exit_time
            })


    return pd.DataFrame(trades)