import pandas as pd
import numpy as np
import TA_function as myTA
import indicators as qtpylib
import talib.abstract as ta


def _prepare_dataframe(self):
    df = self.df
    if 'time_x' in df.columns:
        df.rename(columns={'time_x': 'time'}, inplace=True)

    peaks, fibos, bullish_ob, bearish_ob = myTA.find_pivots(df, 15, min_percentage_change=0.00001)
    df = pd.concat([df, peaks, fibos], axis=1)
    heikinashi = qtpylib.heikinashi(df)
    df[['ha_open', 'ha_close', 'ha_high', 'ha_low']] = heikinashi[['open', 'close', 'high', 'low']]
    self.df = df
    self.bullish_ob = bullish_ob
    self.bearish_ob = bearish_ob

def _calculate_indicators(self):
    df = self.df
    df['atr_tma'] = ta.ATR(df, timeperiod=154)
    df['sma_50'] = ta.SMA(df, timeperiod=50)
    df['sma_200'] = ta.SMA(df, timeperiod=200)
    df['rsi'] = ta.RSI(df, 14)
    df['atr'] = ta.ATR(df, timeperiod=14)
    df['rsi_sma'] = ta.SMA(df['rsi'], 28)
    df['atr_sma'] = ta.SMA(df['atr'], 28)
    df['adx'] = ta.ADX(df)
    df['adx_sma'] = ta.SMA(df['adx'], 14)
    df['plus_di'] = ta.PLUS_DI(df, 25)
    df['minus_di'] = ta.MINUS_DI(df, 25)
    df['macd_fast'] = ta.SMA(df, 12)
    df['macd_slow'] = ta.SMA(df, 26)
    df['macd_line'] = df['macd_fast'] - df['macd_slow']
    df['macd_signal'] = ta.SMA(df['macd_line'], 9)
    df['macd_hist'] = df['macd_line'] - df['macd_signal']

    df['max_body_rolling_5_min'] = df[['close', 'open']].max(axis=1).rolling(5).min().shift(1)
    df['max_body_rolling_5_min_shift2'] = df[['close', 'open']].max(axis=1).rolling(5).min().shift(2)
    df['min_body_rolling_5_max'] = df[['close', 'open']].min(axis=1).rolling(5).max().shift(1)
    df['min_body_rolling_5_max_shift2'] = df[['close', 'open']].min(axis=1).rolling(5).max().shift(2)

    df['idxx'] = df.index

def _mark_fibo_reactions_all(self):
    df = self.df

    # Zdefiniuj wszystkie strefy jako słownik: {prefix: zone_df}
    zones = {
        'bullish_GPL': pd.DataFrame({
            'time': df['time'],
            'low_boundary': df['fibo_local_0660_15'],
            'high_boundary': df['fibo_local_0618_15']
        }),
        'bearish_GPL': pd.DataFrame({
            'time': df['time'],
            'high_boundary': df['fibo_local_0660_15_bear'],
            'low_boundary': df['fibo_local_0618_15_bear']
        }),
        'bullish_GPG': pd.DataFrame({
            'time': df['time'],
            'low_boundary': df['fibo_global_0660_15'],
            'high_boundary': df['fibo_global_0618_15']
        }),
        'bearish_GPG': pd.DataFrame({
            'time': df['time'],
            'high_boundary': df['fibo_global_0660_15_bear'],
            'low_boundary': df['fibo_global_0618_15_bear']
        }),
        'bullish_GPL_H1': pd.DataFrame({
            'time': df['time'],
            'low_boundary': df['fibo_local_0660_15_H1'],
            'high_boundary': df['fibo_local_0618_15_H1']
        }),
        'bearish_GPL_H1': pd.DataFrame({
            'time': df['time_H1'],
            'high_boundary': df['fibo_local_0660_15_bear_H1'],
            'low_boundary': df['fibo_local_0618_15_bear_H1']
        }),
        'bullish_GPG_H1': pd.DataFrame({
            'time': df['time'],
            'low_boundary': df['fibo_global_0660_15_H1'],
            'high_boundary': df['fibo_global_0618_15_H1']
        }),
        'bearish_GPG_H1': pd.DataFrame({
            'time': df['time_H1'],
            'high_boundary': df['fibo_global_0660_15_bear_H1'],
            'low_boundary': df['fibo_global_0618_15_bear_H1']
        }),
    }

    def get_fibo_cols(df_main, zone_df, trend, zone_name, is_H1=False):
        reaction, is_in, low, high = myTA.mark_fibo_reactions(df_main, zone_df, trend)
        prefix = f"{trend}_{zone_name}"
        suffix = "_H1" if is_H1 else ""

        return pd.DataFrame({
            f"{prefix}_reaction{suffix}": reaction,
            f"{prefix}_is_in{suffix}": is_in,
            f"{prefix}_low{suffix}": low,
            f"{prefix}_high{suffix}": high
        }, index=df_main.index)

    results = []

    # Iteruj przez wszystkie zdefiniowane strefy
    for prefix, zone_df in zones.items():
        parts = prefix.split('_')
        trend = parts[0]
        zone_name = parts[1]
        is_H1 = (len(parts) == 3 and parts[2] == 'H1')
        results.append(get_fibo_cols(df, zone_df, trend, zone_name, is_H1))

    # Połącz wyniki w jeden DataFrame
    reactions_df = pd.concat(results, axis=1)

    # Wypełnij brakujące reakcje i is_in jako False
    for col in reactions_df.columns:
        if 'reaction' in col or 'is_in' in col:
            reactions_df[col] = reactions_df[col].fillna(False)

    # Połącz z oryginalnym DataFrame
    self.df = pd.concat([df, reactions_df], axis=1)

    

def _detect_and_validate_zones(self):
    df = self.df
    bullish_fvg, bearish_fvg = myTA.detect_fvg(df, 1.5)
    bullish_gap, bearish_gap = myTA.detect_gaps(df, 0.002)

    # Label zone types
    for zone, name in zip(
        [bullish_fvg, self.bullish_ob, bullish_gap, bearish_fvg, self.bearish_ob, bearish_gap],
        ['fvg', 'ob', 'gap', 'fvg', 'ob', 'gap']
    ):
        zone['zone_type'] = name

    df['idxx'] = df.index
    bullish_zones = pd.concat([bullish_fvg, self.bullish_ob, bullish_gap], ignore_index=True).sort_values(by='idxx')
    bearish_zones = pd.concat([bearish_fvg, self.bearish_ob, bearish_gap], ignore_index=True).sort_values(by='idxx')


    self.bullish_zones_validated, self.bearish_zones_validated = myTA.invalidate_zones_by_candle_extremes(
        "normal", df, bullish_zones, bearish_zones, "idxx"
    )
    self.bullish_fvg_validated = self.bullish_zones_validated[self.bullish_zones_validated['zone_type'] == 'fvg'].copy()
    self.bearish_fvg_validated = self.bullish_zones_validated[self.bullish_zones_validated['zone_type'] == 'fvg'].copy()
    self.bullish_ob_validated = self.bullish_zones_validated[self.bullish_zones_validated['zone_type'] == 'ob'].copy()
    self.bearish_ob_validated = self.bullish_zones_validated[self.bullish_zones_validated['zone_type'] == 'ob'].copy()
    self.bullish_gap_validated = self.bullish_zones_validated[self.bullish_zones_validated['zone_type'] == 'gap'].copy()
    self.bearish_gap_validated = self.bullish_zones_validated[self.bullish_zones_validated['zone_type'] == 'gap'].copy()
    

def _mark_zone_reactions(self):
    df = self.df

    for zone_type in ['fvg', 'ob', 'gap']:
        for direction in ['bullish', 'bearish']:
            zone_df = getattr(self, f'{direction}_{zone_type}_validated')

            result = myTA.mark_zone_reactions("normal", df, zone_df, direction, 'time', 'time')

            reaction_col, is_in_col, low_col, high_col, *rest = result

            df[f'{direction}_{zone_type}_reaction'] = reaction_col
            df[f'{direction}_{zone_type}_is_in'] = is_in_col
            df[f'{direction}_{zone_type}_low'] = low_col
            df[f'{direction}_{zone_type}_high'] = high_col

            if zone_type == 'ob' and rest:
                breaker_attr = f"{'bearish' if direction == 'bullish' else 'bullish'}_breaker"
                setattr(self, breaker_attr, rest[0])


    self.bullish_breaker_validated, self.bearish_breaker_validated = myTA.invalidate_zones_by_candle_extremes(
        "normal", df, self.bullish_breaker, self.bearish_breaker, "idxx"
    )


    bullish_result = myTA.mark_zone_reactions("normal", df, self.bullish_breaker_validated, 'bullish', 'time', 'time')
    bearish_result = myTA.mark_zone_reactions("normal", df, self.bearish_breaker_validated, 'bearish', 'time', 'time')

    df['bullish_breaker_reaction'] = bullish_result[0]
    df['bullish_breaker_is_in'] = bullish_result[1]
    df['bullish_breaker_low'] = bullish_result[2]
    df['bullish_breaker_high'] = bullish_result[3]

    df['bearish_breaker_reaction'] = bearish_result[0]
    df['bearish_breaker_is_in'] = bearish_result[1]
    df['bearish_breaker_low'] = bearish_result[2]
    df['bearish_breaker_high'] = bearish_result[3]

def _process_higher_timeframe_zones(self):
    df = self.df
    df_H1 = df[['open_H1', 'high_H1', 'low_H1', 'close_H1', 'time_H1', 'idxx_H1']].copy()

    def process_zone(zone_name):
        result = myTA.mark_zone_reactions(
            "aditional", df_H1, getattr(self, zone_name), zone_name.split('_')[0], 'time_H1', 'time'
        )

        # Uprość nazwę, np. 'bullish_fvg'
        simple_zone_name = '_'.join(zone_name.split('_')[:2])

        reaction_col, is_in_col, low_col, high_col, *rest = result

        df[f'{simple_zone_name}_reaction_H1'] = reaction_col
        df[f'{simple_zone_name}_is_in_H1'] = is_in_col
        df[f'{simple_zone_name}_low_H1'] = low_col
        df[f'{simple_zone_name}_high_H1'] = high_col

        if rest:
            breaker_blocks_df = rest[0]
            breaker_attr = f"{'bearish' if 'bullish' in zone_name else 'bullish'}_breaker_H1"
            setattr(self, breaker_attr, breaker_blocks_df)

    # Przetwórz strefy FVG i OB
    for name in ['bullish_fvg_validated_H1', 'bullish_ob_validated_H1',
                 'bearish_fvg_validated_H1', 'bearish_ob_validated_H1']:
        process_zone(name)

    # Popraw nazwy kolumn breakerów
    for breaker in ['bullish_breaker_H1', 'bearish_breaker_H1']:
        getattr(self, breaker).rename(columns={'idxx': 'idxx_H1'}, inplace=True)

    # Odśwież DF_H1
    df_H1 = df[['open_H1', 'high_H1', 'low_H1', 'close_H1', 'time_H1', 'idxx_H1']].copy()

    # Walidacja breakerów
    self.bullish_breaker_validated_H1, self.bearish_breaker_validated_H1 = myTA.invalidate_zones_by_candle_extremes_next(
        "aditional", df_H1, self.bullish_breaker_H1, self.bearish_breaker_H1, "idxx_H1"
    )

    # Reakcje na validowane breakery
    bullish_result = myTA.mark_zone_reactions(
        "aditional", df_H1, self.bullish_breaker_validated_H1, 'bullish', 'time_H1', 'time'
    )
    bearish_result = myTA.mark_zone_reactions(
        "aditional", df_H1, self.bearish_breaker_validated_H1, 'bearish', 'time_H1', 'time'
    )

    df['bullish_breaker_reaction_H1'] = bullish_result[0]
    df['bullish_breaker_is_in_H1'] = bullish_result[1]
    df['bullish_breaker_low_H1'] = bullish_result[2]
    df['bullish_breaker_high_H1'] = bullish_result[3]

    df['bearish_breaker_reaction_H1'] = bearish_result[0]
    df['bearish_breaker_is_in_H1'] = bearish_result[1]
    df['bearish_breaker_low_H1'] = bearish_result[2]
    df['bearish_breaker_high_H1'] = bearish_result[3]

def _mark_sweeps(self):
    df = self.df

        # === Equal High ===
    reaction_hh = myTA.check_reaction(df, df['HH_15'], 'bearish')
    reaction_lh = myTA.check_reaction(df, df['LH_15'], 'bearish') & (df['LH_15_idx'] > df['HH_15_idx'])

    close_near_hh = ((df['close'] - df['HH_15']).abs() < 0.2 * df['atr']) | ((df['high'] - df['HH_15']).abs() < 0.2 * df['atr']) 
    close_near_lh = (((df['close'] - df['LH_15']).abs() < 0.2 * df['atr']) | ((df['high'] - df['LH_15']).abs() < 0.2 * df['atr'])) & (df['LH_15_idx'] > df['HH_15_idx']) 

    df['trigger_eqh_hh'] = reaction_hh | close_near_hh
    df['trigger_eqh_lh'] = reaction_lh | close_near_lh

    # Zarejestruj indeks triggera
    df['eqh_hh_trigger_idx'] = np.nan
    df.loc[df['trigger_eqh_hh'], 'eqh_hh_trigger_idx'] = df.loc[df['trigger_eqh_hh']].index
    df['eqh_hh_trigger_idx'] = df['eqh_hh_trigger_idx'].ffill()

    df['eqh_lh_trigger_idx'] = np.nan
    df.loc[df['trigger_eqh_lh'], 'eqh_lh_trigger_idx'] = df.loc[df['trigger_eqh_lh']].index
    df['eqh_lh_trigger_idx'] = df['eqh_lh_trigger_idx'].ffill()

    # Warunki po triggerze
    drawdown_from_hh = df['HH_15'] - df['close'].rolling(5).min()
    drawdown_from_lh = df['LH_15'] - df['close'].rolling(5).min()
    after_trigger_hh = df.index > df['eqh_hh_trigger_idx']
    after_trigger_lh = df.index > df['eqh_lh_trigger_idx']

    price_rejected_eqh_hh = (drawdown_from_hh > 1.5 * df['atr']) & (df['close'] < df['HH_15']) & after_trigger_hh
    price_rejected_eqh_lh = (drawdown_from_lh > 1.5 * df['atr']) & (df['close'] < df['LH_15']) & after_trigger_lh

    guard_eqh_hh = (df['close'].rolling(15).max() ) < df['HH_15'] * 1.001
    guard_eqh_lh = (df['close'].rolling(15).max() ) < df['LH_15'] * 1.001

    eqh_condition = (
        (df['trigger_eqh_hh'].rolling(5).max().astype(bool) & price_rejected_eqh_hh & guard_eqh_hh) |
        (df['trigger_eqh_lh'].rolling(5).max().astype(bool) & price_rejected_eqh_lh & guard_eqh_lh)
    )


    # === Equal Low ===
    reaction_ll = myTA.check_reaction(df, df['LL_15'], 'bullish')
    reaction_hl = myTA.check_reaction(df, df['HL_15'], 'bullish') & (df['HL_15_idx'] > df['LL_15_idx'])

    close_near_ll = ((df['close'] - df['LL_15']).abs() < 0.2 * df['atr']) | ((df['low'] - df['LL_15']).abs() < 0.2 * df['atr'])
    close_near_hl = (((df['close'] - df['HL_15']).abs() < 0.2 * df['atr']) | ((df['low'] - df['HL_15']).abs() < 0.2 * df['atr'])) & (df['HL_15_idx'] > df['LL_15_idx'])

    df['trigger_eql_ll'] = reaction_ll | close_near_ll
    df['trigger_eql_hl'] = reaction_hl | close_near_hl

    df['eql_ll_trigger_idx'] = np.nan
    df.loc[df['trigger_eql_ll'], 'eql_ll_trigger_idx'] = df.loc[df['trigger_eql_ll']].index
    df['eql_ll_trigger_idx'] = df['eql_ll_trigger_idx'].ffill()

    df['eql_hl_trigger_idx'] = np.nan
    df.loc[df['trigger_eql_hl'], 'eql_hl_trigger_idx'] = df.loc[df['trigger_eql_hl']].index
    df['eql_hl_trigger_idx'] = df['eql_hl_trigger_idx'].ffill()

    markup_from_ll = df['close'].rolling(5).max() - df['LL_15']
    markup_from_hl = df['close'].rolling(5).max() - df['HL_15']
    after_trigger_ll = df.index > df['eql_ll_trigger_idx']
    after_trigger_hl = df.index > df['eql_hl_trigger_idx']

    price_rejected_eql_ll = (markup_from_ll > 1.5 * df['atr']) & (df['close'] > df['LL_15']) & after_trigger_ll
    price_rejected_eql_hl = (markup_from_hl > 1.5 * df['atr']) & (df['close'] > df['HL_15']) & after_trigger_hl

    guard_eql_ll = (df['close'].rolling(15).min() * 0.999) > df['LL_15']
    guard_eql_hl = (df['close'].rolling(15).min() * 0.999) > df['HL_15']

    eql_condition = (
        (df['trigger_eql_ll'].rolling(5).max().astype(bool) & price_rejected_eql_ll & guard_eql_ll) |
        (df['trigger_eql_hl'].rolling(5).max().astype(bool) & price_rejected_eql_hl & guard_eql_hl)
    )
    

    df['EQH'] = eqh_condition
    df['EQL'] = eql_condition
    df['bullish_mons_reaction'] = ((myTA.check_reaction(df, df['monday_low_H1'], 'bullish')) & (df['weekday_H1'] != 0))
    df['bearish_mons_reaction'] = ((myTA.check_reaction(df, df['monday_high_H1'], 'bearish')) & (df['weekday_H1'] != 0))

    df['bullish_lqs'] = (
        (myTA.check_reaction(df, df['LL_15'], 'bullish') ) |
        (myTA.check_reaction(df, df['HL_15'], 'bullish') & (df['LL_15_idx'] < df['HL_15_idx']))
    )

    df['bearish_lqs'] = (
        (myTA.check_reaction(df, df['HH_15'], 'bearish') ) |
        (myTA.check_reaction(df, df['LH_15'], 'bearish') & (df['HH_15_idx'] < df['LH_15_idx']))
    )

    
    self.df = df