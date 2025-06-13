import pandas as pd
import TA_function as myTA
import indicators as qtpylib
import talib.abstract as ta


def _prepare_dataframe(self):
    df = self.df
    if 'time_x' in df.columns:
        df.rename(columns={'time_x': 'time'}, inplace=True)

    peaks, fibos, divs, bullish_ob, bearish_ob = myTA.find_pivots(df, 15, min_percentage_change=0.00001)
    df = pd.concat([df, peaks, fibos, divs], axis=1)
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



def _mark_fibo_reactions_all(self):
    df = self.df

    # Przygotuj strefy fibo dla normalnego TF
    GPL_bullish = pd.DataFrame({
        'time': df['time'],
        'low_boundary': df['fibo_local_0660_15'],
        'high_boundary': df['fibo_local_0618_15']
    })
    GPL_bearish = pd.DataFrame({
        'time': df['time'],
        'high_boundary': df['fibo_local_0660_15_bear'],
        'low_boundary': df['fibo_local_0618_15_bear']
    })
    GPG_bullish = pd.DataFrame({
        'time': df['time'],
        'low_boundary': df['fibo_global_0660_15'],
        'high_boundary': df['fibo_global_0618_15']
    })
    GPG_bearish = pd.DataFrame({
        'time': df['time'],
        'high_boundary': df['fibo_global_0660_15_bear'],
        'low_boundary': df['fibo_global_0618_15_bear']
    })

    # Przygotuj strefy fibo dla H1 timeframe (self)
    self.GPL_bullish_H1 = pd.DataFrame({
        'time': df['time'],
        'low_boundary': df['fibo_local_0660_15_H1'],
        'high_boundary': df['fibo_local_0618_15_H1']
    })
    self.GPL_bearish_H1 = pd.DataFrame({
        'time': df['time_H1'],
        'high_boundary': df['fibo_local_0660_15_bear_H1'],
        'low_boundary': df['fibo_local_0618_15_bear_H1']
    })
    self.GPG_bullish_H1 = pd.DataFrame({
        'time': df['time'],
        'low_boundary': df['fibo_global_0660_15_H1'],
        'high_boundary': df['fibo_global_0618_15_H1']
    })
    self.GPG_bearish_H1 = pd.DataFrame({
        'time': df['time_H1'],
        'high_boundary': df['fibo_global_0660_15_bear_H1'],
        'low_boundary': df['fibo_global_0618_15_bear_H1']
    })

    # Funkcja pomocnicza do wywołania mark_fibo_reactions i zwrócenia df z kolumnami reakcji
    def get_fibo_cols(df_main, zone_df, trend, prefix):
        reaction, low, high = myTA.mark_fibo_reactions(df_main, zone_df, trend)
        
        # Przenieś '_H1' na koniec, jeśli jest w prefixie
        if prefix.endswith('_H1'):
            base_prefix = prefix[:-3]  # usuń '_H1'
            reaction_col = f'{base_prefix}_reaction_H1'
            low_col = f'{base_prefix}_low_H1'
            high_col = f'{base_prefix}_high_H1'
        else:
            reaction_col = f'{prefix}_reaction'
            low_col = f'{prefix}_low'
            high_col = f'{prefix}_high'

        return pd.DataFrame({
            reaction_col: reaction,
            low_col: low,
            high_col: high,
        }, index=df_main.index)

    # Zbierz kolumny dla normal timeframe
    results = []
    results.append(get_fibo_cols(df, GPL_bullish, 'bullish', 'GPL_bullish'))
    results.append(get_fibo_cols(df, GPL_bearish, 'bearish', 'GPL_bearish'))
    results.append(get_fibo_cols(df, GPG_bullish, 'bullish', 'GPG_bullish'))
    results.append(get_fibo_cols(df, GPG_bearish, 'bearish', 'GPG_bearish'))

    # Zbierz kolumny dla H1 timeframe
    results.append(get_fibo_cols(df, self.GPL_bullish_H1, 'bullish', 'GPL_bullish_H1'))
    results.append(get_fibo_cols(df, self.GPL_bearish_H1, 'bearish', 'GPL_bearish_H1'))
    results.append(get_fibo_cols(df, self.GPG_bullish_H1, 'bullish', 'GPG_bullish_H1'))
    results.append(get_fibo_cols(df, self.GPG_bearish_H1, 'bearish', 'GPG_bearish_H1'))

    # Połącz wszystkie kolumny w jeden df
    reactions_df = pd.concat(results, axis=1)

    # Zainicjuj kolumny reakcji na False tam, gdzie mogą ich nie być (opcjonalnie, zależnie od potrzeb)
    for col in [c for c in reactions_df.columns if 'reaction' in c]:
        reactions_df[col].fillna(False, inplace=True)

    # Scal z oryginalnym df
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
            cols = myTA.mark_zone_reactions("normal", df, zone_df, direction, 'time', 'time')
            df[f'{direction}_{zone_type}_reaction'], df[f'{direction}_{zone_type}_low'], df[f'{direction}_{zone_type}_high'] = cols[:3]
            if zone_type == 'ob':
                setattr(self, f'{("bearish" if direction == "bullish" else "bullish")}_breaker', cols[3])

    self.bullish_breaker_validated, self.bearish_breaker_validated = myTA.invalidate_zones_by_candle_extremes(
        "normal", df, self.bullish_breaker, self.bearish_breaker, "idxx"
    )

    df['bullish_breaker_reaction'], df['bullish_breaker_low'], df['bullish_breaker_high'] = myTA.mark_zone_reactions(
        "normal", df, self.bullish_breaker_validated, 'bullish', 'time', 'time'
    )
    df['breaish_breaker_reaction'], df['breaish_breaker_low'], df['breaish_breaker_high'] = myTA.mark_zone_reactions(
        "normal", df, self.bearish_breaker_validated, 'bearish', 'time', 'time'
    )

def _process_higher_timeframe_zones(self):
    df = self.df
    df_H1 = df[['open_H1', 'high_H1', 'low_H1', 'close_H1', 'time_H1', 'idxx_H1']].copy()

    def process_zone(zone_name):
        reaction_cols = myTA.mark_zone_reactions("aditional", df_H1, getattr(self, zone_name), zone_name.split('_')[0], 'time_H1', 'time')
        # uprość nazwę do formatu np. 'bullish_fvg'
        simple_zone_name = '_'.join(zone_name.split('_')[:2])
        for i, suffix in enumerate(['reaction_H1', 'low_H1', 'high_H1']):
            df[f'{simple_zone_name}_{suffix}'] = reaction_cols[i]
        if 'ob' in zone_name:
            setattr(self, f'{("bearish" if "bullish" in zone_name else "bullish")}_breaker_H1', reaction_cols[3])

    for name in ['bullish_fvg_validated_H1', 'bullish_ob_validated_H1', 'bearish_fvg_validated_H1', 'bearish_ob_validated_H1']:
        process_zone(name)

    # Validate H1 breakers
    for breaker in ['bullish_breaker_H1', 'bearish_breaker_H1']:
        getattr(self, breaker).rename(columns={'idxx': 'idxx_H1'}, inplace=True)

    self.bullish_breaker_validated_H1, self.bearish_breaker_validated_H1 = myTA.invalidate_zones_by_candle_extremes_next(
        "aditional", df_H1, self.bullish_breaker_H1, self.bearish_breaker_H1, "idxx_H1"
    )

    df['bullish_breaker_reaction_H1'], df['bullish_breaker_low_H1'], df['bullish_breaker_high_H1'] = myTA.mark_zone_reactions(
        "aditional", df_H1, self.bullish_breaker_validated_H1, 'bullish', 'time_H1', 'time'
    )
    df['breaish_breaker_reaction_H1'], df['breaish_breaker_low_H1'], df['breaish_breaker_high_H1'] = myTA.mark_zone_reactions(
        "aditional", df_H1, self.bearish_breaker_validated_H1, 'bearish', 'time_H1', 'time'
    )

def _mark_sweeps(self):
    df = self.df
    df['liquidity_swep'] = (
        (myTA.check_reaction(df, df['LL_15']) & (df['LL_15_idx'] > df['HL_15_idx'])) |
        (myTA.check_reaction(df, df['HL_15']) & (df['LL_15_idx'] < df['HL_15_idx']))
    )
    df['monday_low_reaction'] = ((myTA.check_reaction(df, df['monday_low_H1'])) & (df['weekday_H1'] != 0))
    self.df = df