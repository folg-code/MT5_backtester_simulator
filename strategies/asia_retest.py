# strategy.py
import pandas as pd
from TA_function import find_pivots
import TA_function as myTA
import indicators as qtpylib
import talib.abstract as ta
from functools import reduce
import config
from utils.decorators import informative  # ← NOWE
from utils.data_loader import get_data             # ← NOWE
import MetaTrader5 as mt5

class AsiaRetest:
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        self.informative_dataframes = {}

    def get_informative_dataframe(self, timeframe: str) -> pd.DataFrame:
        return get_data(
            config.SYMBOL,
            mt5.TIMEFRAME_M15,
            pd.to_datetime(config.TIMERANGE['start']),
            pd.to_datetime(config.TIMERANGE['end'])
        )

    def populate_informative_indicators(self):
        for attr_name in dir(self):
            attr = getattr(self, attr_name)
            if callable(attr) and getattr(attr, '_informative', False):
                timeframe = attr._informative_timeframe
                if timeframe not in self.informative_dataframes:
                    info_df = self.get_informative_dataframe(timeframe)
                    info_df = attr(df = info_df.copy())
                    self.informative_dataframes[timeframe] = info_df
                else:
                    info_df = self.informative_dataframes[timeframe]

                # Załóżmy, że timeframe to np. 'M15', to tworzymy kolumnę 'time_M15'
                freq = pandas_freq_from_timeframe(timeframe)
                time_col = f'time_{timeframe}'
                self.df[time_col] = self.df['time'].dt.floor(freq)

                # Lista kolumn do merge (wszystkie oprócz 'time')
                cols_to_merge = [col for col in info_df.columns if col != 'time']

                # Przyrostek do kolumn z informacyjnego df, żeby uniknąć kolizji nazw
                suffix = f"_{timeframe}"

                # Przygotuj df do merge: czas + kolumny z sufiksem
                info_df_renamed = info_df[['time'] + cols_to_merge].copy()
                info_df_renamed = info_df_renamed.rename(
                    columns={col: col + suffix for col in cols_to_merge}
                )

                # Scal (merge) po time_xxx (self.df) i time (info_df)
                self.df = self.df.merge(
                    info_df_renamed,
                    left_on=time_col,
                    right_on='time',
                    how='left'
                )
                columns_to_drop = ['time_y', 'spread_M15', 'real_volume_M15']  # podaj kolumny, które chcesz usunąć
                self.df.drop(columns=columns_to_drop, inplace=True)



    @informative('M15')
    def populate_indicators_M15(self, df: pd.DataFrame):

        df['rsi'] = ta.RSI(df['close'], timeperiod=14)
        df['rsi_sma'] = ta.SMA(df['rsi'], timeperiod=14)

        df['breakout_occurred'] = False
        df['london_asia_liq_breakout'] = False
        df['time'] = pd.to_datetime(df['time'])
        df['hour'] = df['time'].dt.hour
        df['date'] = df['time'].dt.date

        for current_date in df['date'].unique():
            day_data = df[df['date'] == current_date]
            asia_time = (day_data['hour'] >= 3) & (day_data['hour'] < 9)
            asia_session = day_data[asia_time]

            if not asia_session.empty:
                high = asia_session['high'].max()
                low = asia_session['low'].min()
                df.loc[df['date'] == current_date, 'asia_high'] = high
                df.loc[df['date'] == current_date, 'asia_low'] = low

                breakout_range = day_data[((day_data['hour'] >= 9)) & (day_data['hour'] < 11)]
                breakout_candle = breakout_range[breakout_range['close'] > high]

                if not breakout_candle.empty:
                    first_breakout = breakout_candle.iloc[0]
                    breakout_low = first_breakout['low']
                    df.loc[df['date'] == current_date, 'breakout_occurred'] = True
                    df.loc[df['date'] == current_date, 'breakout_low'] = breakout_low

        return df

    def populate_indicators(self):
        df = self.df


        if 'time_x' in df.columns:
            df.rename(columns={'time_x': 'time'}, inplace=True)



        df['breakout_occurred'] = False
        df['london_asia_liq_breakout'] = False
        df['time'] = pd.to_datetime(df['time'])
        df['hour'] = df['time'].dt.hour
        df['date'] = df['time'].dt.date

        for current_date in df['date'].unique():
            day_data = df[df['date'] == current_date]
            asia_time = (day_data['hour'] >= 3) & (day_data['hour'] < 9)
            asia_session = day_data[asia_time]

            if not asia_session.empty:
                high = asia_session['high'].max()
                low = asia_session['low'].min()
                df.loc[df['date'] == current_date, 'asia_high'] = high
                df.loc[df['date'] == current_date, 'asia_low'] = low

                breakout_range = day_data[(day_data['hour'] >= 9) & (day_data['hour'] < 11)]
                breakout_candle = breakout_range[breakout_range['close'] > high]

                if not breakout_candle.empty:
                    first_breakout = breakout_candle.iloc[0]
                    breakout_low = first_breakout['low']
                    df.loc[df['date'] == current_date, 'breakout_occurred'] = True
                    df.loc[df['date'] == current_date, 'breakout_low'] = breakout_low

                    test_condition = (
                        (df['date'] == current_date) &
                        (((df['close'] < high) | (df['low'] < high)) & (df['open'] > high)) &
                        ((df['hour'] >= 9) & (df['hour'] < 11))
                    )
                    df.loc[test_condition, 'london_asia_liq_breakout'] = True

        df['ma_fast'] = df['close'].rolling(window=5).mean()
        df['ma_slow'] = df['close'].rolling(window=20).mean()

        heikinashi = qtpylib.heikinashi(df)
        df[['ha_open', 'ha_close', 'ha_high', 'ha_low']] = heikinashi[['open', 'close', 'high', 'low']]

        stoch = ta.STOCH(df, 13, 3, 0, 3, 0)
        df['orange_d'] = stoch['slowd']
        df['blue_k'] = stoch['slowk']

        df['atr'] = ta.ATR(df, timeperiod=14)
        df['rsi'] = ta.RSI(df, 14)
        df['sma_50'] = ta.SMA(df, timeperiod=50)
        df['sma_200'] = ta.SMA(df, timeperiod=200)

        self.n1 = 10
        self.n2 = 7
        df = myTA.market_cipher(self, df)
        df['bands'] = myTA.calculate_vwma(df, 35)
        df['atr_tma'] = ta.ATR(df, timeperiod=154)
        df['tma_low_bands'] = df['bands'] - (2 * df['atr_tma'])
        df['tma_high_bands'] = df['bands'] + (2 * df['atr_tma'])

        peaks10, fibos10, divs10, bullish_ob, bearish_ob = myTA.find_pivots(df, 20, min_percentage_change=0.001)
        df = pd.concat([df, peaks10, fibos10, divs10], axis=1)

        bullish_fvg, bearish_fvg = myTA.detect_fvg(df, 1.5)

        self.bullish_fvg_validated = myTA.validate_fvg(bullish_fvg, price_col='low', idx_col='idx',direction='bullish')
        self.bearish_fvg_validated = myTA.validate_fvg(bearish_fvg, price_col='high', idx_col='idx', direction='bearish')
        self.bullish_ob_validated = myTA.validate_orderblocks(bullish_ob, price_col='pivot_body', idx_col='idx',direction='bullish')
        self.bearish_ob_validated = myTA.validate_orderblocks(bearish_ob, price_col='pivotprice', idx_col='idx', direction='bearish')


        self.df = df

    def populate_entry_trend(self):
        df = self.df
        df['signal'] = None

        for current_date in df['date'].unique():
            day_data = df[df['date'] == current_date]
            asia_time = (day_data['hour'] >= 3) & (day_data['hour'] < 9)
            asia_session = day_data[asia_time]

            if asia_session.empty:
                continue

            high = asia_session['high'].max()
            low = asia_session['low'].min()

            # Zidentyfikuj breakout między 9:00 a 11:00
            breakout_window = day_data[(day_data['hour'] >= 9) & (day_data['hour'] < 11)]
            breakout_candle = breakout_window[breakout_window['close'] > high]

            if breakout_candle.empty:
                continue

            first_breakout = breakout_candle.iloc[0]
            breakout_time = first_breakout.name
            breakout_low = first_breakout['low']
            breakout_high = first_breakout['high']

            # Szukaj retestu po breakout
            post_breakout_data = day_data[day_data.index > breakout_time]

            for idx, row in post_breakout_data.iterrows():
                if row['low'] <= high:
                    # Mamy re-test — generuj sygnał wejścia
                    df.at[idx, 'signal'] = (
                        "enter", "long", "asia_retest", breakout_low,
                        row['close'] + 3 * (row['close'] - breakout_low),  # SL, TP
                        "asia_breakout_sl", "asia_breakout_tp"
                    )
                    break  # tylko jeden sygnał dziennie

        return None

    def populate_exit_trend(self):
        df = self.df
        TMA_exit_long_hodl = (
            (df['red_dot']) &
            (df['close'] < df['sma_50']) &
            (df['sma_200'] > df['sma_200'].shift(5))
        )
        df.loc[TMA_exit_long_hodl, 'signal'] = df.loc[TMA_exit_long_hodl].apply(
            lambda _: ("exit", "long"), axis=1
        )
        return None

    def populate_scaling(self):
        df = self.df
        scale_up = df['rsi'] < 10
        scale_down = df['rsi'] > 90

        df.loc[scale_up, 'signal'] = df.loc[scale_up].apply(lambda _: ("scale", "long", "scale_up", None, None), axis=1)
        df.loc[scale_down, 'signal'] = df.loc[scale_down].apply(lambda _: ("scale", "long", "scale_down", None, None), axis=1)
        return None

    def calculate_custom_sl_tp(self) -> pd.DataFrame:
        df = self.df

        for idx, row in df.iterrows():
            signal = row['signal']
            if isinstance(signal, tuple) and len(signal) == 2:
                action, direction = signal
                tag = 'liq_breakout'
                sl_exit_tag = 'breakout_SL'
                tp_exit_tag = 'breakout_TP'

                if action == "enter":
                    if direction == "long":
                        breakout_low = row.get('breakout_low', None)
                        if breakout_low and row['close'] > breakout_low:
                            custom_sl = breakout_low
                            custom_tp = row['close'] + 5 * (row['close'] - breakout_low)
                            df.at[idx, 'signal'] = (
                                "enter", "long", tag, custom_sl, custom_tp, sl_exit_tag, tp_exit_tag
                            )

                    elif direction == "short":
                        last_high = df['high'].rolling(5).max().iloc[idx]
                        if last_high and last_high > row['close']:
                            custom_sl = last_high
                            custom_tp = row['close'] - 3 * (last_high - row['close'])
                            df.at[idx, 'signal'] = (
                                "enter", "short", tag, custom_sl, custom_tp, sl_exit_tag, tp_exit_tag
                            )

        return df

    def run(self) -> pd.DataFrame:

        self.populate_informative_indicators()  # ← WAŻNE!
        self.populate_indicators()
        self.populate_entry_trend()
        self.populate_exit_trend()
        self.populate_scaling()
        self.calculate_custom_sl_tp()
        # Zapisz kopię pełnego df do plotowania
        self.df_plot = self.df.copy()

        # Przygotuj okrojoną wersję do backtestu
        self.df_backtest = self.df[['time', 'open', 'high', 'low', 'close', 'signal']].copy()

        return self.df_backtest

def pandas_freq_from_timeframe(tf: str) -> str:
    # Przykładowa mapka, dostosuj do swoich timeframe'ów
    mapping = {
        'H1': '1h',
        'H4': '4h',
        'D1': '1d',
        'M1': '1min',
        'M5': '5min',
        'M15': '15min',
        # inne...
    }
    return mapping.get(tf.upper(), tf)  # jeśli brak mapy, zwróć oryginał