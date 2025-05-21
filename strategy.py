# strategy.py
import pandas as pd
from TA_function import find_pivots
import TA_function as myTA
import indicators as qtpylib
import talib.abstract as ta
from functools import reduce
import config

class MovingAverageCrossoverStrategy:
    def __init__(self, df: pd.DataFrame):
        """
        Inicjalizacja strategii na podstawie dostarczonych danych.
        """
        self.df = df.copy()


    def populate_indicators(self):
        """
        Dodaje wskaźniki do ramki danych.
        """
        df = self.df

        df['ma_fast'] = df['close'].rolling(window=5).mean()
        df['ma_slow'] = df['close'].rolling(window=20).mean()

        heikinashi = qtpylib.heikinashi(df)

        df['ha_open'] = heikinashi['open']
        df['ha_close'] = heikinashi['close']
        df['ha_high'] = heikinashi['high']
        df['ha_low'] = heikinashi['low']

        stoch = ta.STOCH(df, 13, 3, 0, 3, 0)
        df['orange_d'] = stoch['slowd']
        df['blue_k'] = stoch['slowk']

        df['atr'] = ta.ATR(df, timeperiod=14)
        df['rsi'] = ta.RSI(df, 14)

        df['sma_50'] = ta.SMA(df, timeperiod=50)
        df['sma_200'] = ta.SMA(df, timeperiod=200)

        self.n1 = 10  # WT Channel Length
        self.n2 = 7  # WT Average Length
        df = myTA.market_cipher(self, df)

        df['bands'] = myTA.calculate_vwma(df, 35)
        df['atr_tma'] = ta.ATR(df, timeperiod=(154))

        df['tma_low_bands'] = df['bands'] - (2 * df['atr_tma'])
        df['tma_high_bands'] = df['bands'] + (2 * df['atr_tma'])

        peaks10, fibos10, divs10 = myTA.find_pivots(df, 10, min_percentage_change=0.001)

        df = pd.concat([df,
                               peaks10, fibos10, divs10,
                                ], axis=1)

    def populate_entry_trend(self) -> dict | None:
        """
        Sprawdza warunki wejścia. Zwraca sygnał lub None.
        """

        df = self.df

        conditions = []
        df.loc[:, 'enter_tag'] = ''
        df['enter_long'] = False
        df['enter_short'] = False

        cipher_positive_mfi_long = (
                (df['mfirsi'] > 0) &
                (df['green_dot']) &
                (df['wt1'] < 0) &
                (df['wt2'] < 0)
        )

        cipher_negative_mfi_long = (
                 (df['green_dot'] == True) &
                 (df['wt1'] < 0) &
                 (df['wt2'] < 0))

        icarius_long = (
                (myTA.check_reaction(df, df["tma_low_bands"]) == True))


        conditions.append(cipher_positive_mfi_long)
        df.loc[cipher_positive_mfi_long, 'enter_tag'] += 'cipher_positive_mfi_long'

        conditions.append(cipher_negative_mfi_long)
        df.loc[cipher_negative_mfi_long, 'enter_tag'] += 'cipher_negative_mfi_long'

        conditions.append(icarius_long)
        df.loc[icarius_long, 'enter_tag'] += 'icarius_long'

        if conditions:
            df.loc[
                reduce(lambda x, y: x | y, conditions),
                'enter_long'] = True

        return None

    def populate_exit_trend(self) -> dict | None:
        """
        Sprawdza warunki wyjścia z pozycji. Zwraca sygnał lub None.
        direction: 'buy' lub 'sell' (pozycja aktywna)
        """

        df = self.df

        conditions = []
        df.loc[:, 'exit_tag'] = ''
        df['exit_long'] = False
        df['exit_short'] = False

        TMA_exit_long_hodl = (
                (df['red_dot']) &
                (df['close'] < df['sma_50']) &
                (df['sma_200'] > df['sma_200'].shift(5))
        )

        TMA_exit_long_quick = (
                (df['red_dot']) &
                (df['wt1'] > 0) &
                (df['wt2'] > 0) &
                ((df['sma_200'] < df['sma_200'].shift(5)))
        )

        conditions.append(TMA_exit_long_hodl)
        df.loc[TMA_exit_long_hodl, 'exit_tag'] += 'TMA_exit_long_hodl'

        conditions.append(TMA_exit_long_quick)
        df.loc[TMA_exit_long_quick, 'exit_tag'] += 'TMA_exit_long_quick'

        if conditions:
            df.loc[
                reduce(lambda x, y: x | y, conditions),
                'exit_long'] = True
        return None

    def populate_scaling(self) -> dict | None:
        """
        -1 - zmniejsz rozmiar pozycji
        0 - brak zmian rozmiaru pozycji
        1 - zwiększ rozmiar pozycji
        """
        df = self.df

        scale_up_long = []
        scale_down_long = []
        scale_up_short = []
        scale_down_short = []
        df.loc[:, 'scale_tag'] = ''
        df['scaling_long'] = 0
        df['scaling_short'] = 0

        scale_up_long_rsi = df['rsi'] < 0  # np. sygnał do powiększenia pozycji long, gdy RSI < 30
        scale_down_long_rsi = df['rsi'] > 100  # zmniejsz pozycję, gdy RSI > 70

        scale_up_long.append(scale_up_long_rsi)
        df.loc[scale_up_long_rsi, 'scale_tag'] += 'scale_up_long_rsi'

        scale_down_long.append(scale_down_long_rsi)
        df.loc[scale_up_long_rsi, 'scale_tag'] += 'scale_down_long_rsi'

        scale_down_short.append(scale_up_long_rsi)
        df.loc[scale_up_long_rsi, 'scale_tag'] += 'scale_down_short_rsi'

        scale_up_short.append(scale_down_long_rsi)
        df.loc[scale_up_long_rsi, 'scale_tag'] += 'scale_up_short_rsi'

        df.loc[reduce(lambda x, y: x | y, scale_up_long), 'scaling_long'] = 1
        df.loc[reduce(lambda x, y: x | y, scale_down_long), 'scaling_long'] = -1
        df.loc[reduce(lambda x, y: x | y, scale_up_short), 'scaling_short'] = 1
        df.loc[reduce(lambda x, y: x | y, scale_down_short), 'scaling_short'] = -1
        return None

    def calculate_custom_sl_tp(self) -> pd.DataFrame:
        """
        Dodaje kolumny custom_sl i custom_tp na podstawie ATR.
        SL i TP liczone są względem ceny zamknięcia.
        """

        df = self.df

        last_low = df['low'].rolling(5).min()
        df['custom_sl_long'] = last_low
        # Long
        df['custom_sl_long'] = None
        df['custom_tp_long'] = None

        long_entries = df['enter_long'] == True
        df.loc[long_entries, 'custom_sl_long'] = last_low
        df.loc[long_entries, 'custom_tp_long'] =  df['close'] + (5 * (df['close'] - last_low))

        last_high = df['high'].rolling(5).max()
        # Short
        df['custom_sl_short'] = None
        df['custom_tp_short'] = None

        short_entries = df['enter_short'] == True
        df.loc[short_entries, 'custom_sl_short'] = last_high
        df.loc[short_entries, 'custom_tp_short'] =  df['close'] - (5 * (last_high - df['close']))

        return df



    def run(self):
        self.populate_indicators()
        self.populate_entry_trend()
        self.populate_scaling()
        self.populate_exit_trend()

        if config.USE_CUSTOM_SL_TP:
            self.calculate_custom_sl_tp()
        return self.df