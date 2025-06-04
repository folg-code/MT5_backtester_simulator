import pandas as pd
from pygments.lexers import q

import TA_function as myTA
import indicators as qtpylib
import talib.abstract as ta
import config
from utils.decorators import informative  # ← NOWE
from utils.data_loader import get_data             # ← NOWE
import MetaTrader5 as mt5
from utils.informative_utils import (
    get_informative_dataframe,
    merge_informative_data,
    populate_informative_indicators
)


class Poi:
    def __init__(self, df: pd.DataFrame, symbol,  startup_candle_count: int = 600):
        self.startup_candle_count = 600
        self.df = df.copy()
        self.symbol = symbol
        self.informative_dataframes = {}



    @informative('H1')
    def populate_indicators_H1(self, df: pd.DataFrame):

        peaks5, fibos5, divs5, bullish_ob, bearish_ob = myTA.find_pivots(df, 5, min_percentage_change=0.001)
        df = pd.concat([df, peaks5, fibos5, divs5], axis=1)

        df['idxx'] = df.index

        bullish_fvg, bearish_fvg = myTA.detect_fvg(df, 1.3)



        bullish_fvg['zone_type'] = 'fvg'
        bullish_ob['zone_type'] = 'ob'
        bearish_fvg['zone_type'] = 'fvg'
        bearish_ob['zone_type'] = 'ob'

        bullish_zones = pd.concat([bullish_fvg, bullish_ob], ignore_index=True).sort_values(by='idxx')
        bearish_zones = pd.concat([bearish_fvg, bearish_ob], ignore_index=True).sort_values(by='idxx')

        bullish_zones_validated_H1, bearish_zones_validated_H1 = myTA.invalidate_zones_by_candle_extremes(df, bullish_zones, bearish_zones,"idxx")

        self.bullish_fvg_validated_H1 = bullish_zones_validated_H1[bullish_zones_validated_H1['zone_type'] == 'fvg'].copy()
        self.bullish_ob_validated_H1 = bullish_zones_validated_H1[bullish_zones_validated_H1['zone_type'] == 'ob'].copy()
        self.bearish_fvg_validated_H1 = bearish_zones_validated_H1[bearish_zones_validated_H1['zone_type'] == 'fvg'].copy()
        self.bearish_ob_validated_H1 = bearish_zones_validated_H1[bearish_zones_validated_H1['zone_type'] == 'ob'].copy()

        
        df['rma_33_low'] = myTA.RMA(df, df['low'], 33)
        df['rma_33_high'] = myTA.RMA(df, df['high'], 33)

        df['rma_144_low'] = myTA.RMA(df, df['low'], 144)
        df['rma_144_high'] = myTA.RMA(df, df['high'], 144)



        return df

    def populate_indicators(self):
        df = self.df
        if 'time_x' in df.columns:
            df.rename(columns={'time_x': 'time'}, inplace=True)

        peaks, fibos, divs, bullish_ob, bearish_ob = myTA.find_pivots(df, 15, min_percentage_change=0.00001)
        df = pd.concat([df, peaks, fibos, divs], axis=1)

        golden_pocket_bullish = pd.DataFrame({
            'time': df['time'],
            'low_boundary': df['fibo2_0660_15'],
            'high_boundary': df['fibo2_0618_15']
        })

        golden_pocket_bearish = pd.DataFrame({
            'time': df['time'],
            'low_boundary': df['fibo2_0660_15_bear'],
            'high_boundary': df['fibo2_0618_15_bear']
        })

        df['golden_pocket_bullish_reaction'] = False
        df['golden_pocket_bearish_reaction'] = False

        df['golden_pocket_bullish_reaction'] = myTA.mark_fibo_reactions(df, golden_pocket_bullish, 'bullish')
        df['golden_pocket_bearish_reaction'] = myTA.mark_fibo_reactions(df, golden_pocket_bearish, 'bearish')


        print(df[(df['golden_pocket_bullish_reaction']) | (df['golden_pocket_bearish_reaction'])])



        bullish_fvg, bearish_fvg = myTA.detect_fvg(df, 1.3)

        bullish_fvg['zone_type'] = 'fvg'
        bullish_ob['zone_type'] = 'ob'
        bearish_fvg['zone_type'] = 'fvg'
        bearish_ob['zone_type'] = 'ob'

        bullish_zones = pd.concat([bullish_fvg, bullish_ob], ignore_index=True).sort_values(by='idxx')
        bearish_zones = pd.concat([bearish_fvg, bearish_ob], ignore_index=True).sort_values(by='idxx')

        df['idxx'] = df.index

        bullish_zones_validated, bearish_zones_validated = myTA.invalidate_zones_by_candle_extremes(df,bullish_zones,bearish_zones, "idxx")

        self.bullish_fvg_validated = bullish_zones_validated[bullish_zones_validated['zone_type'] == 'fvg'].copy()
        self.bullish_ob_validated = bullish_zones_validated[bullish_zones_validated['zone_type'] == 'ob'].copy()
        self.bearish_fvg_validated = bearish_zones_validated[bearish_zones_validated['zone_type'] == 'fvg'].copy()
        self.bearish_ob_validated = bearish_zones_validated[bearish_zones_validated['zone_type'] == 'ob'].copy()

        df['bullish_fvg_reaction'] = False
        df['bullish_ob_reaction'] = False
        df['bearish_fvg_reaction'] = False
        df['bearish_ob_reaction'] = False

        df['bullish_fvg_reaction'] = myTA.mark_zone_reactions("normal", df, self.bullish_fvg_validated,  'bullish', 'time', 'time')
        df['bullish_ob_reaction'] = myTA.mark_zone_reactions("normal", df, self.bullish_ob_validated, 'bullish', 'time', 'time')
        df['bearish_fvg_reaction'] = myTA.mark_zone_reactions("normal", df, self.bearish_fvg_validated,  'bearish', 'time', 'time')
        df['bearish_ob_reaction'] = myTA.mark_zone_reactions("normal", df, self.bearish_ob_validated,  'bearish', 'time', 'time')

        df['bullish_fvg_reaction_H1'] = False
        df['bullish_ob_reaction_H1'] = False
        df['bearish_fvg_reaction_H1'] = False
        df['bearish_ob_reaction_H1'] = False

        df_H1 = df[['open_H1', 'high_H1', 'low_H1', 'close_H1', 'time_H1']].copy()

        df['bullish_fvg_reaction_H1'] = myTA.mark_zone_reactions("aditional", df_H1, self.bullish_fvg_validated_H1,'bullish', 'time_H1', 'time')
        df['bullish_ob_reaction_H1'] = myTA.mark_zone_reactions("aditional", df_H1, self.bullish_ob_validated_H1,'bullish', 'time_H1', 'time')
        df['bearish_fvg_reaction_H1'] = myTA.mark_zone_reactions("aditional", df_H1, self.bearish_fvg_validated_H1,'bearish', 'time_H1', 'time')
        df['bearish_ob_reaction_H1'] = myTA.mark_zone_reactions("aditional", df_H1, self.bearish_ob_validated_H1, 'bearish', 'time_H1', 'time')



        heikinashi = qtpylib.heikinashi(df)
        df[['ha_open', 'ha_close', 'ha_high', 'ha_low']] = heikinashi[['open', 'close', 'high', 'low']]

        self.n1 = 10
        self.n2 = 7
        df = myTA.market_cipher(self, df)
        #df['bands'] = myTA.calculate_vwma(df, 35)
        df['atr_tma'] = ta.ATR(df, timeperiod=154)
        #df['tma_low_bands'] = df['bands'] - (2 * df['atr_tma'])
        #df['tma_high_bands'] = df['bands'] + (2 * df['atr_tma'])
        df['sma_50'] = ta.SMA(df, timeperiod=50)
        df['sma_200'] = ta.SMA(df, timeperiod=200)
        df['atr'] = ta.ATR(df, timeperiod=14)
        df['rsi'] = ta.RSI(df, 14)

        self.df = self.df[self.df['time'] >= pd.to_datetime(config.TIMERANGE['start'])]

        self.bullish_fvg_validated = self.bullish_fvg_validated[self.bullish_fvg_validated['time'] >= pd.to_datetime(config.TIMERANGE['start'])]
        self.bullish_ob_validated = self.bullish_ob_validated[self.bullish_ob_validated['time'] >= pd.to_datetime(config.TIMERANGE['start'])]
        self.bearish_fvg_validated = self.bearish_fvg_validated[self.bearish_fvg_validated['time'] >= pd.to_datetime(config.TIMERANGE['start'])]
        self.bearish_ob_validated = self.bearish_ob_validated[self.bearish_ob_validated['time'] >= pd.to_datetime(config.TIMERANGE['start'])]

        self.bullish_fvg_validated_H1 = self.bullish_fvg_validated_H1[self.bullish_fvg_validated_H1['time'] >= pd.to_datetime(config.TIMERANGE['start'])]
        self.bullish_ob_validated_H1 = self.bullish_ob_validated_H1[self.bullish_ob_validated_H1['time'] >= pd.to_datetime(config.TIMERANGE['start'])]
        self.bearish_fvg_validated_H1 = self.bearish_fvg_validated_H1[self.bearish_fvg_validated_H1['time'] >= pd.to_datetime(config.TIMERANGE['start'])]
        self.bearish_ob_validated_H1 = self.bearish_ob_validated_H1[self.bearish_ob_validated_H1['time'] >= pd.to_datetime(config.TIMERANGE['start'])]



        self.df = df


    def populate_entry_trend(self):
        df = self.df
        df['signal'] = None

        candle_bullish = myTA.candlectick_confirmation(df, 'long')
        candle_bearish = myTA.candlectick_confirmation(df, 'short')



        uptrend_rma = (df['rma_33_low_H1'] > df['rma_144_high_H1'])
        downtrend_rma = (df['rma_33_high_H1'] < df['rma_144_low_H1'])

        price_action_bull_H1 = df['price_action_bull_5_H1']
        price_action_bear_H1 = df['price_action_bear_5_H1']

        price_action_bull = df['price_action_bull_15']
        price_action_bear = df['price_action_bear_15']

        bull_divs_H1 = (df['pivot_bull_div_5_H1'] | df['bull_div_rsi_5_H1'])
        bear_divs_H1 = (df['pivot_bear_div_5_H1'] | df['bear_div_rsi_5_H1'])

        bull_div15 = (df['pivot_bull_div_15'] | df['bull_div_rsi_15'])
        bear_div15 = (df['pivot_bear_div_15'] | df['bear_div_rsi_15'])


        uptrend_long_ob = ((price_action_bull_H1 | price_action_bull) &  df['bullish_ob_reaction']   )
        uptrend_long_fvg = ((price_action_bull_H1 | price_action_bull) & (  df['bullish_fvg_reaction']))

        uptrend_short = (  (bull_div15 |   bull_divs_H1) & (df['bullish_ob_reaction'] | df['bullish_fvg_reaction']) )

        downtrend_short_ob = ((price_action_bear_H1 | price_action_bear) &  (df['bearish_ob_reaction'] ) )
        downtrend_short_fvg = ((price_action_bear_H1 | price_action_bear) & ( df['bearish_fvg_reaction']) )

        downtrend_long = (  (bear_div15 | bear_divs_H1) & (df['bearish_ob_reaction'] | df['bearish_fvg_reaction']) )


        """df['boundary_low'] = df['boundary'].apply(lambda x: x[0] if isinstance(x, tuple) else None)
        df['boundary_high'] = df['boundary'].apply(lambda x: x[1] if isinstance(x, tuple) else None).ffill()

        df['boundary_low'] = df['boundary_low'].ffill()
        df['boundary_high'] = df['boundary_high'].ffill()"""








        #print(df.loc[df['boundary_low'].notna(), 'boundary_low'])
        df['sl_long'] = df['low'].rolling(5).min()
        df['tp_long'] = df['last_high_15']
        df['sl_short'] = df['high'].rolling(5).max()
        df['tp_short'] = df['last_low_15']



        # Dla longów
        df.loc[uptrend_long_ob, 'signal'] = df.loc[uptrend_long_ob].apply(
            lambda row: ("enter", "long", "uptrend_long_ob"), axis=1
        )

        df.loc[uptrend_long_fvg, 'signal'] = df.loc[uptrend_long_fvg].apply(
            lambda row: ("enter", "long", "uptrend_long_fvg"), axis=1
        )

        # Dla shortów
        df.loc[downtrend_short_ob, 'signal'] = df.loc[downtrend_short_ob].apply(
            lambda row: ("enter", "short", "downtrend_short_ob"), axis=1
        )

        df.loc[downtrend_short_fvg, 'signal'] = df.loc[downtrend_short_fvg].apply(
            lambda row: ("enter", "short", "downtrend_short_fvg"), axis=1
        )










        """
        df.loc[df['bullish_ob_reaction'], 'signal'] = df.loc[df['bullish_ob_reaction']].apply(lambda _: ("enter", "long","bullish_ob_reaction"), axis=1)
        df.loc[df['bullish_fvg_reaction'], 'signal'] = df.loc[df['bullish_fvg_reaction']].apply(lambda _: ("enter", "long","bullish_fvg_reaction"), axis=1)
        df.loc[df['bearish_ob_reaction'], 'signal'] = df.loc[df['bearish_ob_reaction']].apply( lambda _: ("enter", "short", "bearish_ob_reaction"), axis=1)
        df.loc[df['bearish_fvg_reaction'], 'signal'] = df.loc[df['bearish_fvg_reaction']].apply(lambda _: ("enter", "short", "bearish_fvg_reaction"),axis=1)
        """


        #df.loc[uptrend_short, 'signal'] = df.loc[downtrend_long].apply(lambda _: ("enter", "long", "downtrend_long"),axis=1)

        #df.loc[downtrend_long, 'signal'] = df.loc[uptrend_short].apply(lambda _: ("enter", "short", "uptrend_short"),axis=1)

        #df.loc[df['bullish_fvg_reaction'], 'signal'] = df.loc[df['bullish_fvg_reaction'] & df['bearish_ob_reaction']].apply(lambda _: ("enter", "long", "fvg_bear_ob_long"), axis=1)




        return None

    def populate_exit_trend(self):
        df = self.df
        TMA_exit_long_hodl = (
            (df['red_dot']) &
            (df['close'] < df['sma_50']) &
            (df['sma_200'] > df['sma_200'].shift(5))
        )
        #df.loc[TMA_exit_long_hodl, 'signal'] = df.loc[TMA_exit_long_hodl].apply(lambda _: ("exit", "long","TMA_exit_long_hodl"), axis=1)
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
                action, direction, tag = signal
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

    def get_bullish_zones(self):
        return [
            ("Bullish FVG H1", self.bullish_fvg_validated_H1, "rgba(76, 175, 80, 0.4)"),
            ("Bullish OB H1", self.bullish_ob_validated_H1, "rgba(56, 142, 60, 0.4)"),
        ]

    def get_bearish_zones(self):
        return [
            ("Bearish FVG H1", self.bearish_fvg_validated_H1, "rgba(183, 28, 28, 0.4)"),
            ("Bearish OB H1", self.bearish_ob_validated_H1, "rgba(198, 40, 40, 0.4)"),
        ]

    def get_extra_values_to_plot(self):
        return [
            ("fibo2_0660_15", self.df["fibo2_0660_15"], "blue", "dash"),
            ("fibo2_0618_15", self.df["fibo2_0618_15"], "blue", "dot"),
            ("fibo2_0660_15_bear", self.df["fibo2_0660_15_bear"], "red", "dash"),
            ("fibo2_0618_15_bear", self.df["fibo2_0618_15_bear"], "red", "dot"),
        ]

    def run(self) -> pd.DataFrame:

        populate_informative_indicators(self)
        self.populate_indicators()



        self.populate_entry_trend()
        self.populate_exit_trend()
        self.populate_scaling()
        self.calculate_custom_sl_tp()




        self.df_plot = self.df.copy()

        # Przygotuj okrojoną wersję do backtestu
        self.df_backtest = self.df[['time', 'open', 'high', 'low', 'close', 'signal']].copy()

        return self.df_backtest

