import pandas as pd
import TA_function as myTA
import indicators as qtpylib
import talib.abstract as ta
import strategies.poi_utils as smc
import config
from utils.decorators import informative  # ← NOWE
from utils.df_trimmer import trim_all_dataframes             # ← NOWE
import MetaTrader5 as mt5
from utils.informative_utils import (
    get_informative_dataframe,
    merge_informative_data,
    populate_informative_indicators,
)


class Poi:
    def __init__(self, df: pd.DataFrame, symbol,  startup_candle_count: int = 600):
        self.startup_candle_count = 600
        self.df = df.copy()
        self.symbol = symbol
        self.informative_dataframes = {}

    



    @informative('H1')
    def populate_indicators_H1(self, df: pd.DataFrame):

        peaks, fibos, divs, bullish_ob, bearish_ob = myTA.find_pivots(df, 15, min_percentage_change=0.001)
        df = pd.concat([df, peaks, fibos, divs], axis=1)

        df['idxx'] = df.index

        bullish_fvg, bearish_fvg = myTA.detect_fvg(df, 1.5)

        bullish_fvg['zone_type'] = 'fvg'
        bullish_ob['zone_type'] = 'ob'
        bearish_fvg['zone_type'] = 'fvg'
        bearish_ob['zone_type'] = 'ob'

        self.bullish_zones_H1 = pd.concat([bullish_fvg, bullish_ob], ignore_index=True).sort_values(by='idxx')
        self.bearish_zones_H1 = pd.concat([bearish_fvg, bearish_ob], ignore_index=True).sort_values(by='idxx')

        bullish_zones_validated_H1, bearish_zones_validated_H1 = myTA.invalidate_zones_by_candle_extremes("normal", df, self.bullish_zones_H1, self.bearish_zones_H1,"idxx")

        self.bullish_fvg_validated_H1 = bullish_zones_validated_H1[bullish_zones_validated_H1['zone_type'] == 'fvg'].copy()
        self.bullish_ob_validated_H1 = bullish_zones_validated_H1[bullish_zones_validated_H1['zone_type'] == 'ob'].copy()
        self.bearish_fvg_validated_H1 = bearish_zones_validated_H1[bearish_zones_validated_H1['zone_type'] == 'fvg'].copy()
        self.bearish_ob_validated_H1 = bearish_zones_validated_H1[bearish_zones_validated_H1['zone_type'] == 'ob'].copy()

        

        
        df['rma_33_low'] = myTA.RMA(df, df['low'], 33)
        df['rma_33_high'] = myTA.RMA(df, df['high'], 33)

        df['rma_144_low'] = myTA.RMA(df, df['low'], 144)
        df['rma_144_high'] = myTA.RMA(df, df['high'], 144)

        df['atr'] = ta.ATR(df, timeperiod=14)
        heikinashi = qtpylib.heikinashi(df)
        df[['ha_open', 'ha_close', 'ha_high', 'ha_low']] = heikinashi[['open', 'close', 'high', 'low']]

        df['candle_bullish'] = myTA.candlectick_confirmation(df, 'long')

        df['liquidity_swep'] = ((myTA.check_reaction(df, df['LL_15']) & (df['LL_15_idx'] > df['HL_15_idx'])) | (myTA.check_reaction(df, df['HL_15']) & (df['LL_15_idx'] < df['HL_15_idx'])))

        df = myTA.calculate_monday_high_low(df)

        df['monday_low_reaction'] = ((myTA.check_reaction(df, df['monday_low'])) & (df['weekday'] != 0))




        return df

    def populate_indicators(self):


        smc._prepare_dataframe(self)
        smc._calculate_indicators(self)
        smc._mark_fibo_reactions_all(self)
        smc._detect_and_validate_zones(self)
        smc._mark_zone_reactions(self)
        smc._process_higher_timeframe_zones(self)
        smc._mark_sweeps(self)

    def populate_entry_trend(self):
        df = self.df
        df = df.copy()

    
        df['signal'] = None

        GPG_diff_ =  myTA.diff_percentage(df['close'], self.GPG_bullish_H1['low_boundary'])
        df[f'GPG_diff_H1'] = ((GPG_diff_ > 0) & (GPG_diff_ < (df['atr'] * 2 ))& (GPG_diff_ < (df['atr'] * 3 )))

        local_min = df['low'].rolling(15).min()

        adx_cond = (((df['minus_di'] > df['plus_di']) & (df['atr'] < df['atr_sma']))|
                    ((df['minus_di'] < df['plus_di']) & (df['atr'] > df['atr_sma'])))
        rsi_cond = ((df['rsi'] < 35) | (df['rsi'] > df['rsi_sma']) | df['bull_div_rsi_15'] == True)
        macd_cond = (df['macd_line'] > df['macd_signal'])

        
        consolidation = myTA.diff_percentage(df['close'], local_min)  
        consolidation_condition1 = ((consolidation > 0) & (consolidation < (df['atr'] * 3 )))
        
        price_action_bull =  df['price_action_bull_15']

        uptrend_long_zones = (df['bullish_fvg_reaction'] |  df['bullish_ob_reaction']   )
        uptrend_long_zones_H1 = (df['bullish_fvg_reaction_H1'] |  df['bullish_ob_reaction_H1']   )

        candle_bullish_15 = myTA.candlectick_confirmation(df, 'long')

        candle_bullish = (candle_bullish_15 )
        candle_bearish = myTA.candlectick_confirmation(df, 'short')

        uptrend_rma = (df['rma_33_low_H1'] > df['rma_144_high_H1'])
        downtrend_rma = (df['rma_33_high_H1'] < df['rma_144_low_H1'])

        PA_H1_MAU = (df['price_action_bull_15_H1'] & uptrend_rma)

        bull_breaker = (df['bullish_breaker_reaction'] | df['bullish_breaker_reaction_H1'])
        FVG = (df['bullish_fvg_reaction'] | df['bullish_fvg_reaction_H1'])

        H1 = (df['bullish_fvg_reaction_H1'] | df['bullish_breaker_reaction_H1'])

        low_tf_zones = (df['bullish_fvg_reaction'] |  df['bullish_ob_reaction'] | df['bullish_breaker_reaction']   )

        additional_check=   (   price_action_bull | df['liquidity_swep'])
        

        FVG_H1__BullB__PA_H1_ = (df['bullish_fvg_reaction_H1'] & df['bullish_breaker_reaction'] & df['price_action_bull_15']  )

        FVG_H1__GPL_H1__GPG_H1_ = (df['bullish_fvg_reaction_H1'] & (df['GPL_bullish_reaction_H1'] & df['GPG_bullish_reaction_H1'])  )

        
        local_min = df['low'].rolling(15).min()

        sl = local_min - df['atr']
        
        entries = {idx: [] for idx in df.index}

        def add_signal(mask, signal_tuple):


            indexes = df.index[mask]
            for i in indexes:
                entries[i].append(signal_tuple)

        
        # Przykładowe dodawanie sygnałów (rozwiń wg potrzeb)
        add_signal(df['bullish_ob_reaction'] & additional_check & price_action_bull, ("enter", "long", "OB_"))
        #add_signal(df['bullish_fvg_reaction'] & additional_check , ("enter", "long", "FVG_"))
        #add_signal(df['GPL_bullish_reaction'] & additional_check , ("enter", "long", "GPL_"))
        #add_signal(df['GPG_bullish_reaction'] & additional_check , ("enter", "long", "GPG_"))
        #add_signal(df['bullish_gap_reaction'] & additional_check , ("enter", "long", "GAP_H1"))

        add_signal(df['bullish_ob_reaction_H1'] & additional_check & candle_bullish , ("enter", "long", "OB_H1_"))
        add_signal(df['bullish_fvg_reaction_H1'] & additional_check & candle_bullish , ("enter", "long", "FVG_H1_"))

        #add_signal(df['GPL_bullish_reaction_H1'] & additional_check , ("enter", "long", "GPL_H1_"))
        add_signal(df['GPG_bullish_reaction_H1'] & additional_check & candle_bullish , ("enter", "long", "GPG_H1_"))

        add_signal(df['bullish_breaker_reaction_H1'] & additional_check & candle_bullish , ("enter", "long", "BullB_H1_"))
        #add_signal(df['bullish_breaker_reaction'] & additional_check , ("enter", "long", "BullB_"))

        #add_signal(df['liquidity_swep'] & additional_check , ("enter", "long", "LQS_"))
        add_signal(df['liquidity_swep_H1'] & additional_check , ("enter", "long", "LQS_H1"))

        #add_signal(df['monday_low_reaction'] & additional_check , ("enter", "long", "MonS_"))
        add_signal(df['monday_low_reaction_H1'] & additional_check , ("enter", "long", "MonS_H1_"))
        #add_signal(price_action_bull & additional_check, ("enter", "long", "PA_"))
        
        
        #add_signal(FVG_H1__BullB__PA_H1_ & additional_check, ("enter", "long", "FVG_H1__BullB__PA_H1_"))
        #add_signal(FVG_H1__GPL_H1__GPG_H1_ & additional_check, ("enter", "long", "FVG_H1__GPL_H1__GPG_H1_"))

        def safe_merge_signals(signals):
            if not signals:
                return None
            return merge_signals(signals)

        # Zamiast map - używamy apply, żeby zwrócić None lub tuple i uniknąć problemów z MultiIndex
        df['signal'] = df.index.to_series().apply(lambda i: safe_merge_signals(entries[i]))


        #long = {PA__PA_H1__MAD_ : PA__C__MAU_}

        self.df = df




        return None

    def populate_exit_trend(self):
        df = self.df
        TMA_exit_long_hodl = (
            (df['close'] > df['HH_15']) 
        )
        df.loc[TMA_exit_long_hodl, 'signal'] = df.loc[TMA_exit_long_hodl].apply(lambda _: ("exit", "long","TMA_exit_long_hodl"), axis=1)
        return None


    def populate_scaling(self):
        df = self.df

        df['scaling'] = None

        """


        #df.loc[scale_up, 'scaling'] = df.loc[scale_up].apply(lambda _: ("scale", "long", 1), axis=1)
        #df.loc[scale_down, 'scaling'] = df.loc[scale_down].apply(lambda _: ("scale", "long", -0.5), axis=1)

        # === WARUNKI DODATKOWE: PARTIAL TAKE PROFIT ===
        for idx, row in df.iterrows():
            signal = row['signal']

            if isinstance(signal, tuple) and len(signal) >= 3:
                action, direction, tag = signal

                if direction == "long":
                    entry_price = row['close'] if action == "enter" else None
                    current_price = row['close']
                    

                    # Procentowy target (np. 2% zysku)
                    percent_gain = 0.02  # 2%
                    price_target_pct = entry_price * (1 + percent_gain)

                    # ATR-based target
                    price_target_atr = entry_price + row['atr']

                    # Jeśli któryś warunek spełniony → częściowa realizacja
                    if current_price >= price_target_pct or current_price >= price_target_atr:
                        df.at[idx, 'scaling'] = ("scale", "long", -0.3, "partial_tp")  # zamknij 30% pozycji

                elif direction == "short":
                    entry_price = row['close'] if action == "enter" else None
                    current_price = row['close']

                    percent_gain = 0.02
                    price_target_pct = entry_price * (1 - percent_gain)
                    price_target_atr = entry_price - row['atr']

                    if current_price <= price_target_pct or current_price <= price_target_atr:
                        df.at[idx, 'scaling'] = ("scale", "short", -0.3, "partial_tp")"""

        

        self.df = df

    def calculate_custom_sl_tp(self) -> pd.DataFrame:


        df = self.df
        df = df.copy()

        rr=5.0

                    # Calculate rolling min/max on the entire Series first
        rolling_low = self.df['low'].rolling(10).min()
        rolling_high = self.df['high'].rolling(10).max()

        for idx, row in df.iterrows():
            signal = row['signal']
            if not (isinstance(signal, tuple) and len(signal) >= 3):
                continue

            action, direction, tag = signal[:3]
            


            last_low = rolling_low[idx]
            last_high = rolling_high[idx]
            close = row['close']
            potential_sls = []
            potential_TP = []

            # === LONG ===
            if action == "enter":
                if direction == "long":
                    if row.get('bullish_breaker_reaction_H1', False):
                        val = row.get('bullish_breaker_low_H1')
                        if pd.notna(val):
                            if close >= val:
                                potential_sls.append(('SL_breaker_H1', val))

                    if row.get('bullish_ob_reaction_H1', False):
                        val = row.get('bullish_ob_low_H1')
                        if pd.notna(val):
                            if close >= val:
                                potential_sls.append(('SL_ob_H1', val))

                    if row.get('bullish_fvg_reaction_H1', False):
                        val = row.get('bullish_fvg_low_H1')
                        if pd.notna(val):
                            if close >= val:
                                potential_sls.append(('SL_fvg_H1', val))

                    if row.get('GPL_bullish_reaction_H1', False):
                        val = row.get('fibo_local_0660_15_H1')
                        if pd.notna(val):
                            if close >= val:
                                potential_sls.append(('SL_ob_H1', val))

                    if row.get('GPG_bullish_reaction_H1', False):
                        val = row.get('fibo_global_0660_15_H1')
                        if pd.notna(val):
                            if close >= val:
                                potential_sls.append(('SL_fvg_H1', val))

                    if potential_sls:
                        # wybieramy najniższy SL
                        # Wybór najniższego SL
                        sl_exit_tag, sl = min(potential_sls, key=lambda x: x[1])

                        # Zabezpieczenie fallback, jeśli SL jest wyżej niż cena
                        if sl >= close:
                            sl = close - row['atr']
                            sl_exit_tag = 'SL_ATR'

                        # Oblicz różne opcje TP
                        tp_rr = close + rr * (close - sl)
                        tp_atr = close + 5 * row['atr']
                        tp_hh = row.get('HH_15') if pd.notna(row.get('HH_15')) else None

                        # Wybierz najniższy dostępny TP, większy od close
                        tp_candidates = [tp for tp in [tp_rr, tp_atr, tp_hh] if tp is not None and tp > close]
                        if tp_candidates:
                            tp = min(tp_candidates)
                            # Nadaj etykietę TP w zależności od źródła
                            if tp == tp_rr:
                                tp_exit_tag = f"TP_{sl_exit_tag.split('_')[1]}"
                            elif tp == tp_atr:
                                tp_exit_tag = "TP_ATR"
                            else:
                                tp_exit_tag = "TP_HH"
                        else:
                            # fallback jeśli wszystko zawiedzie
                            tp = tp_rr
                            tp_exit_tag = f"TP_{sl_exit_tag.split('_')[1]}"

                        # Przypisz pełny sygnał
                        df.at[idx, 'signal'] = (
                            "enter", "long", tag, sl, tp, sl_exit_tag, tp_exit_tag
                        )
                    final_signal = df.at[idx, 'signal']
                if not (isinstance(final_signal, tuple) and len(final_signal) == 7):
                    # fallback - wymuszenie poprawnego formatu
                    sl = last_low - row['atr']
                    tp = close + rr * (close - sl)
                    df.at[idx, 'signal'] = (
                        "enter", direction, tag, sl, tp, "SL_fallback", "TP_fallback"
                    )
                    #print(f"[FALLBACK WYMUSZONY] idx={idx}, signal={df.at[idx, 'signal']}")

                # === SHORT ===
                elif direction == "short":
                    if row.get('bearish_breaker_reaction_H1', False):
                        val = row.get('bearish_breaker_high_H1')
                        if pd.notna(val):
                            potential_sls.append(('SL_breaker_H1', val))

                    if row.get('bearish_ob_reaction_H1', False):
                        val = row.get('bearish_ob_high_H1')
                        if pd.notna(val):
                            potential_sls.append(('SL_ob_H1', val))

                    if row.get('bearish_fvg_reaction_H1', False):
                        val = row.get('bearish_fvg_high_H1')
                        if pd.notna(val):
                            potential_sls.append(('SL_fvg_H1', val))

                    if potential_sls:
                        # wybieramy najwyższy SL
                        sl_exit_tag, sl = max(potential_sls, key=lambda x: x[1])
                        if sl > close:
                            tp = close - rr * (sl - close)
                            tp_exit_tag = f"TP_{sl_exit_tag.split('_')[1]}"
                            df.at[idx, 'signal'] = (
                                "enter", "short", tag, sl, tp, sl_exit_tag, tp_exit_tag
                            )

        if isinstance(df['signal'], tuple) and len(df['signal']) < 7:
            print(f"[UWAGA] Niepełny sygnał : {df['signal']}")

        return df
    
    def get_bullish_zones(self):
        return [

            ("Bullish FVG H1", self.bullish_fvg_validated_H1, "rgba(255, 152, 0, 0.7)"),       # Pomarańcz
            #("Bullish FVG ", self.bullish_fvg_validated, "rgba(255, 152, 0, 0.7)"),  
            ("Bullish OB H1", self.bullish_ob_validated_H1, "rgba(56, 142, 60, 0.7)"),
            #("Bullish OB ", self.bullish_ob_validated, "rgba(56, 142, 60, 0.7)"),          # Zielony (bez zmian)
            ("Bullish Breaker H1", self.bullish_breaker_validated_H1, "rgba(33, 150, 243, 0.7)"), # Niebieski
            #("Bullish Breaker ", self.bullish_breaker_validated, "rgba(33, 150, 243, 0.7)"),

            #("Bullish GAP ", self.bullish_gap_validated, "rgba(33, 150, 243, 0.7)"),


        ]

    def get_bearish_zones(self):
        return [
            #("Bearish Breaker", self.bearish_breaker_validated, "rgba(183, 28, 28, 0.7)"),
            #("Bearish OB ", self.bearish_ob_validated, "rgba(198, 40, 40, 0.7)"),
            #("Bearish GAP", self.bearish_gap_validated, "rgba(183, 28, 28, 0.7)"),
            #("Bearish OB H1", self.bearish_ob_validated_H1, "rgba(198, 40, 40, 0.7)"),
        ]

    def get_extra_values_to_plot(self):
        return [
            ("fibo_global_0660_15_H1", self.df["fibo_global_0660_15_H1"], "yellow", "dot"),
            ("fibo_global_0618_15_H1", self.df["fibo_global_0618_15_H1"], "yellow", "dot"),
            ("Monday Low", self.df["monday_low_H1"], "blue", "dash"),
            ("Monday High", self.df["monday_high_H1"], "purple", "dash"),
            ("fibo_local_0660_15_H1", self.df["fibo_local_0660_15_H1"], "green", "dot"),
            ("fibo_local_0618_15_H1", self.df["fibo_local_0618_15_H1"], "green", "dot"),
            #("fibo_local_0660_15_bear", self.df["fibo_local_0660_15_bear"], "red", "dash"),
            #("fibo_local_0618_15_bear", self.df["fibo_local_0618_15_bear"], "red", "dash"),
        ]
    
    def bool_series(self):
        return [
            ("price_action_bull_15_H1", self.df['price_action_bull_15_H1'], "purple"),
            ("price_action_bull_15", self.df['price_action_bull_15'], "blue"),
            ]
        

    def run(self) -> pd.DataFrame:

        populate_informative_indicators(self)
        self.populate_indicators()
        trim_all_dataframes(self)
        self.populate_entry_trend()

        self.populate_exit_trend()
        self.populate_scaling()
        self.df = self.calculate_custom_sl_tp()
        self.df_plot = self.df.copy()

        # Przygotuj okrojoną wersję do backtestu
        self.df_backtest = self.df[['time', 'open', 'high', 'low', 'close', 'signal','scaling']].copy()

        return self.df_backtest

def merge_signals(signals_list):
    if not signals_list:
        return None
    
    # Extract components from first signal
    enter = signals_list[0][0]
    direction = signals_list[0][1]

    
    
    # Combine tags
    tags = [s[2] for s in signals_list]

    combined_tag = "_".join(tags)
    
 
  
    
    return (enter, direction, combined_tag)