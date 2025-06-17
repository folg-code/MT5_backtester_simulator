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
from collections import defaultdict



class Poi:
    def __init__(self, df: pd.DataFrame, symbol,  startup_candle_count: int = 600):
        self.startup_candle_count = 600
        self.df = df.copy()
        self.symbol = symbol
        self.informative_dataframes = {}

    



    @informative('H1')
    def populate_indicators_H1(self, df: pd.DataFrame):

        peaks, fibos, bullish_ob, bearish_ob = myTA.find_pivots(df, 15, min_percentage_change=0.001)
        df = pd.concat([df, peaks, fibos], axis=1)

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

        df['bullish_lqs'] = ((myTA.check_reaction(df, df['LL_15'], 'bullish') ) | (myTA.check_reaction(df, df['HL_15'], 'bullish') & (df['LL_15_idx'] < df['HL_15_idx'])))

        df = myTA.calculate_monday_high_low(df)

        df['bullish_mons_reaction'] = ((myTA.check_reaction(df, df['monday_low'], 'bullish')) & (df['weekday'] != 0))




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
        print("initiate populate_entry_trend")
        df = self.df.copy()

        df['signal_entry'] = None
        price_action_bull =  df['price_action_bull_15']

        bearish_lqs = (df['EQH'].rolling(5).sum() == False)
        candle_bullish = myTA.candlectick_confirmation(df, 'long')
        low_tf_zones = (df['bullish_fvg_reaction'] |  df['bullish_ob_reaction'] | df['bullish_breaker_reaction']   )
        additional_check=   (   price_action_bull |  df['bullish_lqs'] | df['EQL']) 
        local_min = df['low'].rolling(15).min() 
        df['local_min'] = local_min
        sl_atr = local_min - df['atr']
        df['sl_atr'] = sl_atr
        df['double_HH_15'] = (df['close'] + ((df['HH_15'] - df['close']) * 2)   )

        last_low = df['low'].rolling(5).min()
        low_guard = df['close'] - last_low < df['atr']
        
        HTF_zones = (df['bullish_fvg_is_in_H1'] |  df['bullish_ob_is_in_H1'] 
                    |df['bullish_breaker_is_in_H1'] | df['bullish_gap_is_in']
                    |df['bullish_GPG_is_in_H1'] |  df['bullish_GPL_is_in_H1']   )
        
        is_in_zone_solo = additional_check  & low_guard & bearish_lqs & price_action_bull
        H1_is_in_solo = [ "bullish_breaker_is_in_H1", "bullish_breaker_is_in_H1",'bullish_GPG_is_in_H1','bullish_gap_is_in']



        #@fvg_h1_is_in

        entries = defaultdict(list)
        levels = defaultdict(list)

        zone_names_H1_reaction = ['bullish_fvg_reaction_H1', "bullish_breaker_reaction_H1",'bullish_GPG_reaction_H1','bullish_GPL_reaction_H1','bullish_gap_reaction']

        zone_names_H1_is_in = [ "bullish_breaker_is_in_H1", "bullish_breaker_is_in_H1",'bullish_GPG_is_in_H1','bullish_gap_is_in']
        zone_names = ['bullish_fvg_reaction','bullish_ob_reaction' ,"bullish_breaker_reaction"]
        levels_names = ["bullish_lqs_H1","bullish_mons_reaction_H1","bullish_mons_reaction"]


        for i, row in df.iterrows():
            idx = row.name  # Prawdziwy indeks w DataFrame

            for zone in zone_names:
                if row.get(zone) and additional_check[i] and price_action_bull[i] and bearish_lqs[i] :
                    tag = zone.replace("bullish_", "").replace("_reaction", "").upper()
                    direction = "long" if "bullish" in zone else "short"

                
                    zone_base = zone.replace("_reaction", "_low") if "bullish" in zone else zone.replace("_reaction", "_high")
                    # Upewnij się, że kolumna istnieje
                    zone_level = row.get(zone_base)
                    if zone_level is None:
                        print(f"⚠️ Brak kolumny: {zone_base} w df")
                        continue
                    # Dodanie sygnału wejścia
                    entries[idx].append(( direction, tag))

                    # Kandydaci na poziomy
                    level_options = {
                        "tag": tag,
                        "sl_candidates": [("zone_level", zone_level)],
                        "tp1_candidates": [("HH_15", row["HH_15"]), ("TP1_ATR", row["close"] + row["atr"])],
                        "tp2_candidates": [("HH_15_H1", row["HH_15_H1"]),("double_HH_15", row["double_HH_15"]), ("RR_3", row["close"] + 5 * row["atr"])]
                    }
                    # Dodanie poziomów
                    levels[idx].append(level_options)
            
            for zone_H1 in zone_names_H1_reaction:
                if row.get(zone_H1) and additional_check[i]  and low_guard[i] :
                    tag = zone_H1.replace("bullish_", "").replace("_reaction", "").upper()
                    direction = "long" if "bullish" in zone_H1 else "short"
                    

                    # Poziom strefy (np. bullish_fvg_low)
                    zone_base = zone_H1.replace("_reaction", "_low") if "bullish" in zone_H1 else zone_H1.replace("_reaction", "_high")

                    # Upewnij się, że kolumna istnieje
                    zone_level = row.get(zone_base)
                    if zone_level is None:
                        print(f"⚠️ Brak kolumny: {zone_base} w df")
                        continue
                    # Dodanie sygnału wejścia
                    entries[idx].append((direction, tag))

                    # Kandydaci na poziomy
                    level_options = {
                        "tag": tag,
                        "sl_candidates": [("zone_level", zone_level),("sl_atr", row["sl_atr"])],
                        "tp1_candidates": [("HH_15", row["HH_15"]), ("TP1_ATR", row["close"] +  row["atr"])],
                        "tp2_candidates": [("HH_15_H1", row["HH_15_H1"]),("double_HH_15", row["double_HH_15"]), ("RR_3", row["close"] + 3 * row["atr"])]
                    }
                    # Dodanie poziomów
                    levels[idx].append(level_options)

            for zone_H1_is_in in zone_names_H1_is_in:
                if row.get(zone_H1_is_in) and additional_check[i]  and low_guard[i] and bearish_lqs[i] and price_action_bull[i]:
                    tag = zone_H1_is_in.replace("bullish_", "").replace("_reaction", "").upper()
                    direction = "long" if "bullish" in zone_H1_is_in else "short"
                    

                    # Poziom strefy (np. bullish_fvg_low)
                    zone_base = zone_H1_is_in.replace("_is_in", "_low") if "bullish" in zone_H1_is_in else zone_H1_is_in.replace("_is_in", "_high")

                    # Upewnij się, że kolumna istnieje
                    zone_level = row.get(zone_base)
                    if zone_level is None:
                        print(f"⚠️ Brak kolumny: {zone_base} w df")
                        continue
                    # Dodanie sygnału wejścia
                    entries[idx].append((direction, tag))

                    # Kandydaci na poziomy
                    level_options = {
                        "tag": tag,
                        "sl_candidates": [("zone_level", zone_level),("sl_atr", row["sl_atr"])],
                        "tp1_candidates": [("HH_15", row["HH_15"]), ("TP1_ATR", row["close"] +  row["atr"])],
                        "tp2_candidates": [("HH_15_H1", row["HH_15_H1"]),("double_HH_15", row["double_HH_15"]), ("RR_3", row["close"] + 3 * row["atr"])]
                    }
                    # Dodanie poziomów
                    levels[idx].append(level_options)
                    
            for zone in levels_names:
                if row.get(zone) and additional_check[i] and candle_bullish[i]:
                    tag = zone.replace("reaction", "")  # np. bullish_fvg_
                    tag = zone.replace("bullish", "B")  # np. bullish_fvg_
                    direction = "long" if "bullish" in zone else "short"

                    # Poziom strefy (np. bullish_fvg_low)
                    zone_base = zone.replace("_reaction", "_low") if "bullish" in zone else zone.replace("_reaction", "_high")

                    # Dodanie sygnału wejścia
                    entries[idx].append((direction, tag))

                    # Kandydaci na poziomy
                    level_options = {
                        "tag": tag,
                        "sl_candidates": [("last_low_15", row["local_min"]),("sl_atr", row["sl_atr"])],
                        "tp1_candidates": [("HH_15", row["HH_15"]), ("TP1_ATR", row["close"] + row["atr"])],
                        "tp2_candidates": [("HH_15_H1", row["HH_15_H1"]), ("RR_3", row["close"] + 5 * row["atr"])]
                    }
                    # Dodanie poziomów
                    levels[idx].append(level_options)
            
            
            direction = None
            if entries.get(i) and isinstance(entries[i][0], tuple):
                direction = entries[i][0][0]


            level_result = None
            if entries.get(i):
                level_result = merge_levels(
                    levels[i],
                    direction=direction,
                    close_price=df.loc[i, "close"]
                )


        df["signal_entry"] = [merge_signals(entries.get(i)) if entries.get(i) else None for i in df.index]

        
            

        df["levels"] = [
        merge_levels(
            levels.get(i, []),
            direction=entries.get(i, [(None, None)])[0][0] if entries.get(i) and isinstance(entries[i][0], tuple) else None,
            close_price=df.loc[i, "close"]
        ) if entries.get(i) else None for i in df.index
    ]

        self.df = df
        return None

    def populate_exit_trend(self):

        print("initiate populate_exit_trend")
        df = self.df

        df['signal_exit'] = None
        
        allowed_enter_tags = ['FVG', 'OB']


        
        df.loc[df['bearish_mons_reaction'], 'signal_exit'] = df.loc[df['bearish_mons_reaction']].apply(
            lambda _: ("long", allowed_enter_tags, "bearish_mons_reaction"), axis=1
        )
        df.loc[df['bearish_lqs'], 'signal_exit'] = df.loc[df['bearish_lqs']].apply(
            lambda _: ("long", allowed_enter_tags, "bearish_lqs"), axis=1
        )


   
    
    def get_bullish_zones(self):
        return [

            ("Bullish FVG H1", self.bullish_fvg_validated_H1, "rgba(255, 152, 0, 0.3)"),       # Pomarańcz
            ("Bullish FVG ", self.bullish_fvg_validated, "rgba(255, 152, 0, 0.7)"),  
            ("Bullish OB H1", self.bullish_ob_validated_H1, "rgba(56, 142, 60, 0.3)"),
            ("Bullish OB ", self.bullish_ob_validated, "rgba(56, 142, 60, 0.7)"),          # Zielony (bez zmian)
            ("Bullish Breaker H1", self.bullish_breaker_validated_H1, "rgba(33, 150, 243, 0.3)"), # Niebieski
            ("Bullish Breaker ", self.bullish_breaker_validated, "rgba(33, 150, 243, 0.7)"),

            ("Bullish GAP ", self.bullish_gap_validated, "rgba(33, 150, 243, 0.7)"),


        ]

    def get_bearish_zones(self):
        return [
            ("Bearish Breaker", self.bearish_breaker_validated, "rgba(183, 28, 28, 0.7)"),
            ("Bearish Breaker H1", self.bearish_breaker_validated_H1, "rgba(183, 28, 28, 0.3)"),
            #("Bearish OB ", self.bearish_ob_validated, "rgba(198, 40, 40, 0.7)"),
            #("Bearish GAP", self.bearish_gap_validated, "rgba(183, 28, 28, 0.7)"),
            ("Bearish OB ", self.bearish_ob_validated, "rgba(183, 28, 28, 0.7)"),
            ("Bearish OB H1", self.bearish_ob_validated_H1, "rgba(183, 28, 28, 0.3)"),
        ]

    def get_extra_values_to_plot(self):
        return [
            ("fibo_global_0660_15_H1", self.df["fibo_global_0660_15_H1"], "yellow", "dot"),
            ("fibo_global_0618_15_H1", self.df["fibo_global_0618_15_H1"], "yellow", "dot"),
            ("Monday Low", self.df["monday_low_H1"], "blue", "dash"),
            ("Monday High", self.df["monday_high_H1"], "purple", "dash"),
            ("fibo_local_0660_15_H1", self.df["fibo_local_0660_15_H1"], "green", "dot"),
            ("fibo_local_0618_15_H1", self.df["fibo_local_0618_15_H1"], "green", "dot"),

            #("fibo_local_1272_15_H1", self.df["fibo_local_1272_15_H1"], "red", "dot"),
            #("fibo_global_1272_15_H1", self.df["fibo_global_1272_15_H1"], "red", "dot"),

            #("fibo_local_1272_15", self.df["fibo_local_1272_15"], "purple", "dot"),
           # ("fibo_global_1272_15", self.df["fibo_global_1272_15"], "purple", "dot"),

            #("fibo_local_0660_15_bear", self.df["fibo_local_0660_15_bear"], "red", "dash"),
            #("fibo_local_0618_15_bear", self.df["fibo_local_0618_15_bear"], "red", "dash"),
        ]
    
    def bool_series(self):
        return [
            ("EQH", self.df['EQH'], "purple"),
            ("price_action_bull_15", self.df['price_action_bull_15'], "blue"),
            ("trigger_eqh_hh", self.df['trigger_eqh_hh'], "red"),
            ("trigger_eqh_lh", self.df['trigger_eqh_lh'], "green"),
            ]
        

    def run(self) -> pd.DataFrame:

        populate_informative_indicators(self)
        self.populate_indicators()
        trim_all_dataframes(self)
        self.populate_entry_trend()
        self.populate_exit_trend()
        self.df_plot = self.df.copy()

        # Przygotuj okrojoną wersję do backtestu
        self.df_backtest = self.df[['time', 'open', 'high', 'low', 'close', 'signal_entry', 'signal_exit','levels']].copy()

        return self.df_backtest
    
def merge_signals(signal_list):
    if not signal_list:
        return None
    direction = signal_list[0][0]
    reasons = sorted(set(sig[1] for sig in signal_list))
    merged_reason = "_".join(reasons)
    return (direction, merged_reason)

def merge_levels(level_list, direction="long", close_price=None):

    if not level_list:
        return None

    sl_all = []
    tp1_all = []
    tp2_all = []
    tags = []

    for level in level_list:
        tags.append(level["tag"])
        sl_all.extend(level["sl_candidates"])
        tp1_all.extend(level["tp1_candidates"])
        tp2_all.extend(level["tp2_candidates"])

    combined_tag = "_".join(sorted(set(tags)))


    # Filtracja TP względem ceny zamknięcia
    if close_price is not None:
        if direction == "long":
            tp1_all = [tp for tp in tp1_all if (tp[1] > close_price )]
            tp2_all = [tp for tp in tp2_all if( tp[1] > close_price )]
        else:
            tp1_all = [tp for tp in tp1_all if tp[1] < close_price]
            tp2_all = [tp for tp in tp2_all if tp[1] < close_price]

    



    if not tp1_all or not tp2_all:
        print("No TP candidates left after filtering - returning None")
        return None
        

    if direction == "long":
        sl_final = min(sl_all, key=lambda x: x[1])
        tp1_final = min(tp1_all, key=lambda x: x[1])
        tp2_final = min(tp2_all, key=lambda x: x[1])
    else:
        sl_final = max(sl_all, key=lambda x: x[1])
        tp1_final = min(tp1_all, key=lambda x: x[1])
        tp2_final = max(tp2_all, key=lambda x: x[1])


    return (
        ("SL", sl_final[1], f"SL_{sl_final[0]}_{combined_tag}"),
        ("TP", tp1_final[1], f"TP1_{tp1_final[0]}"),
        ("TP", tp2_final[1], f"TP2_{tp2_final[0]}")
    )

