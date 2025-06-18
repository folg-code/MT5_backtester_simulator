import pandas as pd
import numpy as np
import talib.abstract as ta
import indicators as qtpylib

def find_pivots(df2, pivot_range, min_percentage_change):


    df2 = df2.copy()

    df2['rsi'] =  ta.RSI(df2, pivot_range)
    df2['atr'] = ta.ATR(df2, pivot_range)

    ############################## DETECT PIVOTS ##############################
    local_high_price = (
            (df2["high"].rolling(window=pivot_range).max().shift(pivot_range+1) <= df2["high"].shift(pivot_range)) &
            (df2["high"].rolling(window=pivot_range).max() <= df2["high"].shift(pivot_range)))
    local_low_price = (
            ((df2["low"].rolling(window=pivot_range).min()).shift(pivot_range+1) >= df2["low"].shift(pivot_range)) &
            (df2["low"].rolling(window=pivot_range).min() >= df2["low"].shift(pivot_range)))


    df2.loc[local_high_price, 'pivotprice'] = df2['high'].shift(pivot_range)
    df2.loc[local_low_price, 'pivotprice'] = df2['low'].shift(pivot_range)

    df2.loc[local_high_price, 'pivot_body'] = (df2[['open', 'close']].max(axis=1).rolling(int(pivot_range)).max()).shift(int(pivot_range/2))
    df2.loc[local_low_price, 'pivot_body'] = (df2[['open', 'close']].min(axis=1).rolling(int(pivot_range)).min()).shift(int(pivot_range/2))

    


    HH_condition = local_high_price & (df2.loc[local_high_price, 'pivotprice'] > df2.loc[local_high_price, 'pivotprice'].shift(1))
    LL_condition = local_low_price & (df2.loc[local_low_price, 'pivotprice'] < df2.loc[local_low_price, 'pivotprice'].shift(1))
    LH_condition =local_high_price & (df2.loc[local_high_price, 'pivotprice'] < df2.loc[local_high_price, 'pivotprice'].shift(1))
    HL_condition = local_low_price & (df2.loc[local_low_price, 'pivotprice'] > df2.loc[local_low_price, 'pivotprice'].shift(1))



    df2.loc[local_high_price, f'pivot_{pivot_range}'] = 1
    df2.loc[local_low_price, f'pivot_{pivot_range}'] = 2
    df2.loc[HH_condition, f'pivot_{pivot_range}'] = 3
    df2.loc[LL_condition, f'pivot_{pivot_range}'] = 4
    df2.loc[LH_condition, f'pivot_{pivot_range}'] = 5
    df2.loc[HL_condition, f'pivot_{pivot_range}'] = 6
    

    df2['idxx'] = df2.index

    df2.loc[df2[f'pivot_{pivot_range}'] == 3, f'HH_{pivot_range}_idx'] = df2['idxx']
    df2.loc[df2[f'pivot_{pivot_range}'] == 4, f'LL_{pivot_range}_idx'] = df2['idxx']
    df2.loc[df2[f'pivot_{pivot_range}'] == 5, f'LH_{pivot_range}_idx'] = df2['idxx']
    df2.loc[df2[f'pivot_{pivot_range}'] == 6, f'HL_{pivot_range}_idx'] = df2['idxx']
    

    df2[f'HH_{pivot_range}_idx'] = df2[f'HH_{pivot_range}_idx'].ffill()
    df2[f'LL_{pivot_range}_idx'] = df2[f'LL_{pivot_range}_idx'].ffill()
    df2[f'LH_{pivot_range}_idx'] = df2[f'LH_{pivot_range}_idx'].ffill()
    df2[f'HL_{pivot_range}_idx'] = df2[f'HL_{pivot_range}_idx'].ffill()
    
    df2[f'HH_{pivot_range}_idx'] = df2[f'HH_{pivot_range}_idx'].fillna(0)
    df2[f'LL_{pivot_range}_idx'] = df2[f'LL_{pivot_range}_idx'].fillna(0)
    df2[f'LH_{pivot_range}_idx'] = df2[f'LH_{pivot_range}_idx'].fillna(0)
    df2[f'HL_{pivot_range}_idx'] = df2[f'HL_{pivot_range}_idx'].fillna(0)
    

    ############################## MARK VALUES ##############################
    df2.loc[df2[f'pivot_{pivot_range}'] == 3, f'HH_{pivot_range}'] = df2['pivotprice']
    df2.loc[df2[f'pivot_{pivot_range}'] == 4, f'LL_{pivot_range}'] = df2['pivotprice']
    df2.loc[df2[f'pivot_{pivot_range}'] == 5, f'LH_{pivot_range}'] = df2['pivotprice']
    df2.loc[df2[f'pivot_{pivot_range}'] == 6, f'HL_{pivot_range}'] = df2['pivotprice']
    



    df2[f'HH_{pivot_range}'] = df2[f'HH_{pivot_range}'].ffill()
    df2[f'LL_{pivot_range}'] = df2[f'LL_{pivot_range}'].ffill()
    df2[f'LH_{pivot_range}'] = df2[f'LH_{pivot_range}'].ffill()
    df2[f'HL_{pivot_range}'] = df2[f'HL_{pivot_range}'].ffill()

    df2[f'HH_{pivot_range}_shift'] = df2.loc[df2[f'pivot_{pivot_range}'] ==3, 'pivotprice' ].shift(1)
    df2[f'LL_{pivot_range}_shift'] = df2.loc[df2[f'pivot_{pivot_range}'] ==4, 'pivotprice' ].shift(1)
    df2[f'LH_{pivot_range}_shift'] = df2.loc[df2[f'pivot_{pivot_range}'] ==5, 'pivotprice' ].shift(1)
    df2[f'HL_{pivot_range}_shift'] = df2.loc[df2[f'pivot_{pivot_range}'] ==6, 'pivotprice' ].shift(1)

    df2[f'HH_{pivot_range}_shift'] = df2[f'HH_{pivot_range}_shift'].ffill()
    df2[f'LL_{pivot_range}_shift'] = df2[f'LL_{pivot_range}_shift'].ffill()
    df2[f'LH_{pivot_range}_shift'] = df2[f'LH_{pivot_range}_shift'].ffill()
    df2[f'HL_{pivot_range}_shift'] = df2[f'HL_{pivot_range}_shift'].ffill()

    df2[f'HH_{pivot_range}_idx_shift'] = df2.loc[df2[f'pivot_{pivot_range}'] ==3, 'idxx' ].shift(1)
    df2[f'LL_{pivot_range}_idx_shift'] = df2.loc[df2[f'pivot_{pivot_range}'] ==4, 'idxx' ].shift(1)
    df2[f'LH_{pivot_range}_idx_shift'] = df2.loc[df2[f'pivot_{pivot_range}'] ==5, 'idxx' ].shift(1)
    df2[f'HL_{pivot_range}_idx_shift'] = df2.loc[df2[f'pivot_{pivot_range}'] ==6, 'idxx' ].shift(1)

    df2[f'HH_{pivot_range}_idx_shift'] = df2[f'HH_{pivot_range}_idx_shift'].ffill()
    df2[f'LL_{pivot_range}_idx_shift'] = df2[f'LL_{pivot_range}_idx_shift'].ffill()
    df2[f'LH_{pivot_range}_idx_shift'] = df2[f'LH_{pivot_range}_idx_shift'].ffill()
    df2[f'HL_{pivot_range}_idx_shift'] = df2[f'HL_{pivot_range}_idx_shift'].ffill()
    


    ###################################################################################################

    df2['HH2'] = df2[f'HH_{pivot_range}']
    df2['LL2'] = df2[f'LL_{pivot_range}']

    df2['HH2_idx'] = df2[f'HH_{pivot_range}_idx']
    df2['LL2_idx'] = df2[f'LL_{pivot_range}_idx']

    df2.loc[df2[[f'LL_{pivot_range}_idx', f'HL_{pivot_range}_idx']].max(axis=1) ==df2[f'LL_{pivot_range}_idx'], f'last_low_2_{pivot_range}'] = df2[f'LL_{pivot_range}']
    df2.loc[df2[[f'LL_{pivot_range}_idx', f'HL_{pivot_range}_idx']].max(axis=1) ==df2[f'HL_{pivot_range}_idx'], f'last_low_2_{pivot_range}'] =df2[f'HL_{pivot_range}']
    df2.loc[df2[[f'HH_{pivot_range}_idx', f'LH_{pivot_range}_idx']].max(axis=1) ==df2[f'HH_{pivot_range}_idx'], f'last_high_2_{pivot_range}'] =df2[f'HH_{pivot_range}']
    df2.loc[df2[[f'HH_{pivot_range}_idx', f'LH_{pivot_range}_idx']].max(axis=1) ==df2[f'LH_{pivot_range}_idx'], f'last_high_2_{pivot_range}'] =df2[f'LH_{pivot_range}']


    df2.loc[(df2['high'].rolling(pivot_range).max() > df2[f'HH_{pivot_range}']), 'HH2'] = df2['high'].rolling(pivot_range).max()
    df2.loc[(df2['low'].rolling(pivot_range).min() < df2[f'LL_{pivot_range}']),  'LL2'] = df2['low'].rolling(pivot_range).min()



    ############################## CALCULATE FIBO ##############################
    df2.loc[df2[[f'LL_{pivot_range}_idx', f'HL_{pivot_range}_idx']].max(axis=1) ==df2[f'LL_{pivot_range}_idx'], f'last_low_{pivot_range}'] = df2[f'LL_{pivot_range}']
    df2.loc[df2[[f'LL_{pivot_range}_idx', f'HL_{pivot_range}_idx']].max(axis=1) ==df2[f'HL_{pivot_range}_idx'], f'last_low_{pivot_range}'] =df2[f'HL_{pivot_range}']
    df2.loc[df2[[f'HH_{pivot_range}_idx', f'LH_{pivot_range}_idx']].max(axis=1) ==df2[f'HH_{pivot_range}_idx'], f'last_high_{pivot_range}'] = df2[f'HH_{pivot_range}']
    df2.loc[df2[[f'HH_{pivot_range}_idx', f'LH_{pivot_range}_idx']].max(axis=1) ==df2[f'LH_{pivot_range}_idx'], f'last_high_{pivot_range}'] =df2[f'LH_{pivot_range}']

    df2.loc[df2[[f'LL_{pivot_range}_idx', f'HL_{pivot_range}_idx']].max(axis=1) ==df2[f'LL_{pivot_range}_idx'], f'last_low_{pivot_range}_idx'] = df2[f'LL_{pivot_range}_idx']
    df2.loc[df2[[f'LL_{pivot_range}_idx', f'HL_{pivot_range}_idx']].max(axis=1) ==df2[f'HL_{pivot_range}_idx'], f'last_low_{pivot_range}_idx'] =df2[f'HL_{pivot_range}_idx']
    df2.loc[df2[[f'HH_{pivot_range}_idx', f'LH_{pivot_range}_idx']].max(axis=1) ==df2[f'HH_{pivot_range}_idx'], f'last_high_{pivot_range}_idx'] = df2[f'HH_{pivot_range}_idx']
    df2.loc[df2[[f'HH_{pivot_range}_idx', f'LH_{pivot_range}_idx']].max(axis=1) ==df2[f'LH_{pivot_range}_idx'], f'last_high_{pivot_range}_idx'] =df2[f'LH_{pivot_range}_idx']

    df2.loc[(df2['high'].rolling(pivot_range).max() > df2[f'last_high_{pivot_range}']), f'last_high_{pivot_range}'] = df2['high'].rolling(pivot_range).max()
    df2.loc[(df2['low'].rolling(pivot_range).min() < df2[f'last_low_{pivot_range}']), f'last_low_{pivot_range}'] = df2['low'].rolling(pivot_range).min()

    df2.loc[(df2['high'].rolling(pivot_range).max() > df2[f'last_high_{pivot_range}']), f'last_high_{pivot_range}_idx'] = df2['high'].rolling(pivot_range).max()
    df2.loc[(df2['low'].rolling(pivot_range).min() < df2[f'last_low_{pivot_range}']), f'last_low_{pivot_range}_idx'] = df2['low'].rolling(pivot_range).min()



    rise = df2[f'last_high_{pivot_range}'] - df2[f'last_low_{pivot_range}']
    cond_up_local = df2[f'last_low_{pivot_range}_idx'] < df2[f'last_high_{pivot_range}_idx']
    cond_down_local = df2[f'last_low_{pivot_range}_idx'] > df2[f'last_high_{pivot_range}_idx']

    df2.loc[cond_up_local, f'fibo_local_0618_{pivot_range}'] = df2[f'last_high_{pivot_range}'] - (rise * 0.618)
    df2.loc[cond_up_local, f'fibo_local_0660_{pivot_range}'] = df2[f'last_high_{pivot_range}'] - (rise * 0.66)
    df2.loc[cond_up_local, f'fibo_local_1272_{pivot_range}'] = df2[f'last_high_{pivot_range}'] - (rise * 1.25)
    df2.loc[cond_up_local, f'fibo_local_1618_{pivot_range}'] = df2[f'last_high_{pivot_range}'] - (rise *1.618)

    df2.loc[cond_down_local, f'fibo_local_0618_{pivot_range}_bear'] = df2[f'last_low_{pivot_range}'] + (rise * 0.618)
    df2.loc[cond_down_local, f'fibo_local_0660_{pivot_range}_bear'] = df2[f'last_low_{pivot_range}'] + (rise * 0.66)
    df2.loc[cond_down_local, f'fibo_local_1272_{pivot_range}_bear'] = df2[f'last_low_{pivot_range}'] + (rise * 1.25)
    df2.loc[cond_down_local, f'fibo_local_1618_{pivot_range}_bear'] = df2[f'last_low_{pivot_range}'] + (rise * 1.618)

    for level in ['0618', '0660', '1272', '1618']:
        df2[f'fibo_local_{level}_{pivot_range}'] = df2[f'fibo_local_{level}_{pivot_range}'].ffill()
        df2[f'fibo_local_{level}_{pivot_range}'] = df2[f'fibo_local_{level}_{pivot_range}'].fillna(df2['close'])
        df2[f'fibo_local_{level}_{pivot_range}_bear'] = df2[f'fibo_local_{level}_{pivot_range}_bear'].ffill()
        df2[f'fibo_local_{level}_{pivot_range}_bear'] = df2[f'fibo_local_{level}_{pivot_range}_bear'].fillna(df2['close'])



    # ================================================
    # AKTUALIZACJA HH / LL z uwzględnieniem wybicia
    # ================================================

    df2['real_HH'] = df2[f'HH_{pivot_range}']
    df2['real_HH_idx'] = df2[f'HH_{pivot_range}_idx']
    new_hh = df2['high'] > df2['real_HH']
    df2.loc[new_hh, 'real_HH'] = df2['high']
    df2.loc[new_hh, 'real_HH_idx'] = df2.loc[new_hh].index

    df2['real_LL'] = df2[f'LL_{pivot_range}']
    df2['real_LL_idx'] = df2[f'LL_{pivot_range}_idx']
    new_ll = df2['low'] < df2['real_LL']
    df2.loc[new_ll, 'real_LL'] = df2['low']
    df2.loc[new_ll, 'real_LL_idx'] = df2.loc[new_ll].index

    # ===================================================
    # USTALENIE KIERUNKU (czy HH pojawił się po LL, itd.)
    # ===================================================

    cond_up = df2['real_LL_idx'] < df2['real_HH_idx']
    cond_down = df2['real_LL_idx'] > df2['real_HH_idx']

    # ================================================
    # GLOBALNE POZIOMY FIBO (na bazie real_LL i real_HH)
    # ================================================

    rise_global = df2['real_HH'] - df2['real_LL']

    # Bullish fibo poziomy (HH po LL)
    df2.loc[cond_up, f'fibo_global_0618_{pivot_range}'] = df2['real_HH'] - rise_global * 0.618
    df2.loc[cond_up, f'fibo_global_0660_{pivot_range}'] = df2['real_HH'] - rise_global * 0.66
    df2.loc[cond_up, f'fibo_global_1272_{pivot_range}'] = df2['real_HH'] - rise_global * 1.272
    df2.loc[cond_up, f'fibo_global_1618_{pivot_range}'] = df2['real_HH'] - rise_global * 1.618

    # Bearish fibo poziomy (LL po HH)
    df2.loc[cond_down, f'fibo_global_0618_{pivot_range}_bear'] = df2['real_LL'] + rise_global * 0.618
    df2.loc[cond_down, f'fibo_global_0660_{pivot_range}_bear'] = df2['real_LL'] + rise_global * 0.66
    df2.loc[cond_down, f'fibo_global_1272_{pivot_range}_bear'] = df2['real_LL'] + rise_global * 1.272
    df2.loc[cond_down, f'fibo_global_1618_{pivot_range}_bear'] = df2['real_LL'] + rise_global * 1.618

    # ================================================
    # UZUPEŁNIANIE BRAKÓW
    # ================================================

    for level in ['0618', '0660', '1272', '1618']:
        df2[f'fibo_global_{level}_{pivot_range}'] = df2[f'fibo_global_{level}_{pivot_range}'].ffill()
        df2[f'fibo_global_{level}_{pivot_range}'] = df2[f'fibo_global_{level}_{pivot_range}'].fillna(df2['close'])

        df2[f'fibo_global_{level}_{pivot_range}_bear'] = df2[f'fibo_global_{level}_{pivot_range}_bear'].ffill()
        df2[f'fibo_global_{level}_{pivot_range}_bear'] = df2[f'fibo_global_{level}_{pivot_range}_bear'].fillna(df2['close'])



    df2[f'price_action_bull_{pivot_range}'] = False

    df2.loc[df2[[f'LL_{pivot_range}_idx',f'HL_{pivot_range}_idx',f'HH_{pivot_range}_idx',f'LH_{pivot_range}_idx']].max(axis=1) == df2[f'HH_{pivot_range}_idx'], 
            f'price_action_bull_{pivot_range}'] = (df2[['open', 'close']].max(axis=1) > df2[f'LL_{pivot_range}'])

    """df2.loc[df2[[f'LL_{pivot_range}_idx', f'HL_{pivot_range}_idx',f'HH_{pivot_range}_idx',f'LH_{pivot_range}_idx']].max(axis=1) == df2[f'LH_{pivot_range}_idx'], f'price_action_bull_{pivot_range}'] = (
        (df2[f'HL_{pivot_range}_idx'] > df2[[f'HH_{pivot_range}_idx', f'LL_{pivot_range}_idx']].min(axis=1)) &
        (df2[f'HH_{pivot_range}_idx'] > df2[f'LL_{pivot_range}_idx'])
        )"""
    df2.loc[df2[[f'LL_{pivot_range}_idx', f'HL_{pivot_range}_idx',f'HH_{pivot_range}_idx',f'LH_{pivot_range}_idx']].max(axis=1) == df2[f'LL_{pivot_range}_idx'], 
            f'price_action_bull_{pivot_range}'] = (
        ((df2[['open', 'close']].min(axis=1)).rolling(pivot_range).max() >= df2[[f'HH_{pivot_range}', f'LH_{pivot_range}']].min(axis=1))
        )
    df2.loc[df2[[f'LL_{pivot_range}_idx', f'HL_{pivot_range}_idx',f'HH_{pivot_range}_idx',f'LH_{pivot_range}_idx']].max(axis=1) == df2[f'HL_{pivot_range}_idx'], f'price_action_bull_{pivot_range}'] = (
        ((df2[['open', 'close']].min(axis=1)).rolling(pivot_range).max() >= df2[[f'HH_{pivot_range}', f'LH_{pivot_range}']].min(axis=1)) |
        (df2[f'HH_{pivot_range}_idx'] > df2[[f'LL_{pivot_range}_idx', f'LH_{pivot_range}_idx']].min(axis=1))
        )

    df2[f'price_action_bear_{pivot_range}'] = False

    df2.loc[
        df2[[f'LL_{pivot_range}_idx', f'HL_{pivot_range}_idx', f'HH_{pivot_range}_idx', f'LH_{pivot_range}_idx']].max(
            axis=1) == df2[f'LL_{pivot_range}_idx'], f'price_action_bear_{pivot_range}'] = True

    df2.loc[
        df2[[f'LL_{pivot_range}_idx', f'HL_{pivot_range}_idx', f'HH_{pivot_range}_idx', f'LH_{pivot_range}_idx']].max(axis=1) == df2[f'HL_{pivot_range}_idx'],
        f'price_action_bear_{pivot_range}'] = (
            (df2[f'LH_{pivot_range}_idx'] > df2[[f'HH_{pivot_range}_idx', f'LL_{pivot_range}_idx']].min(axis=1)) &
            (df2[f'HH_{pivot_range}_idx'] < df2[f'LL_{pivot_range}_idx'])
    )
    df2.loc[
        df2[[f'LL_{pivot_range}_idx', f'HL_{pivot_range}_idx', f'HH_{pivot_range}_idx', f'LH_{pivot_range}_idx']].max(axis=1) == df2[f'HH_{pivot_range}_idx'],
        f'price_action_bear_{pivot_range}'] = (
        (df2['low'].rolling(pivot_range).min() <= df2[[f'LL_{pivot_range}', f'HL_{pivot_range}']].min(axis=1))
    )
    df2.loc[
        df2[[f'LL_{pivot_range}_idx', f'HL_{pivot_range}_idx', f'HH_{pivot_range}_idx', f'LH_{pivot_range}_idx']].max(axis=1) == df2[f'LH_{pivot_range}_idx'],
        f'price_action_bear_{pivot_range}'] = (
            (df2['low'].rolling(pivot_range).min() <= df2[[f'LL_{pivot_range}', f'HL_{pivot_range}']].min(axis=1)) |
            (df2[f'LL_{pivot_range}_idx'] > df2[[f'HL_{pivot_range}_idx', f'HL_{pivot_range}_idx']].min(axis=1))
    )


    fibos = df2[[f'fibo_global_0618_{pivot_range}',f'fibo_global_0660_{pivot_range}',f'fibo_local_0618_{pivot_range}',f'fibo_local_0660_{pivot_range}',
                 f'fibo_global_1272_{pivot_range}',f'fibo_global_1618_{pivot_range}',f'fibo_local_1272_{pivot_range}',f'fibo_local_1618_{pivot_range}',

                 f'fibo_global_0618_{pivot_range}_bear',f'fibo_global_0660_{pivot_range}_bear',f'fibo_local_0618_{pivot_range}_bear',f'fibo_local_0660_{pivot_range}_bear',
                 f'fibo_global_1272_{pivot_range}_bear',f'fibo_global_1618_{pivot_range}_bear',f'fibo_local_1272_{pivot_range}_bear',f'fibo_local_1618_{pivot_range}_bear']]

    peaks = df2[[f'HH_{pivot_range}',f'HL_{pivot_range}',f'LL_{pivot_range}',f'LH_{pivot_range}',
                 f'HH_{pivot_range}_shift',f'HL_{pivot_range}_shift',f'LL_{pivot_range}_shift',f'LH_{pivot_range}_shift',
                 f'HH_{pivot_range}_idx',f'HL_{pivot_range}_idx',f'LL_{pivot_range}_idx',f'LH_{pivot_range}_idx',
                 f'price_action_bull_{pivot_range}',f'price_action_bear_{pivot_range}',
                 f'last_high_{pivot_range}', f'last_low_{pivot_range}', f'pivot_{pivot_range}']]

    

    ob_bull_cond =  (df2[f'pivot_{pivot_range}'] == 6)
    bullish_ob = df2.loc[ob_bull_cond, ['pivotprice', 'pivot_body', 'idxx','time']]

    ob_bear_cond = (df2[f'pivot_{pivot_range}'] == 3) | (df2[f'pivot_{pivot_range}'] == 5) 
    bearish_ob = df2.loc[ob_bear_cond, ['pivotprice', 'pivot_body', 'idxx','time']]

    bullish_ob_renamed = bullish_ob.rename(columns={'pivotprice': 'low_boundary','pivot_body': 'high_boundary'})


    bearish_ob_renamed = bearish_ob.rename(columns={'pivotprice': 'high_boundary', 'pivot_body': 'low_boundary'})


    return  fibos,  peaks, bullish_ob_renamed, bearish_ob_renamed

def RMA(dataframe, source, period):
    df = dataframe.copy()
    sma3 = ta.SMA(source, int(period * 3))
    sma2 = ta.SMA(source, int(period * 2))
    sma1 = ta.SMA(source, int(period * 1))
    return sma3 -  sma2 + sma1

def market_cipher(self, df2) :
    # df2['volume_rolling'] = df2['volume'].shift(14).rolling(14).mean()
    #
    osLevel = -60
    obLevel = 50
    ap = (df2['high'] + df2['low'] + df2['close']) / 3
    esa = ta.EMA(ap, self.n1)
    d = ta.EMA((ap - esa).abs(), self.n1)
    ci = (ap - esa) / (0.015 * d)
    tci = ta.EMA(ci, self.n2)

    df2['wt1'] = tci
    df2['wt2'] = ta.SMA(df2['wt1'], 4)

    df2['wtOversold'] = df2['wt2'] <= osLevel
    df2['wtOverbought'] = df2['wt2'] >= obLevel

    df2['wtCrossUp'] = df2['wt2'] - df2['wt1'] <= 0
    df2['wtCrossDown'] = df2['wt2'] - df2['wt1'] >= 0
    df2['red_dot'] = qtpylib.crossed_above(df2['wt2'], df2['wt1'])
    df2['green_dot'] = qtpylib.crossed_below(df2['wt2'], df2['wt1'])

    period = 60
    multi = 150
    posy = 2.5

    df2['mfirsi'] = (ta.SMA(
        ((df2['close'] - df2['open']) / (df2['high'] - df2['low']) * multi), period)) - posy

    return df2


def detect_fvg(df, body_multiplier=1.5):
    df2 = df.copy()
    df2['first_high'] = df2['high'].shift(2)
    df2['first_low'] = df2['low'].shift(2)
    df2['middle_open'] = df2['open'].shift(1)
    df2['middle_close'] = df2['close'].shift(1)
    df2['middle_body'] = abs(df2['middle_close'] - df2['middle_open'])
    df2['avg_body_size'] = ta.ATR(df2, 14)
    df2['idxx'] = df2.index

    # Bullish condition
    fvg_bull_cond = (
        (df2['low'] > df2['first_high']) &
        (df2['middle_body'] > df2['avg_body_size'] * body_multiplier)
    )
    bullish_fvg = df2.loc[fvg_bull_cond, ['first_high', 'low', 'idxx','time']]

    # Bearish condition
    fvg_bear_cond = (
        (df2['high'] < df2['first_low']) &
        (df2['middle_body'] > df2['avg_body_size'] * body_multiplier)
    )
    bearish_fvg = df2.loc[fvg_bear_cond, ['first_low', 'high', 'idxx','time']]



    bullish_fvg_renamed = bullish_fvg.rename(columns={'first_high': 'low_boundary', 'low': 'high_boundary'})

    bearish_fvg_renamed = bearish_fvg.rename(columns={'first_low': 'high_boundary', 'high': 'low_boundary'})


    df2.drop(columns=[ 'middle_open', 'middle_close', 'middle_body', 'avg_body_size'],
            inplace=True)

    return bullish_fvg_renamed, bearish_fvg_renamed

def detect_gaps(df, gap_threshold=0.002):
    df2 = df.copy()
    df2['prev_close'] = df2['close'].shift(1)
    df2['curr_open'] = df2['open']
    df2['idxx'] = df2.index

    # Oblicz względną zmianę
    df2['gap_change'] = (df2['curr_open'] - df2['prev_close']) / df2['prev_close']

    # Gap up
    gap_up_cond = df2['gap_change'] > gap_threshold
    gap_up = df2.loc[gap_up_cond, ['prev_close', 'curr_open', 'idxx', 'time']]
    gap_up_renamed = gap_up.rename(columns={
        'prev_close': 'low_boundary',
        'curr_open': 'high_boundary'
    })

    # Gap down
    gap_down_cond = df2['gap_change'] < -gap_threshold
    gap_down = df2.loc[gap_down_cond, ['curr_open', 'prev_close', 'idxx', 'time']]
    gap_down_renamed = gap_down.rename(columns={
        'curr_open': 'low_boundary',
        'prev_close': 'high_boundary'
    })

    df2.drop(columns=['prev_close', 'curr_open', 'gap_change'], inplace=True)

    return gap_up_renamed, gap_down_renamed


def calculate_vwma(prices: pd.DataFrame, window: int):
    weighted_prices = prices['close'] * prices['tick_volume']
    rolling_sum_volume = prices['tick_volume'].rolling(window=window).sum()
    rolling_sum_weighted_prices = weighted_prices.rolling(window=window).sum()
    vwma = rolling_sum_weighted_prices / rolling_sum_volume
    return vwma

def check_reaction(df, lvl, direction='bullish'):
    

    open_ = df['open']
    close = df['close']
    high = df['high']
    low = df['low']
    body = abs(close - open_)
    candle_range = high - low
    upper_shadow = high - df[['close', 'open']].max(axis=1)
    lower_shadow = df[['close', 'open']].min(axis=1) - low

    max_body_rolling_5_min = df[['close', 'open']].max(axis=1).rolling(5).min().shift(1)
    max_body_rolling_5_min_shift2 = df[['close', 'open']].max(axis=1).rolling(5).min().shift(2)
    min_body_rolling_5_max = df[['close', 'open']].min(axis=1).rolling(5).max().shift(1)
    min_body_rolling_5_max_shift2 = df[['close', 'open']].min(axis=1).rolling(5).max().shift(2)

    if direction == 'bullish':
        SFP_hammer = (
            (low < lvl) &
            (close > lvl) & 
            (body < candle_range * 0.3) & 
            (lvl <= max_body_rolling_5_min)
        )

        SFP_green = (
            (low < lvl) &
            (close > lvl) &
            (close > open_) &
            (low <= close.rolling(10).min().shift(1)) &
            (lvl <= min_body_rolling_5_max)
        )

        SFP_red = (
            (low.shift(1) < lvl) &
            (close.shift(1) > lvl) &
            (close.shift(1) < open_.shift(1)) &
            (close > open_) &
            (close > lvl) &
            (lvl <= min_body_rolling_5_max_shift2)
        )

        SFP_fakeout = (
            (close.shift(1) < lvl) &
            (close.shift(1) < open_.shift(1)) &
            (close > lvl) &
            (lvl <= max_body_rolling_5_min_shift2) &
            (close > open_)
        )

    elif direction == 'bearish':
        SFP_hammer = (
            (high > lvl) &
            (close < lvl) &
            (body < candle_range * 0.3) &
            (lvl >= min_body_rolling_5_max)
        )

        SFP_green = (
            (high > lvl) &
            (close < lvl) &
            (close < open_) &
            (high >= close.rolling(10).max().shift(1)) &
            (lvl >= min_body_rolling_5_max)
        )

        SFP_red = (
            (high.shift(1) > lvl) &
            (close.shift(1) < lvl) &
            (close.shift(1) > open_.shift(1)) &
            (close < open_) &
            (close < lvl) &
            (lvl >= min_body_rolling_5_max_shift2)
        )

        SFP_fakeout = (
            (close.shift(1) > lvl) &
            (close.shift(1) > open_.shift(1)) &
            (close < lvl) &
            (lvl >= min_body_rolling_5_max_shift2) &
            (close < open_)
        )

    else:
        raise ValueError("direction must be 'bullish' or 'bearish'")

    return SFP_hammer | SFP_green | SFP_red | SFP_fakeout

def candlectick_confirmation(df2, direction):

    df2 = df2.copy()

    candle = df2['high'] - df2['low']
    body_size = abs(df2['close'] - df2['open'])
    body_min = df2[['close', 'open']].min(axis=1)
    body_max = df2[['close', 'open']].max(axis=1)
    lower_wick = body_min - df2['low']
    upper_wick = df2['high'] - body_max

    body_ratio = body_size / candle
    min_body_ratio = 0.3  # minimalny rozmiar korpusu względem świecy

    hammer = (
        (lower_wick >= 2 * body_size) &
        (body_size <= candle * 0.3) &
        (df2['close'].shift(1) < df2['open'].shift(1))
    )

    bullish_engulfing = (
        (df2['close'].shift(1) < df2['open'].shift(1)) &
        (df2['close'] > df2['open'].shift(1)) &
        (df2['open'] < df2['close'].shift(1))
    )
    
    ha = ((df2['ha_close'].shift(2) < df2['ha_open'].shift(2)) & 
        (df2['ha_close'].shift(1) < df2['ha_open'].shift(1))
         &(df2['ha_close'] > df2['ha_open']))
    
    # Nowe: Doji - bardzo mały korpus (<10% długości świecy)
    doji = ((df2['close'].shift(2) < df2['open'].shift(2))
         &(((abs(df2['close'].shift(1) - df2['open'].shift(1)))/( df2['high'].shift(1) - df2['low'].shift(1)))<= 0.1)
         & (df2['close'] > df2['open']))

    # Nowe: Spinning Top (szpulka) - korpus między 10% a 30% świecy z długimi knotami
    spinning_top = (
        (((abs(df2['close'].shift(1) - df2['open'].shift(1)))/( df2['high'].shift(1) - df2['low'].shift(1))) > 0.1) &
        (((abs(df2['close'].shift(1) - df2['open'].shift(1)))/( df2['high'].shift(1) - df2['low'].shift(1)))<= 0.3) &
        (df2[['close', 'open']].min(axis=1).shift(1) - df2['low'].shift(1) >= df2['high'].shift(1) - df2['low'].shift(1) * 0.2) &
        (df2['high'].shift(1) - df2[['close', 'open']].max(axis=1).shift(1) >= df2['high'].shift(1) - df2['low'].shift(1) * 0.2)&
        (df2['close'] > df2['open'])
    )

    hanging_man = (upper_wick >= candle * 0.5)

    bearish_engulfing = ((df2['close'].shift(1) > df2['open'].shift(1))
                         & (df2['close'] < df2['open'].shift(1)))

    big_red = ((candle < df2['atr'] * 1.5)
                 & (df2['close'] < df2['open']))

    if direction == 'long':
        return hammer + bullish_engulfing + ha + doji + spinning_top
    elif direction == 'short':
        return hanging_man + bearish_engulfing + big_red
    return None




def invalidate_zones_by_candle_extremes(
    timeframe: str,
    ohlcv_df: pd.DataFrame,
    bullish_zones_df: pd.DataFrame,
    bearish_zones_df: pd.DataFrame,
    idx_col: str 
) -> tuple[pd.DataFrame, pd.DataFrame]:

    import numpy as np
    import pandas as pd

    ohlcv_df = ohlcv_df.copy()
    bullish_zones_df = bullish_zones_df.copy()
    bearish_zones_df = bearish_zones_df.copy()

    # Zapewnij int w idx_col
    ohlcv_df[idx_col] = ohlcv_df[idx_col].ffill().astype(int)
    bullish_zones_df[idx_col] = bullish_zones_df[idx_col].astype(int)
    bearish_zones_df[idx_col] = bearish_zones_df[idx_col].astype(int)

    # Inicjalizuj kolumny jeśli ich nie ma
    for df in (bullish_zones_df, bearish_zones_df):
        if 'validate_till' not in df.columns:
            df['validate_till'] = np.nan
        if 'validate_till_time' not in df.columns:
            df['validate_till_time'] = pd.NaT

    # Sortowanie
    ohlcv_df = ohlcv_df.sort_values(idx_col).reset_index(drop=True)
    bullish_zones_df = bullish_zones_df.sort_values(idx_col).reset_index(drop=True)
    bearish_zones_df = bearish_zones_df.sort_values(idx_col).reset_index(drop=True)

    # Przygotuj dane do sprawdzania warunków
    ohlcv_idx = ohlcv_df[idx_col].values
    ohlcv_time = pd.to_datetime(ohlcv_df['time'], utc=True)
    lows = (ohlcv_df[['open', 'close']].min(axis=1))
    highs = (ohlcv_df[['open', 'close']].max(axis=1))

    # Funkcja pomocnicza
    def find_validate_till(zones_df, boundary_col, candle_vals, cmp_op):
        validate_till = np.full(len(zones_df), np.nan)
        validate_till_time = np.full(len(zones_df), pd.NaT, dtype='object')
        for i, (zone_idx, boundary) in enumerate(zip(zones_df[idx_col], zones_df[boundary_col])):
            mask = ohlcv_idx > zone_idx
            candle_candidates = candle_vals[mask]
            idx_candidates = ohlcv_idx[mask]
            time_candidates = ohlcv_time[mask]

            if cmp_op == 'lt':
                breaches = np.where(candle_candidates <= boundary)[0]
            else:
                breaches = np.where(candle_candidates >= boundary)[0]

            if len(breaches) > 0:
                validate_till[i] = idx_candidates[breaches[0]]
                validate_till_time[i] = time_candidates.iloc[breaches[0]]

        return validate_till, validate_till_time

    # Oblicz validate_till
    bullish_na = bullish_zones_df[bullish_zones_df['validate_till'].isna()]
    bearish_na = bearish_zones_df[bearish_zones_df['validate_till'].isna()]

    bullish_validate, bullish_validate_time = find_validate_till(
        bullish_na, 'low_boundary', lows, 'lt'
    )
    bearish_validate, bearish_validate_time = find_validate_till(
        bearish_na, 'high_boundary', highs, 'gt'
    )

    # Zastosuj wartości
    bullish_zones_df.loc[bullish_na.index, 'validate_till'] = bullish_validate
    bearish_zones_df.loc[bearish_na.index, 'validate_till'] = bearish_validate

    # Upewnij się, że validate_till_time ma typ datetime z UTC
    for df in (bullish_zones_df, bearish_zones_df):
        if not pd.api.types.is_datetime64tz_dtype(df['validate_till_time']):
            df['validate_till_time'] = pd.to_datetime(df['validate_till_time'], utc=True, errors='coerce')

    # Przypisz validate_till_time tylko do brakujących wartości
    bullish_zones_df.loc[bullish_na.index, 'validate_till_time'] = pd.to_datetime(bullish_validate_time, utc=True)
    bearish_zones_df.loc[bearish_na.index, 'validate_till_time'] = pd.to_datetime(bearish_validate_time, utc=True)

    # Uzupełnij pozostałe NaN-y
    last_idx = ohlcv_df[idx_col].iloc[-1]
    last_time = ohlcv_time.iloc[-1]  # Już jest z tz='UTC'

    bullish_zones_df['validate_till'] = bullish_zones_df['validate_till'].fillna(ohlcv_idx.max())
    bearish_zones_df['validate_till'] = bearish_zones_df['validate_till'].fillna(ohlcv_idx.max())

    bullish_zones_df['validate_till_time'] = bullish_zones_df['validate_till_time'].fillna(last_time)
    bearish_zones_df['validate_till_time'] = bearish_zones_df['validate_till_time'].fillna(last_time)

    return bullish_zones_df, bearish_zones_df


def invalidate_zones_by_candle_extremes_next(
    timeframe: str,
    ohlcv_df: pd.DataFrame,
    bullish_zones_df: pd.DataFrame,
    bearish_zones_df: pd.DataFrame,
    idx_col: str 
) -> tuple[pd.DataFrame, pd.DataFrame]:

    import numpy as np
    import pandas as pd

    ohlcv_df = ohlcv_df.copy()
    bullish_zones_df = bullish_zones_df.copy()
    bearish_zones_df = bearish_zones_df.copy()




    # Zapewnij int w idx_col
    ohlcv_df[idx_col] = ohlcv_df[idx_col].ffill().astype(int)
    bullish_zones_df[idx_col] = bullish_zones_df[idx_col].astype(int)
    bearish_zones_df[idx_col] = bearish_zones_df[idx_col].astype(int)

    # Inicjalizuj kolumny jeśli ich nie ma
    for df in (bullish_zones_df, bearish_zones_df):
        if 'validate_till' not in df.columns:
            df['validate_till'] = np.nan
        if 'validate_till_time' not in df.columns:
            df['validate_till_time'] = pd.NaT

    # Sortowanie
    ohlcv_df = ohlcv_df.sort_values(idx_col).reset_index(drop=True)
    bullish_zones_df = bullish_zones_df.sort_values(idx_col).reset_index(drop=True)
    bearish_zones_df = bearish_zones_df.sort_values(idx_col).reset_index(drop=True)

    # Przygotuj dane do sprawdzania warunków
    ohlcv_idx = ohlcv_df[idx_col].values
    ohlcv_time = pd.to_datetime(ohlcv_df['time_H1'], utc=True)
    lows = (ohlcv_df[['open_H1', 'close_H1']].min(axis=1))
    highs = (ohlcv_df[['open_H1', 'close_H1']].max(axis=1))

    # Funkcja pomocnicza
    def find_validate_till(zones_df, boundary_col, candle_vals, cmp_op):
        validate_till = np.full(len(zones_df), np.nan)
        validate_till_time = np.full(len(zones_df), pd.NaT, dtype='object')
        for i, (zone_idx, boundary) in enumerate(zip(zones_df[idx_col], zones_df[boundary_col])):
            mask = ohlcv_idx > zone_idx
            candle_candidates = candle_vals[mask]
            idx_candidates = ohlcv_idx[mask]
            time_candidates = ohlcv_time[mask]

            if cmp_op == 'lt':
                breaches = np.where(candle_candidates < boundary)[0]
            else:
                breaches = np.where(candle_candidates > boundary)[0]

            if len(breaches) > 0:
                validate_till[i] = idx_candidates[breaches[0]]
                validate_till_time[i] = time_candidates.iloc[breaches[0]]

        return validate_till, validate_till_time

    # Oblicz validate_till
    bullish_na = bullish_zones_df[bullish_zones_df['validate_till'].isna()]
    bearish_na = bearish_zones_df[bearish_zones_df['validate_till'].isna()]

    bullish_validate, bullish_validate_time = find_validate_till(
        bullish_na, 'low_boundary', lows, 'lt'
    )
    bearish_validate, bearish_validate_time = find_validate_till(
        bearish_na, 'high_boundary', highs, 'gt'
    )

    # Zastosuj wartości
    bullish_zones_df.loc[bullish_na.index, 'validate_till'] = bullish_validate
    bearish_zones_df.loc[bearish_na.index, 'validate_till'] = bearish_validate

    # Upewnij się, że validate_till_time ma typ datetime z UTC
    bullish_zones_df['validate_till_time'] = pd.to_datetime(bullish_zones_df['validate_till_time'], utc=True, errors='coerce')
    bearish_zones_df['validate_till_time'] = pd.to_datetime(bearish_zones_df['validate_till_time'], utc=True, errors='coerce')

    # Przypisz validate_till_time tylko do brakujących wartości
    bullish_zones_df.loc[bullish_na.index, 'validate_till_time'] = pd.to_datetime(bullish_validate_time, utc=True)
    bearish_zones_df.loc[bearish_na.index, 'validate_till_time'] = pd.to_datetime(bearish_validate_time, utc=True)

    # Uzupełnij pozostałe NaN-y
    last_idx = ohlcv_df[idx_col].iloc[-1]
    last_time = ohlcv_time.iloc[-1]  # Już jest z tz='UTC'

    bullish_zones_df['validate_till'] = bullish_zones_df['validate_till'].fillna(ohlcv_idx.max())
    bearish_zones_df['validate_till'] = bearish_zones_df['validate_till'].fillna(ohlcv_idx.max())

    bullish_zones_df['validate_till_time'] = bullish_zones_df['validate_till_time'].fillna(last_time)
    bearish_zones_df['validate_till_time'] = bearish_zones_df['validate_till_time'].fillna(last_time)

    return bullish_zones_df, bearish_zones_df

def mark_zone_reactions(
    timeframe,
    df,
    zone_df,
    direction,
    time_df_col: str = None,
    time_zone_col: str = None,
    always_create_breaker: bool = True
):
    

    if timeframe == "aditional":
        df = df.rename(columns={
            'open_H1': 'open',
            'high_H1': 'high',
            'low_H1': 'low',
            'close_H1': 'close',
            'time_H1': 'time',
            'idxx_H1': 'idxx'
        })
        time_df_col = 'time'  # <- Zmiana po renamowaniu

    df[time_df_col] = make_datetime_naive(df[time_df_col])
    zone_df[time_zone_col] = make_datetime_naive(zone_df[time_zone_col])
    zone_df['validate_till_time'] = make_datetime_naive(zone_df['validate_till_time'])
    

    if not isinstance(time_zone_col, str):
        raise TypeError(f"time_zone_col musi być str, a nie {type(time_zone_col)}")
    if not isinstance(time_df_col, str):
        raise TypeError(f"time_df_col musi być str, a nie {type(time_df_col)}")

    

    # Ciała świec (z przesunięciem dla poprzedniej świecy)
    min_body_prev = df[['open', 'close']].min(axis=1).shift(1).values  # numpy array
    max_body_prev = df[['open', 'close']].max(axis=1).shift(1).values
    min_body_now = df[['open', 'close']].min(axis=1).values

    df_times = df[time_df_col].values
    zone_start_times = zone_df[time_zone_col].values
    zone_end_times = zone_df['validate_till_time'].fillna(pd.Timestamp.max).values
    zone_low_bound = zone_df['low_boundary'].values
    zone_high_bound = zone_df['high_boundary'].values

    valid_till = zone_df['validate_till'].values

    # Filter out zones where validate_till is NaN (ignored)
    valid_zone_mask = ~pd.isna(valid_till)

    # Przygotuj outputy - wszystkie False/NaN na start
    reaction_col = np.zeros(len(df), dtype=bool)
    is_in_col = np.zeros(len(df), dtype=bool)
    low_boundary_col = np.full(len(df), np.nan)
    high_boundary_col = np.full(len(df), np.nan)

    # Zbierz breaker_blocks jako listę słowników (dopiero na końcu DataFrame)
    breaker_blocks = []

    # Wektorowo: Dla każdego df_time sprawdź dla każdej strefy czy czas jest w zakresie
    # Tworzymy macierz (len(df) x len(zone_df))

    time_cond = (df_times[:, None] > zone_start_times[None, :]) & (df_times[:, None] <= zone_end_times[None, :])

    # Odfiltruj strefy nieaktywne (np. validate_till NaN)
    time_cond[:, ~valid_zone_mask] = False

    # Jeśli żadna strefa, zwróć pustki
    if time_cond.sum() == 0:
        breaker_blocks_df = pd.DataFrame(columns=['high_boundary', 'low_boundary', 'idxx', time_zone_col, 'zone_type'])
        if (zone_df['zone_type'] == 'ob').any():
            return pd.Series(reaction_col, index=df.index), pd.Series(is_in_col, index=df.index), \
                   pd.Series(low_boundary_col, index=df.index), pd.Series(high_boundary_col, index=df.index), \
                   breaker_blocks_df
        else:
            return pd.Series(reaction_col, index=df.index), pd.Series(is_in_col, index=df.index), \
                   pd.Series(low_boundary_col, index=df.index), pd.Series(high_boundary_col, index=df.index)

    # Funkcja wektorowa do sprawdzenia reakcji (przykładowa, musisz dopasować do swojej implementacji)
    def vector_check_reaction(prices, boundary, direction):
        # np. zwraca bool array czy reakcja wystąpiła
        if direction == 'long':
            return prices >= boundary
        else:
            return prices <= boundary

    # Sprawdzamy reakcje dla low i high boundary we wszystkich strefach (macierze)
    # powiedzmy, że "prices" to df['high'] i df['low'] (można też dostosować do Twojej funkcji)
    # W oryginale było check_reaction(df_slice, boundary, direction)
    # tutaj zamienimy na proste porównania (dopasuj do swojego check_reaction!)

    # Pobierz ceny do wektorowego sprawdzenia (len(df) x 1)
    high_prices = df['high'].values[:, None]
    low_prices = df['low'].values[:, None]

    reaction_low = vector_check_reaction(low_prices, zone_low_bound[None, :], direction)
    reaction_high = vector_check_reaction(high_prices, zone_high_bound[None, :], direction)

    # Reakcja to OR z low i high reaction, ale tylko w czasie ważności strefy
    reaction_matrix = time_cond & (reaction_low | reaction_high)

    # Sprawdzenie, czy świeca jest wewnątrz strefy (porównanie ciał świec z granicami)
    is_in_matrix = time_cond & (
        (min_body_prev[:, None] <= zone_high_bound[None, :]) &
        (min_body_now[:, None] >= zone_low_bound[None, :])
    )

    # Teraz dla każdej świecy trzeba oznaczyć czy jest reakcja / jest w strefie, oraz przypisać granice

    # Zamień macierze na 1D tablice: jeśli w którejkolwiek strefie świeca ma reakcję lub jest w strefie
    reaction_any = reaction_matrix.any(axis=1)
    is_in_any = is_in_matrix.any(axis=1)

    # Dla przypisania low/high boundary - bierzemy *pierwszą* strefę, gdzie jest reakcja lub jest wewnątrz
    # (możesz zmodyfikować logikę według potrzeb)

    # Znajdź indeksy pierwszych stref spełniających warunek dla każdej świecy
    def first_true_idx(bool_2d_array):
        idx = np.argmax(bool_2d_array, axis=1)
        mask = bool_2d_array.any(axis=1)
        idx[~mask] = -1
        return idx

    idx_reaction = first_true_idx(reaction_matrix)
    idx_in_zone = first_true_idx(is_in_matrix)

    # Ustaw kolumny granic - tam gdzie reakcja lub jest w strefie
    valid_idx = (idx_reaction != -1)
    low_boundary_col[valid_idx] = zone_low_bound[idx_reaction[valid_idx]]
    high_boundary_col[valid_idx] = zone_high_bound[idx_reaction[valid_idx]]

    valid_idx_in = (idx_in_zone != -1)
    # Tu rozszerzamy granice jeśli są w strefie, ale nie były reakcją
    update_idx = valid_idx_in & (~valid_idx)
    low_boundary_col[update_idx] = zone_low_bound[idx_in_zone[update_idx]]
    high_boundary_col[update_idx] = zone_high_bound[idx_in_zone[update_idx]]

    # Ustaw kolumny reakcji i is_in
    reaction_col = reaction_any
    is_in_col = is_in_any

    # Tworzenie breaker_blocks jeśli always_create_breaker=True
    if always_create_breaker:
        for i, valid in enumerate(valid_zone_mask):
            if not valid:
                continue
            breaker_blocks.append({
                'high_boundary': zone_high_bound[i],
                'low_boundary': zone_low_bound[i],
                'idxx': int(valid_till[i]),
                time_zone_col: zone_end_times[i],
                'zone_type': 'breaker'
            })

    breaker_blocks_df = pd.DataFrame(breaker_blocks)
    if not breaker_blocks_df.empty and 'idxx' in breaker_blocks_df.columns:
        breaker_blocks_df = breaker_blocks_df.sort_values(by='idxx')
    else:
        breaker_blocks_df = pd.DataFrame(columns=['high_boundary', 'low_boundary', 'idxx', time_zone_col, 'zone_type'])

    # Zwracanie wyników
    if (zone_df['zone_type'] == 'ob').any():
        return pd.Series(reaction_col, index=df.index), pd.Series(is_in_col, index=df.index), \
               pd.Series(low_boundary_col, index=df.index), pd.Series(high_boundary_col, index=df.index), \
               breaker_blocks_df
    else:
        return pd.Series(reaction_col, index=df.index), pd.Series(is_in_col, index=df.index), \
               pd.Series(low_boundary_col, index=df.index), pd.Series(high_boundary_col, index=df.index)
    




def mark_fibo_reactions(df, zone, direction):
    df = df.copy()

    min_body_prev = df[['open', 'close']].min(axis=1).shift(1)
    max_body_prev = df[['open', 'close']].max(axis=1).shift(1)
    min_body_now = df[['open', 'close']].min(axis=1)
    max_body_now = df[['open', 'close']].max(axis=1)

    reaction_col = pd.Series(False, index=df.index)
    is_in_col = pd.Series(False, index=df.index)
    low_boundary_col = pd.Series(np.nan, index=df.index, dtype=float)
    high_boundary_col = pd.Series(np.nan, index=df.index, dtype=float)

    closes = df['close']

    if direction == 'bullish':
        is_in = ((min_body_prev < zone['high_boundary']) & (min_body_now > zone['low_boundary']))
        reaction = (check_reaction(df, zone['low_boundary'] ,direction )
                   |check_reaction(df, zone['high_boundary'] ,direction))
        
        # Tworzymy serię bool - czy close jest poniżej low_boundary
        below_low = (closes < zone['low_boundary']).astype(int)
        # sumujemy w oknie 15 świec, ale shiftujemy o 1 by nie liczyć bieżącej świecy
        count_below_15 = below_low.shift(1).rolling(window=15, min_periods=1).sum()
        # warunek: w oknie 15 ostatnich świec było mniej niż 2 zamknięć poniżej fibo
        condition_no_two_below = count_below_15 < 2

        is_in = is_in & condition_no_two_below
        reaction = reaction & condition_no_two_below

    elif direction == 'bearish':
        is_in = ((max_body_prev > zone['low_boundary']) & (max_body_now < zone['high_boundary']))
        reaction = (check_reaction(df, zone['low_boundary'] ,direction )
                   |check_reaction(df, zone['high_boundary'] ,direction))

        above_high = (closes > zone['high_boundary']).astype(int)
        count_above_15 = above_high.shift(1).rolling(window=15, min_periods=1).sum()
        condition_no_two_above = count_above_15 < 2

        is_in = is_in & condition_no_two_above
        reaction = reaction & condition_no_two_above

    else:
        raise ValueError("direction must być 'bullish' lub 'bearish'")
    reaction_col.loc[reaction] = True
    is_in_col.loc[is_in] = True
    low_boundary_col.loc[is_in] = zone['low_boundary']
    high_boundary_col.loc[is_in] = zone['high_boundary']

    low_boundary_col = low_boundary_col.ffill()
    high_boundary_col = high_boundary_col.ffill()

    return reaction_col, is_in, low_boundary_col, high_boundary_col

def diff_percentage(v2, v1) -> float:
        diff = (v2 - v1) 
        diff = np.round(diff, 4)

        return np.abs(diff)

def calculate_monday_high_low(df, timezone='UTC'):
    df = df.copy()
    df['time'] = pd.to_datetime(df['time'], utc=True)
    df['date'] = df['time'].dt.date
    df['weekday'] = df['time'].dt.weekday  # Monday = 0
    df['week'] = df['time'].dt.isocalendar().week
    df['year'] = df['time'].dt.isocalendar().year

    # Filtruj tylko poniedziałek
    monday_data = df[df['weekday'] == 0].copy()
    monday_data['monday'] = monday_data['date']

    # Upewnij się, że masz tylko dane z poniedziałku (00:00–23:59)
    monday_ranges = monday_data.groupby(['year', 'week']).agg({
        'high': 'max',
        'low': 'min',
        'monday': 'first'
    }).reset_index()

    # Mapuj te wartości do całego tygodnia
    df = df.merge(monday_ranges, on=['year', 'week'], how='left', suffixes=('', '_monday'))
    df.rename(columns={
        'high_monday': 'monday_high',
        'low_monday': 'monday_low'
    }, inplace=True)

    df.drop(columns=['date', 'monday'], inplace=True)

    return df

def detect_equal_extreme(df, pivot_col: str, atr_col: str, direction='bearish', window=10) -> pd.Series:
    """
    Wykrywa EQH lub EQL na podstawie reakcji i price rejection w kolejnych świecach.
    """
    mask = pd.Series(False, index=df.index)

    if direction == 'bearish':
        trigger_mask = (
            check_reaction(df, df[pivot_col], direction) |
            (df['close'] - df[pivot_col]).abs() < 0.2 * df[atr_col]
        )
    else:
        trigger_mask = (
            check_reaction(df, df[pivot_col], direction) |
            (df[pivot_col] - df['close']).abs() < 0.2 * df[atr_col]
        )

    trigger_indices = df[trigger_mask].index

    for idx in trigger_indices:
        atr_val = df.at[idx, atr_col]
        pivot_price = df.at[idx, pivot_col]
        future_window = df.loc[idx:].head(window)

        if direction == 'bearish':
            if (future_window['low'] < (pivot_price - 1.5 * atr_val)).any():
                mask.at[idx] = True
        else:
            if (future_window['high'] > (pivot_price + 1.5 * atr_val)).any():
                mask.at[idx] = True

    return mask

def make_datetime_naive(series_or_df_column):
    """Usuwa strefę czasową z daty, jeśli istnieje"""
    if hasattr(series_or_df_column.dt, 'tz') and series_or_df_column.dt.tz is not None:
        return series_or_df_column.dt.tz_localize(None)
    return series_or_df_column

