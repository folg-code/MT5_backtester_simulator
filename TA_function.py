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
            (df2["high"].rolling(window=pivot_range).max().shift(pivot_range+1) < df2["high"].shift(pivot_range)) &
            (df2["high"].rolling(window=pivot_range).max() <= df2["high"].shift(pivot_range)))
    local_low_price = (
            ((df2["low"].rolling(window=pivot_range).min()).shift(pivot_range+1) > df2["low"].shift(pivot_range)) &
            (df2["low"].rolling(window=pivot_range).min() > df2["low"].shift(pivot_range)))


    df2.loc[local_high_price, 'pivotprice'] = df2['high'].shift(pivot_range)
    df2.loc[local_low_price, 'pivotprice'] = df2['low'].shift(pivot_range)

    df2.loc[local_high_price, 'pivot_body'] = (df2[['open', 'close']].max(axis=1).rolling(int(pivot_range)).max()).shift(int(pivot_range/2))
    df2.loc[local_low_price, 'pivot_body'] = (df2[['open', 'close']].min(axis=1).rolling(int(pivot_range)).min()).shift(int(pivot_range/2))

    df2.loc[local_high_price, 'pivotprice_rsi'] = (df2['rsi'].rolling(int(pivot_range)).max()).shift(int(pivot_range/2))
    df2.loc[local_low_price, 'pivotprice_rsi'] = (df2['rsi'].rolling(int(pivot_range)).min()).shift(int(pivot_range/2))


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

    df2.loc[df2[f'pivot_{pivot_range}'] == 3, f'HH_{pivot_range}_rsi'] = df2['pivotprice_rsi']
    df2.loc[df2[f'pivot_{pivot_range}'] == 4, f'LL_{pivot_range}_rsi'] = df2['pivotprice_rsi']
    df2.loc[df2[f'pivot_{pivot_range}'] == 5, f'LH_{pivot_range}_rsi'] = df2['pivotprice_rsi']
    df2.loc[df2[f'pivot_{pivot_range}'] == 6, f'HL_{pivot_range}_rsi'] = df2['pivotprice_rsi']

    df2[f'HH_{pivot_range}'] = df2[f'HH_{pivot_range}'].ffill()
    df2[f'LL_{pivot_range}'] = df2[f'LL_{pivot_range}'].ffill()
    df2[f'LH_{pivot_range}'] = df2[f'LH_{pivot_range}'].ffill()
    df2[f'HL_{pivot_range}'] = df2[f'HL_{pivot_range}'].ffill()

    df2[f'HH_{pivot_range}_rsi'] = df2[f'HH_{pivot_range}_rsi'].ffill()
    df2[f'LL_{pivot_range}_rsi'] = df2[f'LL_{pivot_range}_rsi'].ffill()
    df2[f'LH_{pivot_range}_rsi'] = df2[f'LH_{pivot_range}_rsi'].ffill()
    df2[f'HL_{pivot_range}_rsi'] = df2[f'HL_{pivot_range}_rsi'].ffill()

    df2['HH_rsi_shift1'] = df2.loc[df2[f'pivot_{pivot_range}'] == 3, 'pivotprice_rsi'].shift(1)
    df2['LL_rsi_shift1'] = df2.loc[df2[f'pivot_{pivot_range}'] == 4, 'pivotprice_rsi'].shift(1)
    df2['LH_rsi_shift1'] = df2.loc[df2[f'pivot_{pivot_range}'] == 5, 'pivotprice_rsi'].shift(1)
    df2['HL_rsi_shift1'] = df2.loc[df2[f'pivot_{pivot_range}'] == 6, 'pivotprice_rsi'].shift(1)

    df2['HH_rsi_shift1'] = df2['HH_rsi_shift1'].ffill()
    df2['LL_rsi_shift1'] = df2['LL_rsi_shift1'].ffill()
    df2['LH_rsi_shift1'] = df2['LH_rsi_shift1'].ffill()
    df2['HL_rsi_shift1'] = df2['HL_rsi_shift1'].ffill()

    df2[f'HH_{pivot_range}_shift1'] = df2.loc[df2[f'pivot_{pivot_range}'] == 3, 'pivotprice'].shift(1)
    df2['LL_shift1'] = df2.loc[df2[f'pivot_{pivot_range}'] == 4, 'pivotprice'].shift(1)
    df2['LH_shift1'] = df2.loc[df2[f'pivot_{pivot_range}'] == 5, 'pivotprice'].shift(1)
    df2['HL_shift1'] = df2.loc[df2[f'pivot_{pivot_range}'] == 6, 'pivotprice'].shift(1)

    df2[f'HH_{pivot_range}_shift1'] = df2[f'HH_{pivot_range}_shift1'].ffill()
    df2['LL_shift1'] = df2['LL_shift1'].ffill()
    df2['LH_shift1'] = df2['LH_shift1'].ffill()
    df2['HL_shift1'] = df2['HL_shift1'].ffill()

    df2[f'HH_{pivot_range}_idx_shift1'] = df2.loc[df2[f'pivot_{pivot_range}'] == 3, 'idxx'].shift(1)
    df2['LL_idx_shift1'] = df2.loc[df2[f'pivot_{pivot_range}'] == 4, 'idxx'].shift(1)
    df2['LH_idx_shift1'] = df2.loc[df2[f'pivot_{pivot_range}'] == 5, 'idxx'].shift(1)
    df2['HL_idx_shift1'] = df2.loc[df2[f'pivot_{pivot_range}'] == 6, 'idxx'].shift(1)

    df2[f'HH_{pivot_range}_idx_shift1'] = df2[f'HH_{pivot_range}_idx_shift1'].ffill()
    df2['LL_idx_shift1'] = df2['LL_idx_shift1'].ffill()
    df2['LH_idx_shift1'] = df2['LH_idx_shift1'].ffill()
    df2['HL_idx_shift1'] = df2['HL_idx_shift1'].ffill()








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

    df2.loc[(df2[f'pivot_{pivot_range}']== 3) , 'last_LL_HH'] = df2[f'LL_{pivot_range}']
    df2['last_LL_HH'] = df2['last_LL_HH'].ffill()

    df2.loc[(df2[f'pivot_{pivot_range}']== 3) , 'last_HH_LL'] = df2[f'HH_{pivot_range}']
    df2['last_HH_LL'] = df2['last_HH_LL'].ffill()




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






    rise_HH = df2[f'HH_{pivot_range}'] - df2[f'last_low_{pivot_range}']
    warun_HH_bear = df2[f'last_low_{pivot_range}_idx'] > df2[f'HH_{pivot_range}_idx']

    rise_LH = df2[f'LH_{pivot_range}'] - df2[f'last_low_{pivot_range}']
    warun_LH_bear = df2[f'last_low_{pivot_range}_idx'] > df2[f'LH_{pivot_range}_idx']

    rise_LL = df2[f'last_high_{pivot_range}'] - df2[f'LL_{pivot_range}']
    warun_LL_bull = df2[f'last_high_{pivot_range}_idx'] > df2[f'LL_{pivot_range}_idx']

    rise_HL = df2[f'last_high_{pivot_range}'] - df2[f'HL_{pivot_range}']
    warun_HL_bull = df2[f'last_high_{pivot_range}_idx'] > df2[f'HL_{pivot_range}_idx']


    #####################################################################################################

    #####################################################################################################
    df2[f'bear_div_rsi_{pivot_range}'] = (
            ((df2['high'] > df2[f'HH_{pivot_range}']) &
             (df2['rsi'].rolling(2).max() < df2[f'HH_{pivot_range}_rsi']) &
             ((df2['close'] < (df2[f'last_low_{pivot_range}'] + (rise_HH * 1.618))) & warun_HH_bear)) |

            ((df2['high'] > df2[f'LH_{pivot_range}']) &
             (df2['rsi'].rolling(2).max() < df2[f'LH_{pivot_range}_rsi']) &
             (df2[f'LH_{pivot_range}_idx'] > df2[f'HH_{pivot_range}_idx']) &
             ((df2['close'] < (df2[f'last_low_{pivot_range}'] + (rise_LH * 1.618))) & warun_LH_bear))
    )

    df2[f'bull_div_rsi_{pivot_range}'] = (
            (
                    (df2['low'] < df2[f'LL_{pivot_range}']) &
                    (df2['rsi'].rolling(2).min() > df2[f'LL_{pivot_range}_rsi'])&
                    ((df2['close'] > (df2[f'last_high_{pivot_range}'] - (rise_LL * 1.618))) & warun_LL_bull)) |

            (
                    (df2['low'] < df2[f'HL_{pivot_range}']) &
                    (df2['rsi'].rolling(2).max() > df2[f'HL_{pivot_range}_rsi']) &
                    (df2[f'HL_{pivot_range}_idx'] > df2[f'LL_{pivot_range}_idx'])&
                    ((df2['close'] > (df2[f'last_high_{pivot_range}'] - (rise_LL * 1.618))) & warun_HL_bull))
    )

    df2.loc[df2[f'pivot_{pivot_range}'] == 3, f'HH_{pivot_range}_div'] = df2[f'bull_div_rsi_{pivot_range}']
    df2.loc[df2[f'pivot_{pivot_range}'] == 4, f'LL_{pivot_range}_div'] = df2[f'bear_div_rsi_{pivot_range}']

    df2[f'HH_{pivot_range}_div'] = df2[f'HH_{pivot_range}_div'].ffill()
    df2[f'LL_{pivot_range}_div'] = df2[f'LL_{pivot_range}_div'].ffill()

    df2[f'HH_{pivot_range}_div'] = df2[f'HH_{pivot_range}_div'].fillna(df2['close'])
    df2[f'LL_{pivot_range}_div'] = df2[f'LL_{pivot_range}_div'].fillna(df2['close'])


    ####################### HIDDEN ############################################################

    df2[f'hidden_bear_div_rsi_{pivot_range}'] = (
        ((df2['rsi'] > df2[f'HH_{pivot_range}_rsi']) &
         (df2['high'].rolling(2).max() < df2[f'HH_{pivot_range}']) &
         (df2[f'HH_{pivot_range}_rsi'] > 30) |

        ((df2['high'] > df2[f'LH_{pivot_range}']) &
         (df2['rsi'].rolling(2).max()  < df2[f'LH_{pivot_range}_rsi']) &
         (df2[f'LH_{pivot_range}_idx'] > df2[f'HH_{pivot_range}_idx']) &
         (df2[f'LH_{pivot_range}_rsi'] > 60) &
         ((df2['close'] < (df2[f'last_low_{pivot_range}'] + (rise_LH * 1.618))) & warun_LH_bear))
        ))

    ####################### DIVS ON PIVOTS ############################################################
    df2[f'pivot_bull_div_{pivot_range}'] = False
    df2[f'pivot_bear_div_{pivot_range}'] = False

    df2.loc[df2[f'pivot_{pivot_range}'] == 3, f'pivot_bear_div_{pivot_range}'] = ((HH_condition == True) &
                                                                   (
                                                                           (
                                                                                    (df2[f'HH_{pivot_range}'] > df2[f'HH_{pivot_range}_shift1']) &
                                                                                    (df2[f'HH_{pivot_range}_rsi'] < df2['HH_rsi_shift1'])) |
                                                                           (
                                                                                    (df2[f'HH_{pivot_range}'] > df2[f'LH_{pivot_range}']) &
                                                                                    (df2[f'LH_{pivot_range}_rsi'] > df2[f'HH_{pivot_range}_rsi']) &
                                                                                    (df2[f'LH_{pivot_range}_idx'] < df2[f'HH_{pivot_range}_idx']))))

    df2.loc[df2[f'pivot_{pivot_range}'] == 4, f'pivot_bull_div_{pivot_range}'] = (
        (((df2['LL_shift1'] > df2[f'LL_{pivot_range}']) &
          (df2['LL_rsi_shift1'] < df2[f'LL_{pivot_range}_rsi'])) |
         ((df2['HL_shift1'] > df2[f'LL_{pivot_range}']) &
          (df2['HL_rsi_shift1'] < df2[f'LL_{pivot_range}_rsi']) &
          (df2['HL_idx_shift1'] < df2[f'LL_{pivot_range}_idx'])) |
         ((df2[f'HL_{pivot_range}'] > df2[f'LL_{pivot_range}']) &
          (df2[f'HL_{pivot_range}_rsi'] < df2[f'LL_{pivot_range}_rsi']) &
          (df2[f'HL_{pivot_range}_idx'] < df2[f'LL_{pivot_range}_idx']))))

    df2.loc[df2[f'pivot_bear_div_{pivot_range}'] == True, f'pivot_bear_div_idx_{pivot_range}'] = df2['idxx']
    df2.loc[df2[f'pivot_bull_div_{pivot_range}'] == True, f'pivot_bull_div_idx_{pivot_range}'] = df2['idxx']

    df2[f'pivot_bear_div_{pivot_range}'] = df2[f'pivot_bear_div_{pivot_range}'].ffill()
    df2[f'pivot_bull_div_{pivot_range}'] = df2[f'pivot_bull_div_{pivot_range}'].ffill()

    df2[f'pivot_bear_div_idx_{pivot_range}'] = df2[f'pivot_bear_div_idx_{pivot_range}'].ffill()
    df2[f'pivot_bull_div_idx_{pivot_range}'] = df2[f'pivot_bull_div_idx_{pivot_range}'].ffill()



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

    ob_bull_cond = (df2[f'pivot_{pivot_range}'] == 4) | (df2[f'pivot_{pivot_range}'] == 6)
    bullish_ob = df2.loc[ob_bull_cond, ['pivotprice', 'pivot_body', 'idxx','time']]

    ob_bear_cond = (df2[f'pivot_{pivot_range}'] == 3) | (df2[f'pivot_{pivot_range}'] == 1)
    bearish_ob = df2.loc[ob_bear_cond, ['pivotprice', 'pivot_body', 'idxx','time']]


    fibos = df2[[f'fibo_global_0618_{pivot_range}',f'fibo_global_0660_{pivot_range}',f'fibo_local_0618_{pivot_range}',f'fibo_local_0660_{pivot_range}',
                 f'fibo_global_1272_{pivot_range}',f'fibo_global_1618_{pivot_range}',f'fibo_local_1272_{pivot_range}',f'fibo_local_1618_{pivot_range}',

                 f'fibo_global_0618_{pivot_range}_bear',f'fibo_global_0660_{pivot_range}_bear',f'fibo_local_0618_{pivot_range}_bear',f'fibo_local_0660_{pivot_range}_bear',
                 f'fibo_global_1272_{pivot_range}_bear',f'fibo_global_1618_{pivot_range}_bear',f'fibo_local_1272_{pivot_range}_bear',f'fibo_local_1618_{pivot_range}_bear']]

    peaks = df2[[f'HH_{pivot_range}',f'HL_{pivot_range}',f'LL_{pivot_range}',f'LH_{pivot_range}',
                 f'HH_{pivot_range}_idx',f'HL_{pivot_range}_idx',f'LL_{pivot_range}_idx',f'LH_{pivot_range}_idx',
                 f'price_action_bull_{pivot_range}',f'price_action_bear_{pivot_range}',
                 f'last_high_{pivot_range}', f'last_low_{pivot_range}', f'pivot_{pivot_range}']]

    divs = df2[[f'bear_div_rsi_{pivot_range}', f'bull_div_rsi_{pivot_range}',
                f'pivot_bear_div_{pivot_range}', f'pivot_bull_div_{pivot_range}',
                f'pivot_bear_div_idx_{pivot_range}', f'pivot_bull_div_idx_{pivot_range}']]



    bullish_ob_renamed = bullish_ob.rename(columns={'pivotprice': 'low_boundary','pivot_body': 'high_boundary'})


    bearish_ob_renamed = bearish_ob.rename(columns={'pivotprice': 'high_boundary', 'pivot_body': 'low_boundary'})


    return  fibos, divs,  peaks, bullish_ob_renamed, bearish_ob_renamed

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

def check_reaction(df, lvl):

    df = df.copy()

    open = df['open']
    close = df['close']
    high = df['high']
    low = df['low']
    body = abs(close - open)
    candle_range = high - low
    upper_shadow = high - df[['close','open']].max(axis=1)
    lower_shadow = df[['close','open']].min(axis=1) - low

    SFP_hammer = (
        (low < lvl) &
        (close > lvl) & 
        (body < candle_range * 0.3) & 
        (lvl <= df[['close','open']].max(axis=1).rolling(5).min().shift(1)) )

    SFP_green = (
        (low < lvl) & 
        (close > lvl) & 
        (close > open)& 
        (low <= close.rolling(10).min().shift(1)) &
        (lvl <= df[['close','open']].max(axis=1).rolling(5).min().shift(1)) )
    SFP_red = (
        (low.shift(1) < lvl) & 
        (close.shift(1) > lvl)  &  
        (close.shift(1) < open.shift(1)) & 
        (close > open) & 
        (close > lvl)& 
        (lvl <= df[['close','open']].max(axis=1).rolling(5).min().shift(2)) )
    SFP_fakeout = (
        (close.shift(1) < lvl) & 
        (close.shift(1) < open.shift(1)) & 
        (close > lvl) & 
        (lvl <= df[['close','open']].max(axis=1).rolling(5).min().shift(2)) &
        (close > open))
    

    

      
    return SFP_hammer + SFP_green + SFP_red + SFP_fakeout 

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
    ohlcv_df[idx_col] = ohlcv_df[idx_col].astype(int)
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
    ohlcv_df[idx_col] = ohlcv_df[idx_col].astype(int)
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
    timeframe ,
    df,
    zone_df,
    direction,
    time_df_col: str = None,
    time_zone_col: str = None,
    always_create_breaker: bool = True
):
    """
    Oznacza reakcje na strefy (FVG lub OB) w DataFrame świec.
    Może opcjonalnie tworzyć breaker block na końcu każdego OB.

    Parametry:
        is_OB - True jeśli analizujemy order blocki
        timeframe - 'aditional' jeśli kolumny mają sufiks _H1
        df - DataFrame OHLCV z kolumną 'idxx'
        zone_df - DataFrame ze strefami (idxx, validate_till, low_boundary, high_boundary, validate_till_time)
        direction - 'bullish' lub 'bearish'
        time_df_col - kolumna z czasem w df
        time_zone_col - kolumna z czasem w zone_df
        always_create_breaker - jeśli True, breaker block tworzony zawsze po OB
    """

    df = df.copy()


    if timeframe == "aditional":
        df.columns = [
            col if col == 'time_H1' else col.replace('_H1', '')
            for col in df.columns
        ]

    # Ciała świec
    min_body_prev = df[['open', 'close']].min(axis=1).shift(1)
    max_body_prev = df[['open', 'close']].max(axis=1).shift(1)
    min_body_now = df[['open', 'close']].min(axis=1)
    max_body_now = df[['open', 'close']].max(axis=1)

    # Inicjalizacja kolumn
    reaction_col = pd.Series(False, index=df.index)
    low_boundary_col = pd.Series(np.nan, index=df.index, dtype=float)
    high_boundary_col = pd.Series(np.nan, index=df.index, dtype=float)

    breaker_blocks = []

    for _, row in zone_df.iterrows():
        if pd.isna(row['validate_till']):
            continue

        # Zakres ważności strefy
        valid_range = (df[time_df_col] > row[time_zone_col]) & (
            (df[time_df_col] < row['validate_till_time']) | pd.isna(row['validate_till_time'])
        )

        # Reakcje wg kierunku
        if direction == 'bullish':
            reaction = ( #((min_body_prev < row['high_boundary']) &(min_body_now > row['low_boundary'])) |
                 check_reaction(df, row['low_boundary']) | check_reaction(df, row['high_boundary']))
        elif direction == 'bearish':
            reaction = ((max_body_prev > row['low_boundary']) &
                        (max_body_now < row['high_boundary']))
        else:
            raise ValueError("direction must be 'bullish' or 'bearish'")

        mask = valid_range & reaction

        if mask.any():
            reaction_col.loc[mask] = True
            low_boundary_col.loc[mask] = row['low_boundary']
            high_boundary_col.loc[mask] = row['high_boundary']

            low_boundary_col = low_boundary_col.ffill()
            high_boundary_col = high_boundary_col.ffill()

        # Zawsze twórz breaker block na końcu OB
        if always_create_breaker:
            breaker_blocks.append({
                'high_boundary': row['high_boundary'],
                'low_boundary': row['low_boundary'],
                'idxx': int(row['validate_till']),
                time_zone_col: row['validate_till_time'],
                'zone_type': 'breaker'
            })
    breaker_blocks_df = pd.DataFrame(breaker_blocks).sort_values(by='idxx')

    if (zone_df['zone_type'] == 'ob').any():
        return reaction_col, low_boundary_col, high_boundary_col, breaker_blocks_df
    else:
        return reaction_col, low_boundary_col, high_boundary_col
    




def mark_fibo_reactions(df, zone, direction):
    df = df.copy()

    min_body_prev = df[['open', 'close']].min(axis=1).shift(1)
    max_body_prev = df[['open', 'close']].max(axis=1).shift(1)
    min_body_now = df[['open', 'close']].min(axis=1)
    max_body_now = df[['open', 'close']].max(axis=1)

    reaction_col = pd.Series(False, index=df.index)
    low_boundary_col = pd.Series(np.nan, index=df.index, dtype=float)
    high_boundary_col = pd.Series(np.nan, index=df.index, dtype=float)

    closes = df['close']

    if direction == 'bullish':
        reaction = ((min_body_prev < zone['high_boundary']) & (min_body_now > zone['low_boundary']))
        
        # Tworzymy serię bool - czy close jest poniżej low_boundary
        below_low = (closes < zone['low_boundary']).astype(int)
        # sumujemy w oknie 15 świec, ale shiftujemy o 1 by nie liczyć bieżącej świecy
        count_below_15 = below_low.shift(1).rolling(window=15, min_periods=1).sum()
        # warunek: w oknie 15 ostatnich świec było mniej niż 2 zamknięć poniżej fibo
        condition_no_two_below = count_below_15 < 2

        reaction = reaction & condition_no_two_below

    elif direction == 'bearish':
        reaction = ((max_body_prev > zone['low_boundary']) & (max_body_now < zone['high_boundary']))

        above_high = (closes > zone['high_boundary']).astype(int)
        count_above_15 = above_high.shift(1).rolling(window=15, min_periods=1).sum()
        condition_no_two_above = count_above_15 < 2

        reaction = reaction & condition_no_two_above

    else:
        raise ValueError("direction must być 'bullish' lub 'bearish'")

    reaction_col.loc[reaction] = True
    low_boundary_col.loc[reaction] = zone['low_boundary']
    high_boundary_col.loc[reaction] = zone['high_boundary']

    low_boundary_col = low_boundary_col.ffill()
    high_boundary_col = high_boundary_col.ffill()

    return reaction, low_boundary_col, high_boundary_col

def diff_percentage(v2, v1) -> float:
        diff = (v2 - v1) 
        diff = np.round(diff, 4)

        return np.abs(diff)

def calculate_monday_high_low(df, timezone='UTC'):
    df = df.copy()
    df['time'] = pd.to_datetime(df['time'])
    df['time'] = df['time'].dt.tz_convert('UTC') if df['time'].dt.tz is not None else df['time'].dt.tz_localize('UTC')
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

