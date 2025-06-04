import pandas as pd
import numpy as np
import talib.abstract as ta
import indicators as qtpylib

def find_pivots(df2, pivot_range, min_percentage_change):
    pd.set_option('future.no_silent_downcasting', True)

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

    df2.loc[local_high_price, 'pivot'] = 1
    df2.loc[local_low_price, 'pivot'] = 2
    df2.loc[HH_condition, 'pivot'] = 3
    df2.loc[LL_condition, 'pivot'] = 4
    df2.loc[LH_condition, 'pivot'] = 5
    df2.loc[HL_condition, 'pivot'] = 6

    df2['idxx'] = df2.index

    df2.loc[df2['pivot'] == 3, f'HH_{pivot_range}_idx'] = df2['idxx']
    df2.loc[df2['pivot'] == 4, f'LL_{pivot_range}_idx'] = df2['idxx']
    df2.loc[df2['pivot'] == 5, f'LH_{pivot_range}_idx'] = df2['idxx']
    df2.loc[df2['pivot'] == 6, f'HL_{pivot_range}_idx'] = df2['idxx']

    df2[f'HH_{pivot_range}_idx'] = df2[f'HH_{pivot_range}_idx'].ffill()
    df2[f'LL_{pivot_range}_idx'] = df2[f'LL_{pivot_range}_idx'].ffill()
    df2[f'LH_{pivot_range}_idx'] = df2[f'LH_{pivot_range}_idx'].ffill()
    df2[f'HL_{pivot_range}_idx'] = df2[f'HL_{pivot_range}_idx'].ffill()

    df2[f'HH_{pivot_range}_idx'] = df2[f'HH_{pivot_range}_idx'].fillna(0)
    df2[f'LL_{pivot_range}_idx'] = df2[f'LL_{pivot_range}_idx'].fillna(0)
    df2[f'LH_{pivot_range}_idx'] = df2[f'LH_{pivot_range}_idx'].fillna(0)
    df2[f'HL_{pivot_range}_idx'] = df2[f'HL_{pivot_range}_idx'].fillna(0)

    ############################## MARK VALUES ##############################
    df2.loc[df2['pivot'] == 3, f'HH_{pivot_range}'] = df2['pivotprice']
    df2.loc[df2['pivot'] == 4, f'LL_{pivot_range}'] = df2['pivotprice']
    df2.loc[df2['pivot'] == 5, f'LH_{pivot_range}'] = df2['pivotprice']
    df2.loc[df2['pivot'] == 6, f'HL_{pivot_range}'] = df2['pivotprice']

    df2.loc[df2['pivot'] == 3, f'HH_{pivot_range}_rsi'] = df2['pivotprice_rsi']
    df2.loc[df2['pivot'] == 4, f'LL_{pivot_range}_rsi'] = df2['pivotprice_rsi']
    df2.loc[df2['pivot'] == 5, f'LH_{pivot_range}_rsi'] = df2['pivotprice_rsi']
    df2.loc[df2['pivot'] == 6, f'HL_{pivot_range}_rsi'] = df2['pivotprice_rsi']

    df2[f'HH_{pivot_range}'] = df2[f'HH_{pivot_range}'].ffill()
    df2[f'LL_{pivot_range}'] = df2[f'LL_{pivot_range}'].ffill()
    df2[f'LH_{pivot_range}'] = df2[f'LH_{pivot_range}'].ffill()
    df2[f'HL_{pivot_range}'] = df2[f'HL_{pivot_range}'].ffill()

    df2[f'HH_{pivot_range}_rsi'] = df2[f'HH_{pivot_range}_rsi'].ffill()
    df2[f'LL_{pivot_range}_rsi'] = df2[f'LL_{pivot_range}_rsi'].ffill()
    df2[f'LH_{pivot_range}_rsi'] = df2[f'LH_{pivot_range}_rsi'].ffill()
    df2[f'HL_{pivot_range}_rsi'] = df2[f'HL_{pivot_range}_rsi'].ffill()

    df2['HH_rsi_shift1'] = df2.loc[df2['pivot'] == 3, 'pivotprice_rsi'].shift(1)
    df2['LL_rsi_shift1'] = df2.loc[df2['pivot'] == 4, 'pivotprice_rsi'].shift(1)
    df2['LH_rsi_shift1'] = df2.loc[df2['pivot'] == 5, 'pivotprice_rsi'].shift(1)
    df2['HL_rsi_shift1'] = df2.loc[df2['pivot'] == 6, 'pivotprice_rsi'].shift(1)

    df2['HH_rsi_shift1'] = df2['HH_rsi_shift1'].ffill()
    df2['LL_rsi_shift1'] = df2['LL_rsi_shift1'].ffill()
    df2['LH_rsi_shift1'] = df2['LH_rsi_shift1'].ffill()
    df2['HL_rsi_shift1'] = df2['HL_rsi_shift1'].ffill()

    df2[f'HH_{pivot_range}_shift1'] = df2.loc[df2['pivot'] == 3, 'pivotprice'].shift(1)
    df2['LL_shift1'] = df2.loc[df2['pivot'] == 4, 'pivotprice'].shift(1)
    df2['LH_shift1'] = df2.loc[df2['pivot'] == 5, 'pivotprice'].shift(1)
    df2['HL_shift1'] = df2.loc[df2['pivot'] == 6, 'pivotprice'].shift(1)

    df2[f'HH_{pivot_range}_shift1'] = df2[f'HH_{pivot_range}_shift1'].ffill()
    df2['LL_shift1'] = df2['LL_shift1'].ffill()
    df2['LH_shift1'] = df2['LH_shift1'].ffill()
    df2['HL_shift1'] = df2['HL_shift1'].ffill()

    df2[f'HH_{pivot_range}_idx_shift1'] = df2.loc[df2['pivot'] == 3, 'idxx'].shift(1)
    df2['LL_idx_shift1'] = df2.loc[df2['pivot'] == 4, 'idxx'].shift(1)
    df2['LH_idx_shift1'] = df2.loc[df2['pivot'] == 5, 'idxx'].shift(1)
    df2['HL_idx_shift1'] = df2.loc[df2['pivot'] == 6, 'idxx'].shift(1)

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
    df2.loc[df2[[f'HH_{pivot_range}_idx', f'LH_{pivot_range}_idx']].max(axis=1) ==df2[f'HH_{pivot_range}_idx'], 'last_high_idx'] = df2[f'HH_{pivot_range}_idx']
    df2.loc[df2[[f'HH_{pivot_range}_idx', f'LH_{pivot_range}_idx']].max(axis=1) ==df2[f'LH_{pivot_range}_idx'], 'last_high_idx'] =df2[f'LH_{pivot_range}_idx']

    df2.loc[(df2['pivot']== 3) , 'last_LL_HH'] = df2[f'LL_{pivot_range}']
    df2['last_LL_HH'] = df2['last_LL_HH'].ffill()

    df2.loc[(df2['pivot']== 3) , 'last_HH_LL'] = df2[f'HH_{pivot_range}']
    df2['last_HH_LL'] = df2['last_HH_LL'].ffill()


############################################################ BULL DIV ################################################################


    rise = df2[f'last_high_{pivot_range}'] - df2[f'last_low_{pivot_range}']
    warun = df2[f'last_low_{pivot_range}_idx'] < df2['last_high_idx']

    df2.loc[warun, f'fibo_0618_{pivot_range}'] = df2[f'last_high_{pivot_range}'] - (rise * 0.618)
    df2.loc[warun, f'fibo_0660_{pivot_range}'] = df2[f'last_high_{pivot_range}'] - (rise * 0.66)
    df2.loc[warun, f'fibo_1272_{pivot_range}'] = df2[f'last_high_{pivot_range}'] - (rise * 1.25)
    df2.loc[warun, f'fibo_1618_{pivot_range}'] = df2[f'last_high_{pivot_range}'] - (rise *1.618)



    df2[f'fibo_0618_{pivot_range}'] = df2[f'fibo_0618_{pivot_range}'].ffill()
    df2[f'fibo_0660_{pivot_range}'] = df2[f'fibo_0660_{pivot_range}'].ffill()
    df2[f'fibo_1272_{pivot_range}'] = df2[f'fibo_1272_{pivot_range}'].ffill()
    df2[f'fibo_1618_{pivot_range}'] = df2[f'fibo_1618_{pivot_range}'].ffill()

    df2[f'fibo_0618_{pivot_range}'] = df2[f'fibo_0618_{pivot_range}'].fillna(0)
    df2[f'fibo_0660_{pivot_range}'] = df2[f'fibo_0660_{pivot_range}'].fillna(0)
    df2[f'fibo_1272_{pivot_range}'] = df2[f'fibo_1272_{pivot_range}'].fillna(0)
    df2[f'fibo_1618_{pivot_range}'] = df2[f'fibo_1618_{pivot_range}'].fillna(0)

    rise2 = df2[f'HH_{pivot_range}'] - df2['last_LL_HH']


    df2[f'fibo2_0618_{pivot_range}'] = df2[f'HH_{pivot_range}'] - (rise2 * 0.618)
    df2[f'fibo2_0660_{pivot_range}'] = df2[f'HH_{pivot_range}'] - (rise2 * 0.66)
    df2[f'fibo2_1272_{pivot_range}'] = df2[f'HH_{pivot_range}'] - (rise2 * 1.25)
    df2[f'fibo2_1618_{pivot_range}'] = df2[f'HH_{pivot_range}'] - (rise2 * 1.618)


############################################################ BEAR DIV ################################################################


    rise_bear = df2[f'last_high_{pivot_range}'] - df2[f'last_low_{pivot_range}']
    warun_bear = df2[f'last_low_{pivot_range}_idx'] > df2['last_high_idx']

    df2.loc[warun_bear, f'fibo_0618_{pivot_range}_bear'] = df2[f'last_low_{pivot_range}'] + (rise_bear * 0.618)
    df2.loc[warun_bear, f'fibo_0660_{pivot_range}_bear'] = df2[f'last_low_{pivot_range}'] + (rise_bear * 0.66)
    df2.loc[warun_bear, f'fibo_1272_{pivot_range}_bear'] = df2[f'last_low_{pivot_range}'] + (rise_bear * 1.25)
    df2.loc[warun_bear, f'fibo_1618_{pivot_range}_bear'] = df2[f'last_low_{pivot_range}'] + (rise_bear *1.618)



    df2[f'fibo_0618_{pivot_range}_bear'] = df2[f'fibo_0618_{pivot_range}_bear'].ffill()
    df2[f'fibo_0660_{pivot_range}_bear'] = df2[f'fibo_0660_{pivot_range}_bear'].ffill()
    df2[f'fibo_1272_{pivot_range}_bear'] = df2[f'fibo_1272_{pivot_range}_bear'].ffill()
    df2[f'fibo_1618_{pivot_range}_bear'] = df2[f'fibo_1618_{pivot_range}_bear'].ffill()

    df2[f'fibo_0618_{pivot_range}_bear'] = df2[f'fibo_0618_{pivot_range}_bear'].fillna(0)
    df2[f'fibo_0660_{pivot_range}_bear'] = df2[f'fibo_0660_{pivot_range}_bear'].fillna(0)
    df2[f'fibo_1272_{pivot_range}_bear'] = df2[f'fibo_1272_{pivot_range}_bear'].fillna(0)
    df2[f'fibo_1618_{pivot_range}_bear'] = df2[f'fibo_1618_{pivot_range}_bear'].fillna(0)

    rise2_bear = df2[f'HH_{pivot_range}'] - df2['last_HH_LL']


    df2[f'fibo2_0618_{pivot_range}_bear'] = df2[f'LL_{pivot_range}'] + (rise2_bear * 0.618)
    df2[f'fibo2_0660_{pivot_range}_bear'] = df2[f'LL_{pivot_range}'] + (rise2_bear * 0.66)
    df2[f'fibo2_1272_{pivot_range}_bear'] = df2[f'LL_{pivot_range}'] + (rise2_bear * 1.25)
    df2[f'fibo2_1618_{pivot_range}_bear'] = df2[f'LL_{pivot_range}'] + (rise2_bear * 1.618)

    rise_HH = df2[f'HH_{pivot_range}'] - df2[f'last_low_{pivot_range}']
    warun_HH_bear = df2[f'last_low_{pivot_range}_idx'] > df2[f'HH_{pivot_range}_idx']

    rise_LH = df2[f'LH_{pivot_range}'] - df2[f'last_low_{pivot_range}']
    warun_LH_bear = df2[f'last_low_{pivot_range}_idx'] > df2[f'LH_{pivot_range}_idx']

    rise_LL = df2[f'last_high_{pivot_range}'] - df2[f'LL_{pivot_range}']
    warun_LL_bull = df2['last_high_idx'] > df2[f'LL_{pivot_range}_idx']

    rise_HL = df2[f'last_high_{pivot_range}'] - df2[f'HL_{pivot_range}']
    warun_HL_bull = df2['last_high_idx'] > df2[f'HL_{pivot_range}_idx']


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

    df2.loc[df2['pivot'] == 3, f'HH_{pivot_range}_div'] = df2[f'bull_div_rsi_{pivot_range}']
    df2.loc[df2['pivot'] == 4, f'LL_{pivot_range}_div'] = df2[f'bear_div_rsi_{pivot_range}']

    df2[f'HH_{pivot_range}_div'] = df2[f'HH_{pivot_range}_div'].ffill()
    df2[f'LL_{pivot_range}_div'] = df2[f'LL_{pivot_range}_div'].ffill()

    df2[f'HH_{pivot_range}_div'] = df2[f'HH_{pivot_range}_div'].fillna(0)
    df2[f'LL_{pivot_range}_div'] = df2[f'LL_{pivot_range}_div'].fillna(0)


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

    df2.loc[df2['pivot'] == 3, f'pivot_bear_div_{pivot_range}'] = ((HH_condition == True) &
                                                                   (
                                                                           (
                                                                                    (df2[f'HH_{pivot_range}'] > df2[f'HH_{pivot_range}_shift1']) &
                                                                                    (df2[f'HH_{pivot_range}_rsi'] < df2['HH_rsi_shift1'])) |
                                                                           (
                                                                                    (df2[f'HH_{pivot_range}'] > df2[f'LH_{pivot_range}']) &
                                                                                    (df2[f'LH_{pivot_range}_rsi'] > df2[f'HH_{pivot_range}_rsi']) &
                                                                                    (df2[f'LH_{pivot_range}_idx'] < df2[f'HH_{pivot_range}_idx']))))

    df2.loc[df2['pivot'] == 4, f'pivot_bull_div_{pivot_range}'] = (
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

    df2.loc[df2[[f'LL_{pivot_range}_idx',f'HL_{pivot_range}_idx',f'HH_{pivot_range}_idx',f'LH_{pivot_range}_idx']].max(axis=1) == df2[f'HH_{pivot_range}_idx'], f'price_action_bull_{pivot_range}'] = True

    df2.loc[df2[[f'LL_{pivot_range}_idx', f'HL_{pivot_range}_idx',f'HH_{pivot_range}_idx',f'LH_{pivot_range}_idx']].max(axis=1) == df2[f'LH_{pivot_range}_idx'], f'price_action_bull_{pivot_range}'] = (
        (df2[f'HL_{pivot_range}_idx'] > df2[[f'HH_{pivot_range}_idx', f'LL_{pivot_range}_idx']].min(axis=1)) &
        (df2[f'HH_{pivot_range}_idx'] > df2[f'LL_{pivot_range}_idx'])
        )
    df2.loc[df2[[f'LL_{pivot_range}_idx', f'HL_{pivot_range}_idx',f'HH_{pivot_range}_idx',f'LH_{pivot_range}_idx']].max(axis=1) == df2[f'LL_{pivot_range}_idx'], f'price_action_bull_{pivot_range}'] = (
        (df2['high'].rolling(pivot_range).max() >= df2[[f'HH_{pivot_range}', f'LH_{pivot_range}']].min(axis=1))
        )
    df2.loc[df2[[f'LL_{pivot_range}_idx', f'HL_{pivot_range}_idx',f'HH_{pivot_range}_idx',f'LH_{pivot_range}_idx']].max(axis=1) == df2[f'HL_{pivot_range}_idx'], f'price_action_bull_{pivot_range}'] = (
        (df2['high'].rolling(pivot_range).max() >= df2[[f'HH_{pivot_range}', f'LH_{pivot_range}']].min(axis=1)) |
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

    ob_bull_cond = (df2['pivot'] == 4) | (df2['pivot'] == 6)
    bullish_ob = df2.loc[ob_bull_cond, ['pivotprice', 'pivot_body', 'idxx','time']]

    ob_bear_cond = (df2['pivot'] == 3) | (df2['pivot'] == 1)
    bearish_ob = df2.loc[ob_bear_cond, ['pivotprice', 'pivot_body', 'idxx','time']]


    fibos = df2[[f'fibo2_0618_{pivot_range}',f'fibo2_0660_{pivot_range}',f'fibo_0618_{pivot_range}',f'fibo_0660_{pivot_range}',
                 f'fibo2_1272_{pivot_range}',f'fibo2_1618_{pivot_range}',f'fibo_1272_{pivot_range}',f'fibo_1618_{pivot_range}',

                 f'fibo2_0618_{pivot_range}_bear',f'fibo2_0660_{pivot_range}_bear',f'fibo_0618_{pivot_range}_bear',f'fibo_0660_{pivot_range}_bear',
                 f'fibo2_1272_{pivot_range}_bear',f'fibo2_1618_{pivot_range}_bear',f'fibo_1272_{pivot_range}_bear',f'fibo_1618_{pivot_range}_bear']]

    peaks = df2[[f'HH_{pivot_range}',f'HL_{pivot_range}',f'LL_{pivot_range}',f'LH_{pivot_range}',
                 f'price_action_bull_{pivot_range}',f'price_action_bear_{pivot_range}',
                 f'last_high_{pivot_range}', f'last_low_{pivot_range}']]

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


def calculate_vwma(prices: pd.DataFrame, window: int):
    weighted_prices = prices['close'] * prices['tick_volume']
    rolling_sum_volume = prices['tick_volume'].rolling(window=window).sum()
    rolling_sum_weighted_prices = weighted_prices.rolling(window=window).sum()
    vwma = rolling_sum_weighted_prices / rolling_sum_volume
    return vwma


def candlectick_confirmation(df2, direction):

    candle = df2['high'] - df2['low']
    body_size = abs(df2['close'] - df2['open'])
    body_min =  df2[['close', 'open']].min(axis=1)
    body_max = df2[['close', 'open']].max(axis=1)
    lower_wick = (body_min - df2['low'])
    upper_wick = (df2['high'] - body_max)


    hammer = (lower_wick >= candle * 0.5)

    bullish_engulfing = ((df2['close'].shift(1) < df2['open'].shift(1))
                        & (df2['close'] > df2['open'].shift(1)))

    big_green = ( (candle > df2['atr'] * 1.5)
                    & (df2['close'] > df2['open']))

    hanging_man = (upper_wick >= candle * 0.5)

    bearish_engulfing = ((df2['close'].shift(1) > df2['open'].shift(1))
                         & (df2['close'] < df2['open'].shift(1)))

    big_red = ((candle < df2['atr'] * 1.5)
                 & (df2['close'] < df2['open']))

    if direction == 'long':
        return hammer + bullish_engulfing + big_green
    elif direction == 'short':
        return hanging_man + bearish_engulfing + big_red
    return None




def invalidate_zones_by_candle_extremes(
    ohlcv_df: pd.DataFrame,
    bullish_zones_df: pd.DataFrame,
    bearish_zones_df: pd.DataFrame,
    idx_col: str = 'idxx'
) -> tuple[pd.DataFrame, pd.DataFrame]:

    bullish_zones_df = bullish_zones_df.copy()
    bearish_zones_df = bearish_zones_df.copy()

    # Upewnij się, że idx_col jest int
    ohlcv_df[idx_col] = ohlcv_df[idx_col].astype(int)
    bullish_zones_df[idx_col] = bullish_zones_df[idx_col].astype(int)
    bearish_zones_df[idx_col] = bearish_zones_df[idx_col].astype(int)



    # Inicjalizuj validate_till
    for df in (bullish_zones_df, bearish_zones_df):
        if 'validate_till' not in df.columns:
            df['validate_till'] = np.nan
            df['validate_till_time'] = pd.NaT



    ohlcv_df = ohlcv_df.sort_values(idx_col).reset_index(drop=True)
    bullish_zones_df = bullish_zones_df.sort_values(idx_col).reset_index(drop=True)
    bearish_zones_df = bearish_zones_df.sort_values(idx_col).reset_index(drop=True)

    # Przygotuj szybkie wyszukiwanie: indeks do low/high, idx
    ohlcv_idx = ohlcv_df[idx_col].values
    ohlcv_time = ohlcv_df['time']
    lows = ohlcv_df[['open', 'close']].min(axis=1)
    highs = ohlcv_df[['open', 'close']].max(axis=1)


    # Funkcja do znajdowania validate_till dla bullish
    def find_validate_till(zones_df, boundary_col, candle_vals, cmp_op):
        validate_till = np.full(len(zones_df), np.nan)
        validate_till_time = np.full(len(zones_df), np.nan, dtype=object)
        for i, (zone_idx, boundary) in enumerate(zip(zones_df[idx_col], zones_df[boundary_col])):
            # Znajdź pierwszą świecę po zone_idx, która przebija boundary
            mask = ohlcv_idx > zone_idx
            candle_candidates = candle_vals[mask]
            idx_candidates = ohlcv_idx[mask]
            time_candidates = ohlcv_time[mask]

            if cmp_op == 'lt':  # low < low_boundary (bullish)
                breaches = np.where(candle_candidates < boundary)[0]
            else:  # high > high_boundary (bearish)
                breaches = np.where(candle_candidates > boundary)[0]

            if len(breaches) > 0:
                validate_till[i] = idx_candidates[breaches[0]]
                validate_till_time[i] = time_candidates.iloc[breaches[0]]
        return validate_till, validate_till_time

    # Znajdź validate_till dla bullish i bearish
    bullish_validate, bullish_validate_time = find_validate_till(bullish_zones_df[bullish_zones_df['validate_till'].isna()],
        'low_boundary',
        lows,
        'lt'
    )

    bearish_validate, bearish_validate_time = find_validate_till(
        bearish_zones_df[bearish_zones_df['validate_till'].isna()],
        'high_boundary',
        highs,
        'gt'
    )

    bullish_zones_df.loc[bullish_zones_df['validate_till'].isna(), 'validate_till'] = bullish_validate
    bearish_zones_df.loc[bearish_zones_df['validate_till'].isna(), 'validate_till'] = bearish_validate

    bullish_zones_df.loc[bullish_zones_df['validate_till_time'].isna(), 'validate_till_time'] = bullish_validate_time
    bearish_zones_df.loc[bearish_zones_df['validate_till_time'].isna(), 'validate_till_time'] = bearish_validate_time

    last_idx = ohlcv_df[idx_col].iloc[-1]
    last_time = ohlcv_df['time'].iloc[-1]


    bullish_zones_df['validate_till'] = bullish_zones_df['validate_till'].fillna(ohlcv_idx.max())
    bearish_zones_df['validate_till'] = bearish_zones_df['validate_till'].fillna(ohlcv_idx.max())



    bullish_zones_df['validate_till_time'] = bullish_zones_df['validate_till_time'].fillna(last_time)
    bearish_zones_df['validate_till_time'] = bearish_zones_df['validate_till_time'].fillna(last_time)



    return bullish_zones_df, bearish_zones_df

def mark_zone_reactions(timeframe, df, zones_df, direction, time_df_col: str = None, time_zone_col: str = None ):
    """
    Oznacza reakcje na strefy (FVG lub OB) w DataFrame świec.

    Parametry:
        df               - DataFrame OHLCV z kolumną 'idxx'
        zones_df         - DataFrame z walidowanymi strefami (musi mieć kolumny: idxx, validate_till, low_boundary, high_boundary)
        reaction_col_name - Nazwa kolumny do zapisania reakcji (np. 'bullish_fvg_reaction')
        direction        - 'bullish' lub 'bearish'
    """



    if timeframe == "aditional":
        df.columns = [
            col if col == 'time_H1' else col.replace('_H1', '')
            for col in df.columns
        ]



    # Prelicz korpusy
    min_body_prev = df[['open', 'close']].min(axis=1).shift(1)
    max_body_prev = df[['open', 'close']].max(axis=1).shift(1)
    min_body_now = df[['open', 'close']].min(axis=1)
    max_body_now = df[['open', 'close']].max(axis=1)

    # Inicjujemy kolumnę reakcji wartościami False
    reaction_col = pd.Series(False, index=df.index)

    for _, row in zones_df.iterrows():
        if pd.isna(row['validate_till']):
            continue

        valid_range = (df[time_df_col] > row[time_zone_col]) & ((df[time_df_col] < row['validate_till_time']) | (row['validate_till_time'] == np.nan))

        if direction == 'bullish':
            reaction = ((min_body_prev < row['high_boundary']) &  (min_body_now > row['low_boundary']))
            # świeca wchodzi w strefę

        elif direction == 'bearish':
            reaction = ((max_body_prev > row['low_boundary']) & (max_body_now < row['high_boundary']) )
            # świeca wchodzi w strefę

        else:
            raise ValueError("direction must be 'bullish' or 'bearish'")

        # Aktualizujemy tylko tam, gdzie jest reakcja i w valid_range
        reaction_col = reaction_col | (valid_range & reaction)

    return reaction_col


def mark_fibo_reactions(df, zone,  direction):
    """
    Oznacza reakcje na strefy (FVG lub OB) w DataFrame świec.

    Parametry:
        df               - DataFrame OHLCV z kolumną 'idxx'
        zones_df         - DataFrame z walidowanymi strefami (musi mieć kolumny: idxx, validate_till, low_boundary, high_boundary)
        reaction_col_name - Nazwa kolumny do zapisania reakcji (np. 'bullish_fvg_reaction')
        direction        - 'bullish' lub 'bearish'
    """



    # Prelicz korpusy
    min_body_prev = df[['open', 'close']].min(axis=1).shift(1)
    max_body_prev = df[['open', 'close']].max(axis=1).shift(1)
    min_body_now = df[['open', 'close']].min(axis=1)
    max_body_now = df[['open', 'close']].max(axis=1)



    if direction == 'bullish':
        reaction = ((min_body_prev < zone['high_boundary']) &  (min_body_now > zone['low_boundary']))
        # świeca wchodzi w strefę

    elif direction == 'bearish':
        reaction = ((max_body_prev > zone['low_boundary']) & (max_body_now < zone['high_boundary']) )
        # świeca wchodzi w strefę

    else:
        raise ValueError("direction must be 'bullish' or 'bearish'")

    # Aktualizujemy tylko tam, gdzie jest reakcja i w valid_range


    return reaction