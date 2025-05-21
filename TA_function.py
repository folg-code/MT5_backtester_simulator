import pandas as pd
import numpy as np

def find_pivots(dataframe, pivot_range, min_percentage_change):

    df2 = dataframe.copy()

    df2['rsi'] =  ta.RSI(df2, pivot_range)

    ############################## DETECT PIVOTS ##############################
    local_high_price = (
            (df2["high"].rolling(window=pivot_range).max().shift(pivot_range+1) < df2["high"].shift(pivot_range)) &
            (df2["high"].rolling(window=pivot_range).max() <= df2["high"].shift(pivot_range)) &
            ((((df2["high"].shift(pivot_range) - dataframe['low'].rolling(window=pivot_range).min()  ) /dataframe['low'].rolling(window=pivot_range).min()) > min_percentage_change) |
             (df2["high"].shift(pivot_range) - dataframe['low'].rolling(window=pivot_range).min() >dataframe['atr'] * 3)))
    local_low_price = (
            ((df2["low"].rolling(window=pivot_range).min()).shift(pivot_range+1) > df2["low"].shift(pivot_range)) &
            (df2["low"].rolling(window=pivot_range).min() > df2["low"].shift(pivot_range))&
            ((((dataframe['high'].rolling(window=pivot_range).max() -df2["low"].shift(pivot_range)) /dataframe['low'].rolling(window=pivot_range).max()) > min_percentage_change) |
             (dataframe['high'].rolling(window=pivot_range).max() -df2["low"].shift(pivot_range)  > dataframe['atr'] * 3))
            )


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

    df2['idx'] = df2.index

    df2.loc[df2['pivot'] == 3, f'HH_{pivot_range}_idx'] = df2['idx']
    df2.loc[df2['pivot'] == 4, f'LL_{pivot_range}_idx'] = df2['idx']
    df2.loc[df2['pivot'] == 5, f'LH_{pivot_range}_idx'] = df2['idx']
    df2.loc[df2['pivot'] == 6, f'HL_{pivot_range}_idx'] = df2['idx']

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



    df2.loc[df2['pivot'] == 3, 'HH_body'] = df2['pivot_body']
    df2.loc[df2['pivot'] == 4, 'LL_body'] = df2['pivot_body']
    df2.loc[df2['pivot'] == 5, 'LH_body'] = df2['pivot_body']
    df2.loc[df2['pivot'] == 6, 'HL_body'] = df2['pivot_body']

    df2[f'HH_{pivot_range}'] = df2[f'HH_{pivot_range}'].ffill()
    df2[f'LL_{pivot_range}'] = df2[f'LL_{pivot_range}'].ffill()
    df2[f'LH_{pivot_range}'] = df2[f'LH_{pivot_range}'].ffill()
    df2[f'HL_{pivot_range}'] = df2[f'HL_{pivot_range}'].ffill()

    df2['HH_body'] = df2['HH_body'].ffill()
    df2['LL_body'] = df2['LL_body'].ffill()
    df2['LH_body'] = df2['LH_body'].ffill()
    df2['HL_body'] = df2['HL_body'].ffill()

    df2[f'HH_{pivot_range}_rsi'] = df2[f'HH_{pivot_range}_rsi'].ffill()
    df2[f'LL_{pivot_range}_rsi'] = df2[f'LL_{pivot_range}_rsi'].ffill()
    df2[f'LH_{pivot_range}_rsi'] = df2[f'LH_{pivot_range}_rsi'].ffill()
    df2[f'HL_{pivot_range}_rsi'] = df2[f'HL_{pivot_range}_rsi'].ffill()



    df2[f'HH_{pivot_range}_rsi_shift1'] = df2.loc[df2['pivot'] ==3, 'pivotprice_rsi' ].shift(1)
    df2[f'LL_{pivot_range}_rsi_shift1'] = df2.loc[df2['pivot'] ==4, 'pivotprice_rsi' ].shift(1)
    df2[f'LH_{pivot_range}_rsi_shift1'] = df2.loc[df2['pivot'] ==5, 'pivotprice_rsi' ].shift(1)
    df2[f'HL_{pivot_range}_rsi_shift1'] = df2.loc[df2['pivot'] ==6, 'pivotprice_rsi' ].shift(1)

    df2[f'HH_{pivot_range}_rsi_shift1'] = df2[f'HH_{pivot_range}_rsi_shift1'].ffill()
    df2[f'LL_{pivot_range}_rsi_shift1'] = df2[f'LL_{pivot_range}_rsi_shift1'].ffill()
    df2[f'LH_{pivot_range}_rsi_shift1'] = df2[f'LH_{pivot_range}_rsi_shift1'].ffill()
    df2[f'HL_{pivot_range}_rsi_shift1'] = df2[f'HL_{pivot_range}_rsi_shift1'].ffill()

    df2[f'HH_{pivot_range}_shift1'] = df2.loc[df2['pivot'] ==3, 'pivotprice' ].shift(1)
    df2[f'LL_{pivot_range}_shift1'] = df2.loc[df2['pivot'] ==4, 'pivotprice' ].shift(1)
    df2[f'LH_{pivot_range}_shift1'] = df2.loc[df2['pivot'] ==5, 'pivotprice' ].shift(1)
    df2[f'HL_{pivot_range}_shift1'] = df2.loc[df2['pivot'] ==6, 'pivotprice' ].shift(1)

    df2[f'HH_{pivot_range}_shift1'] = df2[f'HH_{pivot_range}_shift1'].ffill()
    df2[f'LL_{pivot_range}_shift1'] = df2[f'LL_{pivot_range}_shift1'].ffill()
    df2[f'LH_{pivot_range}_shift1'] = df2[f'LH_{pivot_range}_shift1'].ffill()
    df2[f'HL_{pivot_range}_shift1'] = df2[f'HL_{pivot_range}_shift1'].ffill()

    df2[f'HH_{pivot_range}_idx_shift1'] = df2.loc[df2['pivot'] ==3, 'idx' ].shift(1)
    df2[f'LL_{pivot_range}_idx_shift1'] = df2.loc[df2['pivot'] ==4, 'idx' ].shift(1)
    df2[f'LH_{pivot_range}_idx_shift1'] = df2.loc[df2['pivot'] ==5, 'idx' ].shift(1)
    df2[f'HL_{pivot_range}_idx_shift1'] = df2.loc[df2['pivot'] ==6, 'idx' ].shift(1)

    df2[f'HH_{pivot_range}_idx_shift1'] = df2[f'HH_{pivot_range}_idx_shift1'].ffill()
    df2[f'LL_{pivot_range}_idx_shift1'] = df2[f'LL_{pivot_range}_idx_shift1'].ffill()
    df2[f'LH_{pivot_range}_idx_shift1'] = df2[f'LH_{pivot_range}_idx_shift1'].ffill()
    df2[f'HL_{pivot_range}_idx_shift1'] = df2[f'HL_{pivot_range}_idx_shift1'].ffill()




    ############################## DETECT DIVS ##############################
    df2.loc[df2['pivot'] == 3, f'pivot_bear_div_{pivot_range}'] =  ((HH_condition ==True) &
        (((df2[f'HH_{pivot_range}'] > df2[f'HH_{pivot_range}_shift1']) &
          (df2[f'HH_{pivot_range}_rsi'] < df2[f'HH_{pivot_range}_rsi_shift1']) &
          (df2[f'HH_{pivot_range}_rsi_shift1'] > 65 ))|
        ((df2[f'HH_{pivot_range}'] > df2[f'LH_{pivot_range}']) &
         (df2[f'LH_{pivot_range}_rsi'] > df2[f'HH_{pivot_range}_rsi']) &
         (df2[f'LH_{pivot_range}_idx'] < df2[f'HH_{pivot_range}_idx']) &
         (df2[f'LH_{pivot_range}_rsi'] > 65))))

    df2.loc[df2['pivot'] == 4, f'pivot_bull_div_{pivot_range}'] =  (
        (((df2[f'LL_{pivot_range}_shift1'] > df2[f'LL_{pivot_range}']) &
          (df2[f'LL_{pivot_range}_rsi_shift1'] < df2[f'LL_{pivot_range}_rsi'])&
          (df2[f'LL_{pivot_range}_rsi_shift1'] < 35)) |
        ((df2[f'HL_{pivot_range}_shift1'] > df2[f'LL_{pivot_range}']) &
         (df2[f'HL_{pivot_range}_rsi_shift1'] < df2[f'LL_{pivot_range}_rsi']) &
         (df2[f'HL_{pivot_range}_idx_shift1'] <df2[f'LL_{pivot_range}_idx'])&
         (df2[f'HL_{pivot_range}_rsi_shift1'] < 35)) |
        ((df2[f'HL_{pivot_range}'] > df2[f'LL_{pivot_range}']) &
         (df2[f'HL_{pivot_range}_rsi'] < df2[f'LL_{pivot_range}_rsi']) &
         (df2[f'HL_{pivot_range}_idx'] < df2[f'LL_{pivot_range}_idx']))))

    df2.loc[df2[f'pivot_bear_div_{pivot_range}'] == True, f'pivot_bear_div_idx_{pivot_range}'] = df2['idx']
    df2.loc[df2[f'pivot_bull_div_{pivot_range}'] == True, f'pivot_bull_div_idx_{pivot_range}'] = df2['idx']

    df2[f'pivot_bear_div_{pivot_range}'] = df2[f'pivot_bear_div_{pivot_range}'].ffill()
    df2[f'pivot_bull_div_{pivot_range}'] = df2[f'pivot_bull_div_{pivot_range}'].ffill()

    df2[f'pivot_bear_div_idx_{pivot_range}'] = df2[f'pivot_bear_div_idx_{pivot_range}'].ffill()
    df2[f'pivot_bull_div_idx_{pivot_range}'] = df2[f'pivot_bull_div_idx_{pivot_range}'].ffill()

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
    df2[f'bear_div_rsi_{pivot_range}'] = (
        ((df2['high'] > df2[f'HH_{pivot_range}']) &
         (df2['rsi'].rolling(2).max() < df2[f'HH_{pivot_range}_rsi']) &
         ((dataframe['close'] < (df2[f'last_low_{pivot_range}'] + (rise_HH * 1.618))) & warun_HH_bear))|



        ((df2['high'] > df2[f'LH_{pivot_range}']) &
         (df2['rsi'].rolling(2).max()  < df2[f'LH_{pivot_range}_rsi']) &
         (df2[f'LH_{pivot_range}_idx'] > df2[f'HH_{pivot_range}_idx']) &
         ((dataframe['close'] < (df2[f'last_low_{pivot_range}'] + (rise_LH * 1.618))) & warun_LH_bear))
        )



    df2[f'bull_div_rsi_{pivot_range}'] = (
        (
            (df2['low'] < df2[f'LL_{pivot_range}']) &
            (df2['rsi'].rolling(2).min() > df2[f'LL_{pivot_range}_rsi']) )|

        (
            (df2['low'] < df2[f'HL_{pivot_range}']) &
            (df2['rsi'].rolling(2).max()  > df2[f'HL_{pivot_range}_rsi']) &
            (df2[f'HL_{pivot_range}_idx'] > df2[f'LL_{pivot_range}_idx']) )
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
         ((dataframe['close'] < (df2[f'last_low_{pivot_range}'] + (rise_LH * 1.618))) & warun_LH_bear))
        ))



    df2[f'price_action_{pivot_range}'] = False

    df2.loc[df2[[f'LL_{pivot_range}_idx',f'HL_{pivot_range}_idx',f'HH_{pivot_range}_idx',f'LH_{pivot_range}_idx']].max(axis=1) == df2[f'HH_{pivot_range}_idx'], f'price_action_{pivot_range}'] = True

    df2.loc[df2[[f'LL_{pivot_range}_idx', f'HL_{pivot_range}_idx',f'HH_{pivot_range}_idx',f'LH_{pivot_range}_idx']].max(axis=1) == df2[f'LH_{pivot_range}_idx'], f'price_action_{pivot_range}'] = (
        (df2[f'HL_{pivot_range}_idx'] > df2[[f'HH_{pivot_range}_idx', f'LL_{pivot_range}_idx']].min(axis=1)) &
        (df2[f'HH_{pivot_range}_idx'] > df2[f'LL_{pivot_range}_idx'])
        )
    df2.loc[df2[[f'LL_{pivot_range}_idx', f'HL_{pivot_range}_idx',f'HH_{pivot_range}_idx',f'LH_{pivot_range}_idx']].max(axis=1) == df2[f'LL_{pivot_range}_idx'], f'price_action_{pivot_range}'] = (
        (df2['high'].rolling(pivot_range).max() >= df2[[f'HH_{pivot_range}', f'LH_{pivot_range}']].min(axis=1))
        )
    df2.loc[df2[[f'LL_{pivot_range}_idx', f'HL_{pivot_range}_idx',f'HH_{pivot_range}_idx',f'LH_{pivot_range}_idx']].max(axis=1) == df2[f'HL_{pivot_range}_idx'], f'price_action_{pivot_range}'] = (
        (df2['high'].rolling(pivot_range).max() >= df2[[f'HH_{pivot_range}', f'LH_{pivot_range}']].min(axis=1)) |
        (df2[f'HH_{pivot_range}_idx'] > df2[[f'LL_{pivot_range}_idx', f'LH_{pivot_range}_idx']].min(axis=1))
        )



    cond_bull = (df2['pivot'] == 4) | (df2['pivot'] == 6)
    bull_ob = df2.loc[cond_bull, ['pivotprice', 'pivot_body','idx']]

    rows_to_remove2 = []
    previous_price1 = bull_ob.iloc[0, bull_ob.columns.get_loc('pivotprice')]
    bull_ob['validate_till'] = np.nan
    for idx, row in bull_ob.iterrows():
        if  row['pivotprice'] < previous_price1:
            rows_to_remove2.extend(bull_ob[(bull_ob['idx'] < row['idx']) &
                                           (bull_ob['pivotprice'] > row['pivotprice'])].idx )
            empty_validate_till1 = bull_ob['validate_till'].isnull()
            bull_ob.loc[(bull_ob['idx'] < row['idx']) & (bull_ob['pivotprice']> row['pivotprice']) &
                        empty_validate_till1, 'validate_till'] = row['idx']

        previous_price1 = row['pivotprice']
    OB_bull = bull_ob.drop(rows_to_remove2)


    bull_ob['validate_till'].fillna(np.inf)




    fibos = df2[[f'fibo2_0618_{pivot_range}',f'fibo2_0660_{pivot_range}',f'fibo_0618_{pivot_range}',f'fibo_0660_{pivot_range}',
                 f'fibo2_1272_{pivot_range}',f'fibo2_1618_{pivot_range}',f'fibo_1272_{pivot_range}',f'fibo_1618_{pivot_range}',

                 f'fibo2_0618_{pivot_range}_bear',f'fibo2_0660_{pivot_range}_bear',f'fibo_0618_{pivot_range}_bear',f'fibo_0660_{pivot_range}_bear',
                 f'fibo2_1272_{pivot_range}_bear',f'fibo2_1618_{pivot_range}_bear',f'fibo_1272_{pivot_range}_bear',f'fibo_1618_{pivot_range}_bear']]

    peaks = df2[[f'HH_{pivot_range}',f'HL_{pivot_range}',f'LL_{pivot_range}',f'LH_{pivot_range}',
                 f'HH_{pivot_range}_rsi',f'HL_{pivot_range}_rsi',f'LL_{pivot_range}_rsi',f'LH_{pivot_range}_rsi',
                 f'HH_{pivot_range}_idx',f'HL_{pivot_range}_idx',f'LL_{pivot_range}_idx',f'LH_{pivot_range}_idx',
                 f'HH_{pivot_range}_shift1',f'HL_{pivot_range}_shift1',f'LL_{pivot_range}_shift1',f'LH_{pivot_range}_shift1',
                 f'HH_{pivot_range}_idx_shift1',f'HL_{pivot_range}_idx_shift1',f'LL_{pivot_range}_idx_shift1',f'LH_{pivot_range}_idx_shift1',
                 f'last_high_{pivot_range}', f'last_low_{pivot_range}', f'last_low_2_{pivot_range}',f'last_high_2_{pivot_range}',
                 f'price_action_{pivot_range}',f'last_low_{pivot_range}_idx'
                 ]]

    divs = df2[[
                f'bear_div_rsi_{pivot_range}',f'bull_div_rsi_{pivot_range}',
                f'pivot_bear_div_{pivot_range}',f'pivot_bull_div_{pivot_range}',
                f'pivot_bear_div_idx_{pivot_range}',f'pivot_bull_div_idx_{pivot_range}',
                f'HH_{pivot_range}_div',f'LL_{pivot_range}_div']]




    return  fibos, divs,  bull_ob, OB_bull, peaks