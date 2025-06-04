def validate_fvg(
        fvg_df: pd.DataFrame,
        idx_col: str,
        direction: str,
        opposite_zones_df: pd.DataFrame = None,
        same_side_zones_df: pd.DataFrame = None
) -> pd.DataFrame:
    """
    Walidacja FVG bez lookahead biasu. Unieważnia FVG w oparciu o:
    - nowe FVG tego samego kierunku
    - strefy przeciwnych kierunków (FVG lub OB)
    - strefy tego samego kierunku (OB)

    Parametry:
        fvg_df               - DataFrame z FVG.
        idx_col              - Nazwa kolumny indeksowej (np. 'idxx').
        direction            - 'bullish' lub 'bearish'.
        opposite_zones_df    - (opcjonalnie) przeciwny FVG lub OB.
        same_side_zones_df   - (opcjonalnie) OB w tym samym kierunku.
    """


    fvg_df = fvg_df.dropna(subset=[idx_col, 'low_boundary', 'high_boundary']).copy()

    fvg_df[idx_col] = fvg_df[idx_col].astype(int)
    fvg_df = fvg_df.sort_values(idx_col).reset_index(drop=True)
    fvg_df['validate_till'] = np.nan

    def preprocess_zones(df):
        if df is not None and not df.empty:

            df = df.dropna(subset=[idx_col, 'low_boundary', 'high_boundary']).copy()

            df[idx_col] = df[idx_col].astype(int)
            return df.sort_values(idx_col).reset_index(drop=True)
        return pd.DataFrame(columns=[idx_col, 'low_boundary', 'high_boundary'])

    opposite_zones_df = preprocess_zones(opposite_zones_df)
    same_side_zones_df = preprocess_zones(same_side_zones_df)



    active_fvg = []

    for current_idx, row in fvg_df.iterrows():
        fvg_idx = row[idx_col]
        fvg_low = row['low_boundary']
        fvg_high = row['high_boundary']
        active_fvg.append((current_idx, fvg_idx, fvg_low, fvg_high))

        to_remove = []

        # Unieważnianie przez inne FVG tego samego typu
        for i, (i_row, i_idx, i_low, i_high) in enumerate(active_fvg):

            if direction == 'bullish' and fvg_low < i_low:
                fvg_df.loc[i_row, 'validate_till'] = fvg_idx
                to_remove.append(i)
            elif direction == 'bearish' and fvg_high > i_high:
                fvg_df.loc[i_row, 'validate_till'] = fvg_idx
                to_remove.append(i)

        # Unieważnianie przez przeciwną strefę
        for _, opp in opposite_zones_df.iterrows():
            opp_idx = opp[idx_col]
            opp_low = opp['low_boundary']
            opp_high = opp['high_boundary']
            if opp_idx <= fvg_idx:
                continue

            for i, (i_row, i_idx, i_low, i_high) in enumerate(active_fvg):
                if pd.notna(fvg_df.loc[i_row, 'validate_till']):
                    continue
                if direction == 'bullish' and opp_high < i_high:
                    fvg_df.loc[i_row, 'validate_till'] = opp_idx
                    to_remove.append(i)
                elif direction == 'bearish' and opp_low > i_low:
                    fvg_df.loc[i_row, 'validate_till'] = opp_idx
                    to_remove.append(i)

        # 🔥 Nowość: Unieważnianie przez Order Blocki w tym samym kierunku
        for _, ob in same_side_zones_df.iterrows():
            ob_idx = ob[idx_col]
            ob_low = ob['low_boundary']
            ob_high = ob['high_boundary']
            if ob_idx - 21 <= fvg_idx:
                continue

            for i, (i_row, i_idx, i_low, i_high) in enumerate(active_fvg):
                if pd.notna(fvg_df.loc[i_row, 'validate_till']):
                    continue
                if direction == 'bullish' and ob_low < i_low:
                    fvg_df.loc[i_row, 'validate_till'] = ob_idx
                    to_remove.append(i)
                elif direction == 'bearish' and ob_high > i_high:
                    fvg_df.loc[i_row, 'validate_till'] = ob_idx
                    to_remove.append(i)

        # Usuwamy unieważnione FVG
        for i in sorted(set(to_remove), reverse=True):
            if i < len(active_fvg):
                del active_fvg[i]

    return fvg_df

def validate_orderblocks(
        ob_df: pd.DataFrame,
        idx_col: str,
        direction: str,
        opposite_zones_df: pd.DataFrame = None
) -> pd.DataFrame:
    """
    Waliduje order blocki bez lookahead biasu: aktualizuje `validate_till` tylko na podstawie
    danych dostępnych do danego momentu (symulacja forward/real-time).

    Parametry:
        ob_df               - DataFrame z order blockami.
        idx_col             - Nazwa kolumny z indeksem czasowym (np. 'idxx').
        direction           - 'bullish' lub 'bearish'.
        opposite_zones_df   - (opcjonalnie) DataFrame ze strefami przeciwnymi.

    Zwraca:
        ob_df z kolumną 'validate_till'
    """
    ob_df = ob_df.dropna(subset=[idx_col, 'low_boundary', 'high_boundary']).copy()
    ob_df[idx_col] = ob_df[idx_col].astype(int)
    ob_df = ob_df.sort_values(idx_col).reset_index(drop=True)
    ob_df['validate_till'] = np.nan

    if opposite_zones_df is not None and not opposite_zones_df.empty:
        opposite_zones_df = opposite_zones_df.dropna(subset=[idx_col, 'low_boundary', 'high_boundary']).copy()
        opposite_zones_df[idx_col] = opposite_zones_df[idx_col].astype(int)
        opposite_zones_df = opposite_zones_df.sort_values(idx_col).reset_index(drop=True)
    else:
        opposite_zones_df = pd.DataFrame(columns=[idx_col, 'low_boundary', 'high_boundary'])

    active_ob = []

    for current_idx, row in ob_df.iterrows():
        ob_idx = row[idx_col]
        ob_low = row['low_boundary']
        ob_high = row['high_boundary']
        active_ob.append((current_idx, ob_idx, ob_low, ob_high))

        to_remove = []

        # Walidacja w ramach własnych Order Blocków (trendowa)
        for i, (i_row, i_idx, i_low, i_high) in enumerate(active_ob):
            if i_row == current_idx:
                continue

            if direction == 'bullish':
                # jeśli kolejny OB ma niższą granicę low_boundary -> unieważniamy poprzedni
                if ob_low < i_low:
                    ob_df.loc[i_row, 'validate_till'] = ob_idx
                    to_remove.append(i)
            elif direction == 'bearish':
                # jeśli kolejny OB ma wyższą granicę high_boundary -> unieważniamy poprzedni
                if ob_high > i_high:
                    ob_df.loc[i_row, 'validate_till'] = ob_idx
                    to_remove.append(i)

        # Walidacja przez przeciwną strefę (np. FVG lub OB przeciwnego kierunku)
        for _, opp in opposite_zones_df.iterrows():
            opp_idx = opp[idx_col]
            opp_low = opp['low_boundary']
            opp_high = opp['high_boundary']

            # Sprawdzamy tylko strefy pojawiające się PO aktualnym OB (żeby uniknąć lookahead bias)
            if opp_idx <= ob_idx:
                continue

            for i, (i_row, i_idx, i_low, i_high) in enumerate(active_ob):
                if pd.notna(ob_df.loc[i_row, 'validate_till']):
                    continue  # już unieważnione

                if direction == 'bullish':
                    # przeciwny bearish OB/FVG unieważnia od dołu (gdy ich low_boundary jest poniżej high_boundary OB)
                    if opp_low < i_low:
                        ob_df.loc[i_row, 'validate_till'] = opp_idx
                        to_remove.append(i)
                elif direction == 'bearish':
                    # przeciwny bullish OB/FVG unieważnia od góry (gdy ich high_boundary jest powyżej low_boundary OB)
                    if opp_high > i_high:
                        ob_df.loc[i_row, 'validate_till'] = opp_idx
                        to_remove.append(i)

        # Usuwamy unieważnione OB z aktywnych
        for i in sorted(set(to_remove), reverse=True):
            if i < len(active_ob):
                del active_ob[i]

    return ob_df



if __name__ == "__main__":
    all_signals = []

    for symbol in config.SYMBOLS:
        df = get_data(symbol, config.TIMEFRAME_MAP[config.TIMEFRAME],
                      datetime.strptime(config.TIMERANGE['start'], "%Y-%m-%d"),
                      datetime.strptime(config.TIMERANGE['end'], "%Y-%m-%d"))

        strategy = load_strategy(config.strategy, df, symbol, 600)

        print(df)
        df_bt = strategy.run()
        df_bt["symbol"] = symbol
        all_signals.append(df_bt)
        print(df_bt)

    df_all_signals = pd.concat(all_signals).sort_values(by=["time", "symbol"])

    trades_all = backtest.vectorized_backtest(
        df_all_signals,
        None,  # symbol=None --> wielosymbolowy backtest
        config.SLIPPAGE,
        config.SL_PCT,
        config.TP_PCT,
        config.INITIAL_SIZE,
        config.MAX_SIZE
    )

    pd.set_option('display.max_rows', None)  # Pokaż wszystkie wiersze
    pd.set_option('display.max_columns', None)  # Pokaż wszystkie kolumny
    pd.set_option('display.width', None)  # Dopasuj szerokość do konsoli (bez łamania linii)

    print(trades_all)

    if not trades_all.empty:
        trades_all = raport.compute_equity(trades_all)
        plot.plot_equity(trades_all)
        raport.print_backtest_report(trades_all, df_all_signals)

        # Rysujemy wykresy po symbolach
        for symbol in config.SYMBOLS:
            trades_symbol = trades_all[trades_all['symbol'] == symbol]
            if not trades_symbol.empty:
                plot.plot_trades_with_indicators(
                    df=strategy.df_plot,
                    trades=trades_symbol,
                    bullish_zones=strategy.get_bullish_zones(),
                    bearish_zones=strategy.get_bearish_zones(),
                    extra_series=strategy.get_extra_values_to_plot()
                )

        if config.SAVE_TRADES_CSV:
            trades_all.to_csv("trades_ALL.csv", index=False)

    if __name__ == "__main__":

        df = get_data(config.SYMBOL, config.TIMEFRAME_MAP[config.TIMEFRAME],
                      datetime.strptime(config.TIMERANGE['start'], "%Y-%m-%d"),
                      datetime.strptime(config.TIMERANGE['end'], "%Y-%m-%d"))
        strategy = load_strategy(config.strategy, df)
        df_bt = strategy.run()
        trades = backtest.vectorized_backtest(df_bt, config.SYMBOL, config.SLIPPAGE, config.SL_PCT, config.TP_PCT,
                                              config.INITIAL_SIZE, config.MAX_SIZE)

        if not trades.empty:
            trades = raport.compute_equity(trades)
            plot.plot_equity(trades)
            raport.print_backtest_report(trades, df_bt)
            plot.plot_trades_with_indicators(
                df=strategy.df_plot,
                trades=trades,
                bullish_zones=strategy.get_bullish_zones(),
                bearish_zones=strategy.get_bearish_zones(),
                extra_series=strategy.get_extra_values_to_plot())

        if config.SAVE_TRADES_CSV:
            trades.to_csv(f"trades_{config.SYMBOL}_{config.TIMEFRAME}.csv", index=False)


def mark_zone_reactions(zone_type, timeframe, df, zones_df, direction, time_df_col: str = None, time_zone_col: str = None ):
    """
    Oznacza reakcje na strefy (FVG lub OB) w DataFrame świec.

    Parametry:
        df               - DataFrame OHLCV z kolumną 'idxx'
        zones_df         - DataFrame z walidowanymi strefami (musi mieć kolumny: idxx, validate_till, low_boundary, high_boundary)
        reaction_col_name - Nazwa kolumny do zapisania reakcji (np. 'bullish_fvg_reaction')
        direction        - 'bullish' lub 'bearish'
    """


    # Inicjujemy kolumnę reakcji wartościami False
    reaction_col = pd.Series(False, index=df.index)


    if zone_type == 'POI':
        if timeframe == "aditional":
            df.columns = [
                col if col == 'time_H1' else col.replace('_H1', '')
                for col in df.columns
            ]

        for _, row in zones_df.iterrows():
            if pd.isna(row['validate_till']):
                continue

            min_body_prev = df[['open', 'close']].min(axis=1).shift(1)
            max_body_prev = df[['open', 'close']].max(axis=1).shift(1)
            min_body_now = df[['open', 'close']].min(axis=1)
            max_body_now = df[['open', 'close']].max(axis=1)

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
            return reaction_col | (valid_range & reaction)
    else:

        min_body_prev = df[['open', 'close']].min(axis=1).shift(1)
        max_body_prev = df[['open', 'close']].max(axis=1).shift(1)
        min_body_now = df[['open', 'close']].min(axis=1)
        max_body_now = df[['open', 'close']].max(axis=1)

        if direction == 'bullish':
            reaction_fibo = ((min_body_prev < zones_df['high_boundary']) & (min_body_now > zones_df['low_boundary']))
            # świeca wchodzi w strefę

        elif direction == 'bearish':
            reaction_fibo = ((max_body_prev > zones_df['low_boundary']) & (max_body_now < zones_df['high_boundary']))
        else:
            raise ValueError("direction must be 'bullish' or 'bearish'")

        # Aktualizujemy tylko tam, gdzie jest reakcja i w valid_range
        return reaction_col | reaction_fibo