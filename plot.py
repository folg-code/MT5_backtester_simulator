import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import matplotlib.pyplot as plt
import os

def plot_equity(trades_df):
    """
    Rysuje krzywą kapitału na podstawie kolumny 'capital' i 'timestamp'.
    """
    if 'capital' not in trades_df.columns:
        raise ValueError("Brakuje kolumny 'capital' – oblicz ją najpierw (np. przez compute_equity()).")

    



    #trades_df = trades_df.sort_values(by='exit_time')

    # Ustawienie opcji wyświetlania, żeby pokazać wszystkie kolumny


    plt.figure(figsize=(12, 5))
    plt.plot(trades_df['exit_time'], trades_df['capital'], label='Equity Curve', color='blue')
    plt.title("Equity Curve")
    plt.xlabel("time")
    plt.ylabel("Capital")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.show()

def add_trade_marker(fig, x, y, tag, pnl, pnl_usd, price, marker_type, color, symbol, showlegend=False):
    fig.add_trace(go.Scatter(
        x=[x],
        y=[y],
        mode='markers',
        name=marker_type,
        showlegend=showlegend,
        marker=dict(color=color, symbol=symbol, size=10),
        hovertemplate=
        f"Enter Tag: {tag.get('enter_tag', '')}<br>" +
        f"Exit Tag: {tag.get('exit_tag', '')}<br>" +
        f"PnL: {pnl:.5f} " +
        f"Profit: {pnl_usd:.5f} " +
        f"Price: {price:.5f}<br>" +
        f"Time: {x}<extra></extra>"
    ))


def add_zone(fig, row, df, label, fillcolor, font_color):
    x0 = df.loc[row['idxx'], 'time'] if 'time' in df.columns else row['idxx']
    x1 = (
        df.loc[row['validate_till'], 'time']
        if pd.notna(row['validate_till']) and 'time' in df.columns
        else (row['validate_till'] if pd.notna(row['validate_till']) else df.iloc[-1]['time'])
    )

    fig.add_shape(
        type='rect',
        x0=x0,
        x1=x1,
        y0=row['low_boundary'],
        y1=row['high_boundary'],
        fillcolor=fillcolor,
        line=dict(width=0),
        layer='below'
    )
    fig.add_annotation(
        x=x0,
        y=row['high_boundary'],
        text=label,
        showarrow=False,
        yshift=10,
        font=dict(color=font_color),
        bgcolor="white",
        opacity=0.8
    )


def plot_trades_with_indicators(df, trades, bullish_zones=None, bearish_zones=None, extra_series=None, bool_series=None, save_path=None):
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        vertical_spacing=0.02, row_heights=[0.9, 0.1])

    # Heikin-Ashi Świece
    fig.add_trace(go.Candlestick(x=df['time'],
                                 open=df['open'],
                                 high=df['high'],
                                 low=df['low'],
                                 close=df['close'],
                                 name='candlestick'), row=1, col=1)

    # Transakcje
    shown_legend = {"Entry": False, "custom_SL": False, "custom_TP": False, "manual_exit": False}
    for _, trade in trades.iterrows():
        # ENTRY
        add_trade_marker(
            fig=fig,
            x=trade['entry_time'],
            y=trade['entry_price'],
            tag=trade,
            pnl=trade['pnl'],
            pnl_usd=trade['pnl_usd'],
            price=trade['entry_price'],
            marker_type='Entry',
            color='black',
            symbol='circle',
            showlegend=not shown_legend["Entry"]
        )
        shown_legend["Entry"] = True

        # EXIT
        exit_tag = trade.get('exit_tag', 'manual_exit')
        if 'TP' in exit_tag:
            color, symbol, legend_key = 'blue', 'triangle-down', 'custom_TP'
        elif 'SL' in exit_tag:
            color, symbol, legend_key = 'orange', 'triangle-up', 'custom_SL'
        else:
            color, symbol, legend_key = 'gray', 'x', 'manual_exit'

        add_trade_marker(
            fig=fig,
            x=trade['exit_time'],
            y=trade['exit_price'],
            tag=trade,
            pnl=trade['pnl'],
            pnl_usd=trade['pnl_usd'],
            price=trade['exit_price'],
            marker_type=legend_key,
            color=color,
            symbol=symbol,
            showlegend=not shown_legend[legend_key]
        )
        shown_legend[legend_key] = True

        # Linia między ENTRY a EXIT
        fig.add_trace(go.Scatter(
            x=[trade['entry_time'], trade['exit_time']],
            y=[trade['entry_price'], trade['exit_price']],
            mode='lines',
            line=dict(color='gray', width=1, dash='dot'),
            showlegend=False,
            hoverinfo='skip'
        ))

    # Średnie, linie itd.
    if extra_series:
        for extra in extra_series:
            if len(extra) == 4:
                name, series, color, dash = extra
                line_style = dict(color=color, dash=dash)
            else:
                name, series = extra[:2]
                line_style = dict()
            fig.add_trace(go.Scatter(
                x=df['time'].shift(20),
                y=series,
                mode='lines',
                name=name,
                line=line_style
            ))

    # BULLISH ZONES z legendą
    if bullish_zones is not None:
        for zone in bullish_zones:
            if len(zone) == 3:
                zone_name, zone_df, fillcolor = zone
            else:
                zone_name, zone_df = zone
                fillcolor = 'rgba(255, 152, 0, 0.4)'  # pomarańcz (FVG) lub domyślny
            if zone_df.empty:
                continue
            for i, (_, row) in enumerate(zone_df.iterrows()):
                x0 = row['time']
                x1 = row['validate_till_time'] if pd.notna(row['validate_till_time']) else df['time'].iloc[-1]
                y0 = row['low_boundary']
                y1 = row['high_boundary']
                fig.add_trace(go.Scatter(
                    x=[x0, x1, x1, x0, x0],
                    y=[y0, y0, y1, y1, y0],
                    fill='toself',
                    fillcolor=fillcolor,
                    line=dict(width=0),
                    mode='lines',
                    name=zone_name if i == 0 else None,
                    showlegend=(i == 0),
                    opacity=0.4,
                    hoverinfo='skip'
                ), row=1, col=1)

    # BEARISH ZONES z legendą
    if bearish_zones is not None:
        for zone in bearish_zones:
            if len(zone) == 3:
                zone_name, zone_df, fillcolor = zone
            else:
                zone_name, zone_df = zone
                fillcolor = 'rgba(33, 150, 243, 0.4)'  # niebieski (BB) lub domyślny
            if zone_df.empty:
                continue
            for i, (_, row) in enumerate(zone_df.iterrows()):
                x0 = row['time']
                x1 = row['validate_till_time'] if pd.notna(row['validate_till_time']) else df['time'].iloc[-1]
                y0 = row['low_boundary']
                y1 = row['high_boundary']
                fig.add_trace(go.Scatter(
                    x=[x0, x1, x1, x0, x0],
                    y=[y0, y0, y1, y1, y0],
                    fill='toself',
                    fillcolor=fillcolor,
                    line=dict(width=0),
                    mode='lines',
                    name=zone_name if i == 0 else None,
                    showlegend=(i == 0),
                    opacity=0.4,
                    hoverinfo='skip'
                ), row=1, col=1)

    # Dolny pasek z binarną serią
    if bool_series:
        for name, bool_series, color in bool_series:
            if bool_series is None or not isinstance(bool_series, pd.Series):
                continue
            bar_y = bool_series.astype(int)
            fig.add_trace(go.Bar(
                x=df['time'],
                y=bar_y,
                name=name,
                marker_color=color,
                opacity=0.6,
            ), row=2, col=1)

    # Linie pomocnicze z pivotów (HH, LL, LH, HL)
    if 'pivot_15' in df.columns:
        pivot_map = {
            3: {'color': 'red',   'label': 'HH'},
            4: {'color': 'green', 'label': 'LL'},
            5: {'color': 'red',   'label': 'LH'},
            6: {'color': 'green', 'label': 'HL'},
        }

    low_rolling_min = df['low'].rolling(16).min()
    high_rolling_max = df['high'].rolling(16).max()

        # Użyj rolling wartości w pętli
    for i, row in df.reset_index(drop=True).iterrows():
        pivot_val = row['pivot_15']
        if pivot_val in pivot_map:
            info = pivot_map[pivot_val]
            start_idx = max(i - 15, 0)
            end_idx = min(start_idx + 30, len(df) - 1)

            x_values = [df['time'].iloc[start_idx], df['time'].iloc[end_idx]]

            if pivot_val == 3:
                y_value = row['HH_15']
            elif pivot_val == 4:
                y_value = row['LL_15']
            elif pivot_val == 5:
                y_value = row['LH_15']
            elif pivot_val == 6:
                y_value = row['HL_15']

            if pd.isna(y_value):
                continue  # pomiń jeśli rolling wartość nie istnieje

            fig.add_trace(go.Scatter(
                x=x_values,
                y=[y_value, y_value],
                mode='lines+text',
                line=dict(color=info['color'], width=1.5, dash='dash'),
                name=info['label'],
                text=[info['label'], None],
                textposition='top right',
                showlegend=False,
                hoverinfo='text'
            ), row=1, col=1)
    else:
        print("no pivot")

    fig.update_layout(
        title='Wykres z transakcjami i strefami',
        xaxis_title='Czas',
        yaxis_title='Cena',
        xaxis_rangeslider_visible=False,
        height=800
    )

    if save_path:
        folder = os.path.dirname(save_path)
        if folder and not os.path.exists(folder):
            os.makedirs(folder, exist_ok=True)
        try:
            fig.write_image(save_path)
            print(f"Wykres zapisany do {save_path}")
        except Exception as e:
            print(f"Nie udało się zapisać PNG: {e}")
            html_path = save_path.rsplit('.', 1)[0] + ".html"
            fig.write_html(html_path)
            print(f"Wykres zapisany jako HTML do {html_path}")
    else:
        fig.show(renderer="browser")