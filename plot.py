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

    pd.set_option('display.max_rows', None)



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


def plot_trades_with_indicators(df, trades, bullish_zones=None, bearish_zones=None, extra_series=None, save_path=None ):
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        vertical_spacing=0.02, row_heights=[0.7, 0.3])

    # Świece
    fig.add_trace(go.Candlestick(x=df['time'],
                                 open=df['open'],
                                 high=df['high'],
                                 low=df['low'],
                                 close=df['close'],
                                 name='Świece'), row=1, col=1)



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

        # Przerywana linia między ENTRY a EXIT
        fig.add_trace(go.Scatter(
            x=[trade['entry_time'], trade['exit_time']],
            y=[trade['entry_price'], trade['exit_price']],
            mode='lines',
            line=dict(color='gray', width=1, dash='dot'),
            showlegend=False,
            hoverinfo='skip'
        ))

    #Średnie itd
    if extra_series:
        for name, series in extra_series:
            fig.add_trace(go.Scatter(
                x=df['time'],  # albo inna kolumna z czasem
                y=series,
                mode='lines',
                name=name
            ))

    # Strefy
    bullish_colors = {
        'bull_fvg': 'rgba(144, 245, 154, 0.2)',  # jaśniejszy zielony
        'bull_ob': 'rgba(76, 175, 80, 0.2)',
        'bull_fvg_H1': 'rgba(76, 175, 80, 0.4)',  # ciemniejszy zielony
        'bull_ob_H1': 'rgba(56, 142, 60, 0.4)',
    }

    bearish_colors = {
        'bear_fvg': 'rgba(255, 0, 0, 0.2)',  # jaśniejszy czerwony
        'bear_ob': 'rgba(255, 50, 50, 0.2)',
        'bear_fvg_H1': 'rgba(183, 28, 28, 0.4)',  # ciemniejszy czerwony
        'bear_ob_H1': 'rgba(198, 40, 40, 0.4)',}

    if bullish_zones is not None:
        for zone_name, zone_df in bullish_zones:
            if zone_df.empty:
                continue
            fillcolor = bullish_colors.get(zone_name, 'rgba(144, 245, 154, 0.8)')
            for _, row in zone_df.iterrows():
                x0 = row['time']  # start time (datetime)
                x1 = row['validate_till_time'] if pd.notna(row['validate_till_time']) else df['time'].iloc[-1]

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
                    text=zone_name,
                    showarrow=False,
                    yshift=10,
                    font=dict(color="green"),
                    bgcolor="white",
                    opacity=0.8
                )

    # Analogicznie dla bearish zones:
    if bearish_zones is not None:
        for zone_name, zone_df in bearish_zones:
            if zone_df.empty:
                continue
            fillcolor = bearish_colors.get(zone_name, 'rgba(255, 0, 0, 0.2)')
            for _, row in zone_df.iterrows():
                x0 = row['time']
                x1 = row['validate_till_time'] if pd.notna(row['validate_till_time']) else df['time'].iloc[-1]

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
                    text=zone_name,
                    showarrow=False,
                    yshift=10,
                    font=dict(color="red"),
                    bgcolor="white",
                    opacity=0.8
                )

    fig.update_layout(title='Wykres transakcji z wskaźnikami',
                      xaxis_title='Czas',
                      yaxis_title='Cena',
                      xaxis_rangeslider_visible=False)

    """# Linie Asia High / Low
        fig.add_trace(go.Scatter(x=df['time'], y=df['asia_high'], mode='lines',
                                 line=dict(color='green', dash='dash'), name='Asia High'))
        fig.add_trace(go.Scatter(x=df['time'], y=df['asia_low'], mode='lines',
                                 line=dict(color='red', dash='dash'), name='Asia Low'))

        # Linie pionowe o 9:00 i 11:00
        for current_date in df['time'].dt.date.unique():
            for hour in [9, 11]:
                time_point = pd.Timestamp(f"{current_date} {hour:02d}:00:00")
                if time_point in df['time'].values:
                    fig.add_shape(
                        type='line',
                        x0=time_point, x1=time_point,
                        y0=df['low'].min(), y1=df['high'].max(),
                        line=dict(color="blue", dash="dash"),
                        xref='x', yref='y'
                    )"""
    # Na końcu funkcji:
    if save_path:
        folder = os.path.dirname(save_path)
        if folder and not os.path.exists(folder):
            os.makedirs(folder, exist_ok=True)

        if not save_path.lower().endswith(".html"):
            save_path += ".html"

        fig.write_html(save_path)
    else:
        fig.show(renderer="browser")