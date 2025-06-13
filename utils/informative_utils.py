import pandas as pd
import config
from utils.data_loader import get_data
import MetaTrader5 as mt5
from utils.decorators import informative  # ← jeśli masz taką funkcję

def pandas_freq_from_timeframe(tf: str) -> str:
    mapping = {
        'H1': '1h',
        'H4': '4h',
        'D1': '1d',
        'M1': '1min',
        'M5': '5min',
        'M15': '15min',
    }
    return mapping.get(tf.upper(), tf)


def get_informative_dataframe(symbol, timeframe: str, startup_candle_count: int) -> pd.DataFrame:
    freq = pandas_freq_from_timeframe(timeframe)
    tf_minutes = pd.to_timedelta(freq).total_seconds() / 60
    extra_minutes = tf_minutes * startup_candle_count
    start_time = pd.to_datetime(config.TIMERANGE['start'], utc=True) - pd.to_timedelta(extra_minutes, unit='m')
    end_time = pd.to_datetime(config.TIMERANGE['end'], utc=True)

    df = get_data(
        symbol,
        getattr(mt5, f"TIMEFRAME_{timeframe}"),
        start_time,
        end_time
    )

    # Zapewnij, że 'time' ma strefę UTC
    if df['time'].dt.tz is None:
        df['time'] = df['time'].dt.tz_localize('UTC')
    else:
        df['time'] = df['time'].dt.tz_convert('UTC')

    return df


def merge_informative_data(df: pd.DataFrame, timeframe: str, informative_df: pd.DataFrame) -> pd.DataFrame:
    freq = pandas_freq_from_timeframe(timeframe)
    time_col = f'time_{timeframe}'

    # Zapewnij, że df['time'] ma strefę UTC
    if df['time'].dt.tz is None:
        df['time'] = df['time'].dt.tz_localize('UTC')
    else:
        df['time'] = df['time'].dt.tz_convert('UTC')

    df[time_col] = df['time'].dt.floor(freq)

    # Upewnij się, że informative_df['time'] ma strefę UTC
    if informative_df['time'].dt.tz is None:
        informative_df['time'] = informative_df['time'].dt.tz_localize('UTC')
    else:
        informative_df['time'] = informative_df['time'].dt.tz_convert('UTC')

    informative_df = informative_df.rename(columns={
        col: f"{col}_{timeframe}" for col in informative_df.columns if col != 'time'
    })

    merged = df.merge(
        informative_df,
        left_on=time_col,
        right_on='time',
        how='left'
    )

    return merged.drop(columns=['time'], errors='ignore')


def populate_informative_indicators(obj_with_df_and_symbol):
    for attr_name in dir(obj_with_df_and_symbol):
        attr = getattr(obj_with_df_and_symbol, attr_name)
        if callable(attr) and getattr(attr, '_informative', False):
            timeframe = attr._informative_timeframe
            if timeframe not in obj_with_df_and_symbol.informative_dataframes:
                informative_df = get_informative_dataframe(
                    symbol=obj_with_df_and_symbol.symbol,
                    timeframe=timeframe,
                    startup_candle_count=obj_with_df_and_symbol.startup_candle_count
                )
                informative_df = attr(df=informative_df.copy())
                obj_with_df_and_symbol.informative_dataframes[timeframe] = informative_df
            else:
                informative_df = obj_with_df_and_symbol.informative_dataframes[timeframe]

            obj_with_df_and_symbol.df = merge_informative_data(
                obj_with_df_and_symbol.df,
                timeframe,
                informative_df
            )