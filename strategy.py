# strategy.py
import pandas as pd
from TA_function import find_pivots
# import talib.abstract as ta

class MovingAverageCrossoverStrategy:
    def __init__(self, df: pd.DataFrame):
        """
        Inicjalizacja strategii na podstawie dostarczonych danych.
        """
        self.df = df.copy()
        self.signals = []

    def populate_indicators(self):
        """
        Dodaje wskaźniki do ramki danych.
        """
        self.df['ma_fast'] = self.df['close'].rolling(window=5).mean()
        self.df['ma_slow'] = self.df['close'].rolling(window=20).mean()

    def populate_entry_trend(self) -> dict | None:
        """
        Sprawdza warunki wejścia. Zwraca sygnał lub None.
        """
        if self.df['ma_fast'].iloc[-2] < self.df['ma_slow'].iloc[-2] and self.df['ma_fast'].iloc[-1] > self.df['ma_slow'].iloc[-1]:
            return {
                "signal": "entry",
                "direction": "buy",
                "enter_tag": "MA crossover up",
                "timestamp": self.df['time'].iloc[-1]
            }
        elif self.df['ma_fast'].iloc[-2] > self.df['ma_slow'].iloc[-2] and self.df['ma_fast'].iloc[-1] < self.df['ma_slow'].iloc[-1]:
            return {
                "signal": "entry",
                "direction": "sell",
                "enter_tag": "MA crossover down",
                "timestamp": self.df['time'].iloc[-1]
            }
        return None

    def populate_exit_trend(self, direction: str) -> dict | None:
        """
        Sprawdza warunki wyjścia z pozycji. Zwraca sygnał lub None.
        direction: 'buy' lub 'sell' (pozycja aktywna)
        """
        # Przykład logiki: zamknij BUY jeśli szybka MA spada poniżej wolnej
        if direction == 'buy' and self.df['ma_fast'].iloc[-1] < self.df['ma_slow'].iloc[-1]:
            return {
                "signal": "exit",
                "direction": "buy",
                "exit_tag": "MA crossover reverse",
                "timestamp": self.df['time'].iloc[-1]
            }
        elif direction == 'sell' and self.df['ma_fast'].iloc[-1] > self.df['ma_slow'].iloc[-1]:
            return {
                "signal": "exit",
                "direction": "sell",
                "exit_tag": "MA crossover reverse",
                "timestamp": self.df['time'].iloc[-1]
            }
        return None