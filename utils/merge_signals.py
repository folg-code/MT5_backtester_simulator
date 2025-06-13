from functools import wraps
import pandas as pd
import numpy as np
import copy


def merge_signals_decorator(func):
    def wrapper(self, *args, **kwargs):
        result = func(self, *args, **kwargs)

        entries = getattr(self, 'entries', None)
        if entries is None:
            raise ValueError("Brak atrybutu 'entries' w obiekcie po wykonaniu funkcji")

        df = self.df
        # Nie czyścimy signal, bo już są w df lub wpisujemy poniżej

        index_pos = {idx: pos for pos, idx in enumerate(df.index)}

        for idx, signals in entries.items():
            if idx in index_pos:
                pos = index_pos[idx]
                first = signals[0][0]
                second = signals[0][1]
                combined_tag = "_".join(signal[2] for signal in signals)
                df.iat[pos, df.columns.get_loc('signal')] = (first, second, combined_tag)
            else:
                print(f"Warning: idx {idx} not in df.index — sygnał pominięty")

        return result
    return wrapper