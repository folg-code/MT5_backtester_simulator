import pandas as pd

class Backtest:
    def __init__(self, df: pd.DataFrame, strategy_class, initial_capital=10000, lot_size=1000):
        self.df = df
        self.strategy_class = strategy_class
        self.capital = initial_capital
        self.lot_size = lot_size
        self.position = None
        self.entry_price = 0
        self.trades = []

    def run(self):
        for i in range(20, len(self.df)):
            data_slice = self.df.iloc[:i+1].copy()
            strat = self.strategy_class(data_slice)
            strat.populate_indicators()

            if self.position is None:
                signal = strat.populate_entry_trend()
                if signal:
                    self.position = signal['direction']
                    self.entry_price = data_slice['close'].iloc[-1]
                    self.trades.append({
                        "timestamp": signal['timestamp'],
                        "type": "entry",
                        "direction": self.position,
                        "price": self.entry_price,
                        "enter_tag": signal['enter_tag']
                    })
            else:
                signal = strat.populate_exit_trend(self.position)
                if signal:
                    exit_price = data_slice['close'].iloc[-1]
                    pnl = (exit_price - self.entry_price) * (1 if self.position == 'buy' else -1) * self.lot_size
                    self.capital += pnl
                    self.trades.append({
                        "timestamp": signal['timestamp'],
                        "type": "exit",
                        "direction": self.position,
                        "price": exit_price,
                        "pnl": pnl,
                        "capital": self.capital,
                        "exit_tag": signal['exit_tag']
                    })
                    self.position = None
                    self.entry_price = 0

    def report(self):
        df_trades = pd.DataFrame(self.trades)
        print("\n=== Trades ===")
        print(df_trades)
        print(f"\nFinal capital: {self.capital:.2f}")
        return df_trades