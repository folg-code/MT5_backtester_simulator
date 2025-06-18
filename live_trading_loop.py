import MetaTrader5 as mt5
import time
import traceback
import config
from datetime import datetime, timedelta, timezone
import pandas as pd
from utils.data_loader import get_data
from utils.strategy_loader import load_strategy
from utils.trade_executor import send_order, get_open_positions, close_position, modify_stop_loss
from concurrent.futures import ProcessPoolExecutor
import os
import json
import sys

# Przekierowanie stdout do pliku logu
log_file = open("trading_log.txt", "a", encoding="utf-8")
sys.stdout = log_file

def initialize_mt5():
    if not mt5.initialize():
        print("❌ MT5 init error:", mt5.last_error())
        return False
    print("✅ MetaTrader5 zainicjalizowany.")
    return True

def shutdown_mt5():
    mt5.shutdown()
    print("📴 Połączenie z MetaTrader5 zostało zamknięte.")

def prepare_initial_data():
    data = {}
    start_dt = datetime.utcnow().replace(tzinfo=timezone.utc) - timedelta(days=60)
    end_dt = datetime.utcnow().replace(minute=0, second=0, microsecond=0, tzinfo=timezone.utc)

    for symbol in config.SYMBOLS:
        df = get_data(
            symbol,
            config.TIMEFRAME_MAP[config.TIMEFRAME],
            start_dt,
            end_dt
        )
        if df is not None and not df.empty:
            data[symbol] = df
        else:
            print(f"⚠️ Brak danych początkowych dla: {symbol}")
    return data

def fetch_and_recalculate_data(strategy, symbol):
    start_dt = datetime.utcnow().replace(tzinfo=timezone.utc) - timedelta(days=60)
    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    minute_rounded = now.minute - (now.minute % 5)
    end_dt = now.replace(minute=minute_rounded, second=0, microsecond=0)

    try:
        df_new = get_data(
            symbol,
            config.TIMEFRAME_MAP[config.TIMEFRAME],
            start_dt,
            end_dt
        )
        if df_new is not None and not df_new.empty:
            strategy.df = df_new.reset_index(drop=True)
            return True
        else:
            print(f"⏳ Brak danych dla {symbol} w podanym zakresie")
            return False
    except Exception as e:
        print(f"❌ Błąd podczas pobierania danych dla {symbol}: {e}")
        return False

def load_active_trades():
    try:
        with open("active_trades.json", "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def save_active_trades(trades):
    with open("active_trades.json", "w") as f:
        json.dump(trades, f, indent=4)




TRADES_FILE = "executed_trades.json"

def load_executed_trades():
    if not os.path.exists(TRADES_FILE):
        return {}
    try:
        with open(TRADES_FILE, "r") as f:
            data = json.load(f)
            if isinstance(data, dict):
                return data
            else:
                print("⚠️ Błąd struktury pliku executed_trades.json — spodziewano się słownika.")
                return {}
    except json.JSONDecodeError:
        print("⚠️ Błąd dekodowania JSON — pusty lub uszkodzony plik.")
        return {}

def save_executed_trades(trades):
    with open(TRADES_FILE, "w") as f:
        json.dump(trades, f, indent=4)

def record_trade_entry(tag, symbol, direction, price, volume, entry_time, sl, tp1, tp2, trade_id, open_price):
    trades = load_executed_trades()
    trades[tag] = {
        "symbol": symbol,
        "direction": direction,
        "entry_price": price,
        "open_price": open_price,
        "entry_time": entry_time,
        "entry_tag": tag,
        "volume": volume,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp1_hit": False,
        "exit_price": None,
        "exit_time": None,
        "exit_reason": None,
        "exit_tag": None,
        "trade_id": trade_id
    }
    save_executed_trades(trades)

def mark_tp1_hit(tag):
    trades = load_executed_trades()
    if tag in trades:
        trades[tag]["tp1_hit"] = True
        save_executed_trades(trades)
    else:
        print(f"⚠️ Nie znaleziono pozycji z tagiem '{tag}' przy próbie oznaczenia TP1 jako trafionego.")

def record_trade_exit(tag, price, time, reason, exit_tag):
    trades = load_executed_trades()
    if tag in trades:
        trades[tag]["exit_price"] = price
        trades[tag]["exit_time"] = time
        trades[tag]["exit_reason"] = reason
        trades[tag]["exit_tag"] = exit_tag
        save_executed_trades(trades)
    else:
        print(f"⚠️ Nie znaleziono pozycji z tagiem '{tag}' przy próbie zapisu wyjścia.")

def run_strategy_and_manage_position(strategy, symbol):
    signals = strategy.run()
    if signals is None:
        return

    symbol_info = mt5.symbol_info(symbol)
    if not symbol_info:
        print(f"❌ Nie można pobrać informacji o symbolu: {symbol}")
        return

    latest_row = strategy.df.iloc[-1]
    signal_entry = latest_row.get("signal_entry")
    signal_exit = latest_row.get("signal_exit")

    # Pobierz WSZYSTKIE otwarte pozycje na koncie (bez filtrowania po symbolu)
    open_positions = mt5.positions_get()
    if open_positions is None:
        open_positions = []
    #print(f"Otwartych pozycji na koncie: {len(open_positions)}")
    #for p in open_positions:
        #print(f"Ticket: {p.ticket}, Symbol: {p.symbol}, Volume: {p.volume}")

    active_trades = load_active_trades()
    open_trade_ids = [p.ticket for p in open_positions]

    # Usuń z active_trades te, które nie mają już otwartej pozycji na koncie MT5
    to_remove = []
    for tag, trade in active_trades.items():
        trade_id = trade.get("trade_id")
        if trade_id not in open_trade_ids:
            print(f"Pozycja {tag} z trade_id {trade_id} jest zamknięta, usuwam z active_trades")
            to_remove.append(tag)
    for tag in to_remove:
        active_trades.pop(tag)
    if to_remove:
        save_active_trades(active_trades)

    # Wypisz aktywne pozycje z active_trades
    #for tag, trade in active_trades.items():
        #print(f"ℹ️ Aktywna pozycja {tag}: symbol={trade.get('symbol')}, direction={trade.get('direction').upper()}, "
        #      f"volume={trade.get('volume')}, open_price={trade.get('open_price')}, SL={trade.get('sl')}, TP={trade.get('tp')}")

    if isinstance(signal_entry, tuple):
        print(f"➡️ Nowy sygnał wejścia dla {symbol}: kierunek={signal_entry[0]}, tag={signal_entry[1]}")

    if isinstance(signal_exit, tuple):
        print(f"⬅️ Nowy sygnał wyjścia dla {symbol}: kierunek={signal_exit[0]}, tagi={signal_exit[1]}, powód={signal_exit[2]}")

    if isinstance(signal_entry, tuple):
        direction, tag = signal_entry
        entry_tag = f"{tag}"

        print(latest_row['signal_entry'])
        print(latest_row['levels'])

        if entry_tag not in active_trades:
            levels = latest_row["levels"]
            if not isinstance(levels, tuple) or len(levels) != 3:
                print(f"❌ Nieprawidłowe poziomy wejścia dla {symbol}")
                return

            sl = levels[0][1]
            tp1 = levels[2][1]
            tp2 = levels[2][1]

            sl_exit_tag = levels[0][2]
            tp1_exit_tag = levels[1][2]
            tp2_exit_tag = levels[2][2]

            result = send_order(
                symbol=symbol,
                direction=direction,
                volume=config.INITIAL_SIZE,
                close=latest_row['close'],
                time=latest_row['time'],
                sl=sl,
                tp=tp2,
                comment=entry_tag
            )
            print(result)
            if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                open_price = result.price
                trade_id = result.order
                print(f"✅ Otwarta pozycja {direction} dla {symbol} po cenie {open_price} z {entry_tag} z TP: {tp2}, SL: {sl}")

                active_trades[entry_tag] = {
                    "symbol": symbol,
                    "trade_id": trade_id,
                    "open_price": open_price,
                    "entry_time": datetime.utcnow().isoformat(),
                    "direction": direction,
                    "sl": sl,
                    "tp1": tp1,
                    "tp": tp2,
                    "volume": config.INITIAL_SIZE,
                    "tp1_closed": False,
                    "tp2_closed": False
                }
                save_active_trades(active_trades)

                record_trade_entry(
                tag=entry_tag,
                symbol=symbol,
                direction=direction,
                price=latest_row['close'],
                volume=config.INITIAL_SIZE,
                entry_time=datetime.utcnow().isoformat(),
                sl=sl,
                tp1=tp1,
                tp2=tp2,
                trade_id=trade_id,
                open_price=open_price
            )
            else:
                print(f"❌ Błąd wysyłania zlecenia: {getattr(result, 'comment', 'brak odpowiedzi')}")
        #else:
            #print(f"⚠️ Pozycja z tagiem '{entry_tag}' już istnieje — pomijam wejście.")

    # Zarządzanie aktywnymi pozycjami na podstawie open_positions i active_trades
    for tag, trade in list(active_trades.items()):
        trade_id = trade.get("trade_id")
        pos = next((p for p in open_positions if p.ticket == trade_id), None)

        #print(f"Szukanie pozycji o trade_id={trade_id}")
        #print(f"Dostępne bilety pozycji: {[p.ticket for p in open_positions]}")

        if pos is None:
            print(f"⚠️ Pozycja dla trade_id {trade_id} nie znaleziona na koncie MT5.")
            continue

        #print(f"Pozycja dla {tag}: symbol={pos.symbol}, volume={pos.volume}, SL={pos.sl}, TP={pos.tp}")

        trade_volume = trade.get("volume", pos.volume)
        direction = trade["direction"]

        # TP1 - częściowe zamknięcie
        if not trade.get("tp1_closed", False):
            tp1_price = trade["tp1"]
            if (pos.type == mt5.ORDER_TYPE_BUY and pos.price_current >= tp1_price) or \
               (pos.type == mt5.ORDER_TYPE_SELL and pos.price_current <= tp1_price):

                volume_to_close = trade_volume / 2
                result = close_position(pos, volume=volume_to_close)
                if result.retcode == mt5.TRADE_RETCODE_DONE:
                    print(f"✅ Częściowo zamknięto pozycję {direction} (TP1) dla {trade['symbol']} (volume {volume_to_close})")
                    trade["volume"] -= volume_to_close
                    trade["tp1_closed"] = True

                    new_sl = trade["open_price"]
                    modify_result = modify_stop_loss(trade_id, new_sl)
                    if modify_result:
                        print(f"🔄 SL przesunięty na cenę wejścia: {new_sl}")
                    else:
                        print(f"❌ Nie udało się przesunąć SL.")

                    save_active_trades(active_trades)
                    mark_tp1_hit(tag)
                else:
                    print(f"❌ Błąd przy częściowym zamknięciu (TP1): {result.comment}")

        # TP2 - pełne zamknięcie
        if not trade.get("tp2_closed", False):
            tp2_price = trade["tp"]
            if (pos.type == mt5.ORDER_TYPE_BUY and pos.price_current >= tp2_price) or \
               (pos.type == mt5.ORDER_TYPE_SELL and pos.price_current <= tp2_price):

                volume_to_close = trade.get("volume", pos.volume)
                result = close_position(pos, volume=volume_to_close)
                if result.retcode == mt5.TRADE_RETCODE_DONE:
                    print(f"✅ Zamknięto pozycję {direction} (TP2) dla {trade['symbol']} (volume {volume_to_close})")
                    trade["tp2_closed"] = True
                    trade["volume"] = 0
                    active_trades.pop(tag, None)
                    save_active_trades(active_trades)

                    record_trade_exit(
                        tag=tag,
                        price=pos.price_current,
                        time=datetime.utcnow().isoformat(),
                        reason="TP2",
                        exit_tag=tp2_exit_tag
                    )
                else:
                    print(f"❌ Nie udało się zamknąć pozycji (TP2): {result.comment}")

    # Obsługa sygnałów wyjścia
    if isinstance(signal_exit, tuple):
        exit_direction, exit_tags, exit_reason = signal_exit

        for tag, trade in list(active_trades.items()):
            if tag in exit_tags:
                pos = next((p for p in open_positions if p.comment == tag), None)
                if pos is None:
                    continue

                volume_left = pos.volume

                if exit_reason == "full_close" and volume_left > 0:
                    result = close_position(pos)
                    if result.retcode == mt5.TRADE_RETCODE_DONE:
                        print(f"✅ Pełne zamknięcie pozycji {exit_direction} ({exit_reason}) dla {trade['symbol']}")
                        active_trades.pop(tag, None)
                        save_active_trades(active_trades)

                        record_trade_exit(
                            tag=tag,
                            price=pos.price_current,
                            time=datetime.utcnow().isoformat(),
                            reason=exit_reason,
                            exit_tag=tag
                        )
                    else:
                        print(f"❌ Nie udało się zamknąć pozycji: {result.comment}")

    if isinstance(signal_exit, tuple):
        exit_direction, exit_tags, exit_reason = signal_exit

        for tag, trade in active_trades.items():
            if tag in exit_tags:
                pos = next((p for p in open_positions if p.comment == tag), None)
                if pos is None:
                    continue

                volume_left = pos.volume

                if exit_reason == "full_close" and volume_left > 0:
                    result = close_position(pos)
                    if result.retcode == mt5.TRADE_RETCODE_DONE:
                        print(f"✅ Pełne zamknięcie pozycji {exit_direction} ({exit_reason}) dla {trade['symbol']}")
                        active_trades.pop(tag, None)
                        save_active_trades(active_trades)

                        record_trade_exit(
                            tag=tag,
                            price=pos.price_current,
                            time=datetime.utcnow().isoformat(),
                            reason=exit_reason,
                            exit_tag=tag
                        )
                    else:
                        print(f"❌ Nie udało się zamknąć pozycji: {result.comment}")

def wait_for_next_minute():
    now = datetime.utcnow()
    seconds_to_wait = 60 - now.second
    if seconds_to_wait == 60:
        seconds_to_wait = 0
    time.sleep(seconds_to_wait + 1)

def process_symbol(symbol, strategy):
    start = time.perf_counter()

    updated = fetch_and_recalculate_data(strategy, symbol)
    fetch_duration = time.perf_counter() - start

    if updated:
        try:
            strategy_start = time.perf_counter()
            run_strategy_and_manage_position(strategy, symbol)
            strategy_duration = time.perf_counter() - strategy_start
        except Exception as e:
            print(f"❌ Błąd w strategii {symbol}: {e}")
            traceback.print_exc()

    total_duration = time.perf_counter() - start

def run_live_loop(strategies):
    print("Live trading started...")

    while True:
        print('NEW LOOP')
        wait_for_next_minute()

        with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = [
                executor.submit(process_symbol, symbol, strategy)
                for symbol, strategy in strategies.items()
            ]

def prepare_strategies():
    strategies = {}
    start_dt = datetime.utcnow().replace(tzinfo=timezone.utc) - timedelta(days=60)
    end_dt = datetime.strptime(config.TIMERANGE['end'], "%Y-%m-%d")

    for symbol in config.SYMBOLS:
        df = get_data(symbol, config.TIMEFRAME_MAP[config.TIMEFRAME], start_dt, end_dt)
        strategy = load_strategy(config.strategy, df, symbol, config.TIMEFRAME_MAP[config.TIMEFRAME])
        strategies[symbol] = strategy
    return strategies

def main():
    if not initialize_mt5():
        print("mt5 error")
        return

    try:
        print("start")
        strategies = prepare_strategies()
        print("end prepare_strategies")
        run_live_loop(strategies)
    except KeyboardInterrupt:
        print("🛌 Zatrzymano przez użytkownika")
    finally:
        shutdown_mt5()
        log_file.close()

if __name__ == "__main__":
    main()