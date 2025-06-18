import MetaTrader5 as mt5
import time
import os
import json
from datetime import datetime, timedelta, timezone

def send_order(symbol, direction, volume, close, time, sl, tp, comment="", max_retries=5, retry_delay=0.5):
    symbol_info = mt5.symbol_info(symbol)
    if not symbol_info:
        print(f"❌ Nie można pobrać informacji o symbolu: {symbol}")
        return None

    point = symbol_info.point
    stops_level = symbol_info.trade_stops_level  # liczba punktów
    step = symbol_info.trade_tick_size
    min_stop_distance = (stops_level + 5) * point  # z buforem bezpieczeństwa

    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        print(f"❌ Brak ticków dla {symbol}")
        return None

    order_type = mt5.ORDER_TYPE_BUY if direction == "long" else mt5.ORDER_TYPE_SELL
    order_price = tick.bid if direction == "long" else tick.bid

    order_price = tick.bid if direction == "long" else tick.bid
    order_price_ask = tick.ask if direction == "long" else tick.bid

    # 🟡 Kandydaci przed korektą
    sl = round(sl / step) * step
    tp = round(tp / step) * step

    sl = sl - min_stop_distance
    tp = tp + min_stop_distance
    dist_sl_close = abs(close - sl)
    dist_tp_close = abs(tp - close)

    dist_sl_bid_price = abs(order_price - sl)
    dist_tp_bid_price = abs(tp - order_price)

    dist_sl_ask_price = abs(order_price_ask - sl)
    dist_tp_ask_price = abs(tp - order_price_ask)

    print(f"📍 {symbol} | {direction.upper()} | Cena rynkowa: {order_price:.5f} | Cena sygnału: {close:.5f}| {comment} | CZAS SYGNAŁU: {time} ")
    print(f"🟨 Kandydat SL: {sl:.5f}, TP: {tp:.5f}")
    print(f"🔎 Dystans SL od row[close].iloc[-1]: {dist_sl_close:.5f}, TP: {dist_tp_close:.5f} | Min wymagany dystans: {min_stop_distance:.5f}")
    print(f"🔎 Dystans SL od order_price.ask: {dist_sl_ask_price:.5f}, TP: {dist_tp_ask_price:.5f} | Min wymagany dystans: {min_stop_distance:.5f}")
    print(f"🔎 Dystans SL od order_price.bid: {dist_sl_bid_price:.5f}, TP: {dist_tp_bid_price:.5f} | Min wymagany dystans: {min_stop_distance:.5f}")

    # ✅ Korekta SL jeśli za blisko
    if dist_sl_bid_price < min_stop_distance:
        sl = round((order_price - min_stop_distance) / step, 5) * step if direction == "long" \
            else round((order_price + min_stop_distance) / step, 5) * step
        dist_sl_bid_price = abs(order_price - sl)
        print(f"⚠️ SL był zbyt blisko ➡️ skorygowany na: {sl:.5f} (nowy dystans: {dist_sl_bid_price:.5f}")

    # ✅ Korekta TP jeśli za blisko
    if dist_tp_bid_price < min_stop_distance:
        tp = round((order_price + min_stop_distance) / step, 5) * step if direction == "long" \
            else round((order_price - min_stop_distance) / step, 5) * step
        dist_tp_bid_price = abs(tp - order_price)
        print(f"⚠️ TP był zbyt blisko ➡️ skorygowany na: {tp:.5f} (nowy dystans: {dist_tp_bid_price:.5f}")

    print(f"⚠️ final SL: {sl:.5f} ⚠️ final TP: {tp:.5f}")
    # 📦 Request do MT5

    order_price = tick.ask if direction == "long" else tick.bid
    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": symbol,
        "volume": volume,
        "type": order_type,
        "price": order_price,
        "sl": sl,
        "tp": tp,
        "deviation": 30,
        "magic": 234000,
        "comment": comment,
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }

    result = mt5.order_send(request)
    if result is None:
        print("Błąd wysłania zlecenia: brak odpowiedzi z MT5")
        return None
    if result.retcode != mt5.TRADE_RETCODE_DONE:
        print(f"Błąd zlecenia: {result.retcode} - {result.comment}")
        return result

    return result


def get_open_positions(symbol):
    positions = mt5.positions_get(symbol=symbol)
    if positions is None:
        return []
    return list(positions)


def load_active_trades():
    try:
        with open("active_trades.json", "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        return {}


def save_active_trades(trades):
    with open("active_trades.json", "w") as f:
        json.dump(trades, f, indent=4)


def close_position(position):
    direction = position.type
    symbol = position.symbol
    volume = position.volume
    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        print(f"❌ Brak ticków dla {symbol}")
        return None

    if tick.bid == 0 or tick.ask == 0:
        print(f"❌ Brak realnych cen (bid/ask = 0) dla {symbol}")
        return None
    price = mt5.symbol_info_tick(symbol).bid if direction == mt5.ORDER_TYPE_BUY else mt5.symbol_info_tick(symbol).ask
    order_type = mt5.ORDER_TYPE_SELL if direction == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY

    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": symbol,
        "volume": volume,
        "type": order_type,
        "position": position.ticket,
        "price": price,
        "deviation": 20,
        "magic": 123456,
        "comment": "Auto-close",
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_RETURN,
    }

    print(price)
    print(request)
    result = mt5.order_send(request)
    return result

def modify_stop_loss(trade_id, new_sl):
    # Pobierz informacje o pozycji
    position = mt5.positions_get(ticket=trade_id)
    if not position:
        print(f"❌ Nie znaleziono pozycji o ID {trade_id}")
        return False
    position = position[0]

    request = {
        "action": mt5.TRADE_ACTION_SLTP,
        "position": trade_id,
        "sl": new_sl,
        "tp": position.tp,
        "symbol": position.symbol,
    }

    result = mt5.order_send(request)
    if result.retcode == mt5.TRADE_RETCODE_DONE:
        return True
    else:
        print(f"❌ Błąd modyfikacji SL: {result.comment}")
        return False
    
