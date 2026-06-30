#!/usr/bin/env python3
import asyncio
import json
import threading
from datetime import datetime
from statistics import median
from flask import Flask, render_template, jsonify
import websockets
import aiohttp

SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'XRPUSDT', 'ADAUSDT',
           'DOGEUSDT', 'DOTUSDT', 'LINKUSDT', 'MATICUSDT', 'AVAXUSDT']
ANOMALY_MULTIPLIER = 20
MIN_VOLUME_USDT = 1_000_000
MIN_LIFETIME_SEC = 25
DEPTH = 20

app = Flask(__name__)
data_store = {'binance': {}, 'okx': {}, 'bybit': {}}
active_walls = {'binance': {}, 'okx': {}, 'bybit': {}}

def format_price(p): return f"${p:.2f}"
def format_volume(v): return f"${v:,.0f}"

def find_anomalies(bids, asks):
    all_vol = [v for _, v in bids] + [v for _, v in asks]
    if not all_vol:
        return [], []
    med = median(all_vol) or 1e-6
    an_bids = []
    for p, v in bids:
        cost = p * v
        if cost >= MIN_VOLUME_USDT and v / med >= ANOMALY_MULTIPLIER:
            an_bids.append((p, v, cost, v/med))
    an_asks = []
    for p, v in asks:
        cost = p * v
        if cost >= MIN_VOLUME_USDT and v / med >= ANOMALY_MULTIPLIER:
            an_asks.append((p, v, cost, v/med))
    an_bids.sort(key=lambda x: x[3], reverse=True)
    an_asks.sort(key=lambda x: x[3], reverse=True)
    return an_bids, an_asks

async def fetch_depth_binance(symbol):
    sym = symbol.lower()
    url = f"wss://stream.binance.com:9443/ws/{sym}@depth{DEPTH}"
    while True:
        try:
            async with websockets.connect(url) as ws:
                while True:
                    msg = await ws.recv()
                    data = json.loads(msg)
                    if "bids" not in data or "asks" not in data:
                        continue
                    bids = [[float(p), float(v)] for p, v in data["bids"][:DEPTH]]
                    asks = [[float(p), float(v)] for p, v in data["asks"][:DEPTH]]
                    if not bids or not asks:
                        continue
                    price = (bids[0][0] + asks[0][0]) / 2.0
                    an_bids, an_asks = find_anomalies(bids, asks)
                    update_store('binance', symbol, price, an_bids, an_asks)
        except:
            await asyncio.sleep(2)

async def fetch_depth_okx(symbol):
    url = "wss://ws.okx.com:8443/ws/v5/public"
    sym = symbol.replace('USDT', '-USDT')
    sub = {"op": "subscribe", "args": [{"channel": "books-l2-tbt", "instId": sym}]}
    while True:
        try:
            async with websockets.connect(url) as ws:
                await ws.send(json.dumps(sub))
                while True:
                    msg = await ws.recv()
                    data = json.loads(msg)
                    if 'data' not in data:
                        continue
                    for item in data['data']:
                        if item['instId'] != sym:
                            continue
                        bids = [[float(p), float(v)] for p, v in item.get('bids', [])[:DEPTH]]
                        asks = [[float(p), float(v)] for p, v in item.get('asks', [])[:DEPTH]]
                        if not bids or not asks:
                            continue
                        price = (bids[0][0] + asks[0][0]) / 2.0
                        an_bids, an_asks = find_anomalies(bids, asks)
                        update_store('okx', symbol, price, an_bids, an_asks)
        except:
            await asyncio.sleep(2)

async def fetch_depth_bybit(symbol):
    url = "wss://stream.bybit.com/v5/public/spot"
    sym = symbol
    sub = {"op": "subscribe", "args": [f"orderbook.200.{sym}"]}
    while True:
        try:
            async with websockets.connect(url) as ws:
                await ws.send(json.dumps(sub))
                while True:
                    msg = await ws.recv()
                    data = json.loads(msg)
                    if 'topic' not in data or 'data' not in data:
                        continue
                    if data['topic'] != f"orderbook.200.{sym}":
                        continue
                    book = data['data']
                    bids = [[float(p), float(v)] for p, v in book.get('b', [])[:DEPTH]]
                    asks = [[float(p), float(v)] for p, v in book.get('a', [])[:DEPTH]]
                    if not bids or not asks:
                        continue
                    price = (bids[0][0] + asks[0][0]) / 2.0
                    an_bids, an_asks = find_anomalies(bids, asks)
                    update_store('bybit', symbol, price, an_bids, an_asks)
        except:
            await asyncio.sleep(2)

def update_store(exchange, symbol, price, an_bids, an_asks):
    data_store[exchange][symbol] = {'price': price, 'bids': an_bids, 'asks': an_asks}
    now = datetime.now()
    best_bid = an_bids[0] if an_bids else None
    best_ask = an_asks[0] if an_asks else None
    cur = set()
    if best_bid:
        cur.add((symbol, 'bid', best_bid[0]))
    if best_ask:
        cur.add((symbol, 'ask', best_ask[0]))
    for key in list(active_walls[exchange].keys()):
        if key not in cur:
            del active_walls[exchange][key]
    if best_bid:
        key = (symbol, 'bid', best_bid[0])
        if key not in active_walls[exchange]:
            active_walls[exchange][key] = {'vol': best_bid[2], 'time': now}
    if best_ask:
        key = (symbol, 'ask', best_ask[0])
        if key not in active_walls[exchange]:
            active_walls[exchange][key] = {'vol': best_ask[2], 'time': now}

def get_table_data(exchange):
    rows = []
    now = datetime.now()
    for sym, d in data_store[exchange].items():
        if not d:
            continue
        best_bid = None
        best_ask = None
        for b in d['bids']:
            key = (sym, 'bid', b[0])
            if key in active_walls[exchange] and (now - active_walls[exchange][key]['time']).total_seconds() >= MIN_LIFETIME_SEC:
                best_bid = b
                break
        for a in d['asks']:
            key = (sym, 'ask', a[0])
            if key in active_walls[exchange] and (now - active_walls[exchange][key]['time']).total_seconds() >= MIN_LIFETIME_SEC:
                best_ask = a
                break
        if not best_bid and not best_ask:
            continue
        row = {
            'symbol': sym,
            'price': format_price(d['price']),
            'bid_price': format_price(best_bid[0]) if best_bid else '—',
            'bid_vol': format_volume(best_bid[2]) if best_bid else '—',
            'ask_price': format_price(best_ask[0]) if best_ask else '—',
            'ask_vol': format_volume(best_ask[2]) if best_ask else '—',
            'bid_life': '',
            'ask_life': ''
        }
        if best_bid:
            sec = int((now - active_walls[exchange][(sym,'bid',best_bid[0])]['time']).total_seconds())
            row['bid_life'] = f"{sec}с" if sec < 60 else f"{sec//60}м {sec%60}с"
        if best_ask:
            sec = int((now - active_walls[exchange][(sym,'ask',best_ask[0])]['time']).total_seconds())
            row['ask_life'] = f"{sec}с" if sec < 60 else f"{sec//60}м {sec%60}с"
        rows.append(row)
    rows.sort(key=lambda x: float(x['bid_vol'].replace('$','').replace(',','') or 0) + float(x['ask_vol'].replace('$','').replace(',','') or 0), reverse=True)
    return rows

@app.route('/')
def index():
    return render_template('new_index.html')

@app.route('/data')
def data():
    return jsonify({
        'binance': get_table_data('binance'),
        'okx': get_table_data('okx'),
        'bybit': get_table_data('bybit')
    })

async def run_tasks():
    tasks = []
    for sym in SYMBOLS:
        tasks.append(asyncio.create_task(fetch_depth_binance(sym)))
        tasks.append(asyncio.create_task(fetch_depth_okx(sym)))
        tasks.append(asyncio.create_task(fetch_depth_bybit(sym)))
    await asyncio.gather(*tasks)

def run_flask():
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)

if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    threading.Thread(target=lambda: loop.run_until_complete(run_tasks()), daemon=True).start()
    run_flask()
