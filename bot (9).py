#!/usr/bin/env python3
"""
Bybit Order Flow Bot (Python) — для запуска 24/7 на Railway / VPS.
Только PostOnly chase (минимальная комиссия мейкера). Никаких рыночных ордеров.

Зависимости: pip install requests websockets flask
Запуск: python bot.py
На Railway: просто задеплой, укажи переменные окружения.
"""
import os, json, time, hmac, hashlib, asyncio, threading, logging
from collections import deque
from decimal import Decimal, ROUND_DOWN
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import urllib.request, urllib.parse, urllib.error

import websockets

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bot")

# ============ конфиг ============
def env(name, default=None, cast=str):
    v = os.getenv(name, default)
    if v is None:
        return None
    try:
        return cast(v)
    except Exception:
        return default

CFG = {
    "api_key": env("BYBIT_API_KEY", ""),
    "api_secret": env("BYBIT_API_SECRET", ""),
    "env": env("BYBIT_ENV", "testnet"),  # testnet | mainnet
    "deposit": env("DEPOSIT", "100", float),
    "leverage": env("LEVERAGE", "3", float),
    "max_trades": env("MAX_TRADES", "3", int),
    "portfolio_stop_pct": env("PORTFOLIO_STOP_PCT", "15", float),
    "tp_pct": env("TP_PCT", "0.9", float),
    "sl_pct": env("SL_PCT", "10", float),
    "max_hold_min": env("MAX_HOLD_MIN", "20", float),
    "universe_mode": env("UNIVERSE_MODE", "volatile"),  # auto | volatile | custom
    "top_n": env("TOP_N", "10", int),
    "min_turnover": env("MIN_TURNOVER", "5000000", float),
    "exclude_majors": env("EXCLUDE_MAJORS", "1") in ("1", "true", "yes"),
    "custom_symbols": env("CUSTOM_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT"),
    "entry_threshold": env("ENTRY_THRESHOLD", "0.35", float),
    "exit_threshold": env("EXIT_THRESHOLD", "0.45", float),
    "tape_window_sec": env("TAPE_WINDOW_SEC", "60", float),
    "ob_depth": env("OB_DEPTH", "20", int),
    "ob_weight": env("OB_WEIGHT", "0.5", float),
    "short_only": env("SHORT_ONLY", "0") in ("1", "true", "yes"),
    "chase_max_attempts": env("CHASE_MAX_ATTEMPTS", "8", int),
    "chase_interval_ms": env("CHASE_INTERVAL_MS", "1500", int),
    "require_pullback_for_short": env("REQUIRE_PULLBACK", "1") in ("1", "true", "yes"),
    "pullback_pct": env("PULLBACK_PCT", "0.15", float),
    "pullback_window_sec": env("PULLBACK_WINDOW_SEC", "30", float),
    "port": env("PORT", "8080", int),
}

# ============ Bybit REST ============
class BybitError(Exception):
    def __init__(self, path, code, msg):
        self.code = code
        self.path = path
        super().__init__(f"[{path}] {msg} (retCode={code})")

class BybitREST:
    def __init__(self, key, secret, env):
        self.key = key.strip()
        self.secret = secret.strip().encode()
        self.base = "https://api.bybit.com" if env == "mainnet" else "https://api-testnet.bybit.com"
        self.offset = 0
        self.recv = "10000"

    def _req(self, method, path, params=None, signed=False):
        params = params or {}
        url = self.base + path
        headers = {"Content-Type": "application/json"}
        payload = ""
        if method == "GET":
            qs = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
            if qs:
                url += "?" + qs
            payload = qs
        else:
            payload = json.dumps(params, separators=(",", ":"))
        if signed:
            ts = str(int(time.time() * 1000 + self.offset))
            sign = hmac.new(self.secret, (ts + self.key + self.recv + payload).encode(), hashlib.sha256).hexdigest()
            headers.update({
                "X-BAPI-API-KEY": self.key,
                "X-BAPI-TIMESTAMP": ts,
                "X-BAPI-SIGN": sign,
                "X-BAPI-RECV-WINDOW": self.recv,
            })
        data = payload.encode() if method != "GET" else None
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                res = json.loads(r.read())
        except urllib.error.HTTPError as e:
            raise BybitError(path, e.code, e.read()[:200].decode(errors="ignore"))
        except urllib.error.URLError as e:
            raise BybitError(path, -1, f"network: {e.reason}")
        if res.get("retCode") != 0:
            raise BybitError(path, res.get("retCode"), res.get("retMsg"))
        return res.get("result")

    def sync_time(self):
        try:
            r = self._req("GET", "/v5/market/time")
            server = int(r["timeNano"]) / 1e6 if r.get("timeNano") else int(r["timeSecond"]) * 1000
            self.offset = server - time.time() * 1000
        except Exception:
            pass

    def get_tickers(self):
        return self._req("GET", "/v5/market/tickers", {"category": "linear"})["list"]

    def get_instrument(self, symbol):
        r = self._req("GET", "/v5/market/instruments-info", {"category": "linear", "symbol": symbol})
        return r["list"][0] if r.get("list") else None

    def get_equity(self):
        r = self._req("GET", "/v5/account/wallet-balance", {"accountType": "UNIFIED"}, True)
        usdt = next((c for c in r["list"][0]["coin"] if c["coin"] == "USDT"), None)
        return float(usdt["equity"]) if usdt else 0.0

    def get_available(self):
        r = self._req("GET", "/v5/account/wallet-balance", {"accountType": "UNIFIED"}, True)
        return float(r["list"][0].get("totalAvailableBalance") or 0)

    def set_leverage(self, symbol, lev):
        try:
            self._req("POST", "/v5/position/set-leverage", {
                "category": "linear", "symbol": symbol,
                "buyLeverage": str(lev), "sellLeverage": str(lev)
            }, True)
        except BybitError:
            pass

    def place_limit(self, symbol, side, qty, price, reduce_only=False):
        body = {
            "category": "linear", "symbol": symbol, "side": side,
            "orderType": "Limit", "qty": str(qty), "price": str(price),
            "timeInForce": "PostOnly", "reduceOnly": reduce_only,
            "positionIdx": 0,
        }
        return self._req("POST", "/v5/order/create", body, True)["orderId"]

    def cancel(self, symbol, oid):
        try:
            self._req("POST", "/v5/order/cancel", {"category": "linear", "symbol": symbol, "orderId": oid}, True)
        except BybitError:
            pass

    def is_open(self, symbol, oid):
        try:
            r = self._req("GET", "/v5/order/realtime", {"category": "linear", "symbol": symbol, "orderId": oid}, True)
            return any(o["orderId"] == oid and o["orderStatus"] not in ("Filled", "Cancelled", "Rejected") for o in r.get("list", []))
        except Exception:
            return True

# ============ Symbol state ============
class SymbolState:
    def __init__(self, symbol):
        self.symbol = symbol
        self.bids = {}  # price str -> size
        self.asks = {}
        self.trades = deque()  # (ts, side, size)
        self.price_history = deque()  # (ts, price)
        self.last_price = None

    def apply_orderbook(self, data, is_snapshot):
        if is_snapshot:
            self.bids.clear()
            self.asks.clear()
        for p, s in data.get("b", []):
            sz = float(s)
            if sz == 0:
                self.bids.pop(p, None)
            else:
                self.bids[p] = sz
        for p, s in data.get("a", []):
            sz = float(s)
            if sz == 0:
                self.asks.pop(p, None)
            else:
                self.asks[p] = sz

    def apply_trades(self, data, window_ms, pb_window_ms):
        now = time.time() * 1000
        for t in data:
            self.last_price = float(t["p"])
            self.trades.append((now, t["S"], float(t["v"])))
            self.price_history.append((now, self.last_price))
        cutoff = now - window_ms
        while self.trades and self.trades[0][0] < cutoff:
            self.trades.popleft()
        pb_cut = now - max(window_ms, pb_window_ms)
        while self.price_history and self.price_history[0][0] < pb_cut:
            self.price_history.popleft()

    def orderbook_imbalance(self, depth):
        bid_prices = sorted((float(p) for p in self.bids), reverse=True)[:depth]
        ask_prices = sorted(float(p) for p in self.asks)[:depth]
        bid_vol = sum(self.bids.get(str(p), 0) for p in bid_prices)
        ask_vol = sum(self.asks.get(str(p), 0) for p in ask_prices)
        total = bid_vol + ask_vol
        return (bid_vol - ask_vol) / total if total else 0.0

    def tape_ratio(self):
        if not self.trades:
            return 0.0
        net = total = 0.0
        for _, side, size in self.trades:
            total += size
            net += size if side == "Buy" else -size
        return net / total if total else 0.0

    def score(self, depth, ob_weight):
        return self.orderbook_imbalance(depth) * ob_weight + self.tape_ratio() * (1 - ob_weight)

    def mid_price(self):
        if self.bids and self.asks:
            best_bid = max(float(p) for p in self.bids)
            best_ask = min(float(p) for p in self.asks)
            return (best_bid + best_ask) / 2
        return self.last_price

    def best_bid(self):
        return max((float(p) for p in self.bids), default=None)

    def best_ask(self):
        return min((float(p) for p in self.asks), default=None)

    def recent_low(self, window_ms):
        cutoff = time.time() * 1000 - window_ms
        lows = [p for t, p in self.price_history if t >= cutoff]
        return min(lows) if lows else None

    def has_pulled_back_up(self, window_ms, pct):
        low = self.recent_low(window_ms)
        price = self.last_price or self.mid_price()
        if not low or not price:
            return False
        return (price - low) / low * 100 >= pct

# ============ Engine ============
class Engine:
    def __init__(self, cfg):
        self.cfg = cfg
        self.rest = BybitREST(cfg["api_key"], cfg["api_secret"], cfg["env"])
        self.sym_states = {}
        self.positions = {}  # symbol -> dict
        self.filters = {}
        self.levered = set()
        self.initial_equity = None
        self.running = False
        self.logs = deque(maxlen=200)
        self.ws_status = "disconnected"

    def say(self, msg, level="info"):
        ts = time.strftime("%H:%M:%S")
        line = f"[{ts}] {msg}"
        self.logs.appendleft(line)
        log.info(msg)

    def state(self):
        return {
            "running": self.running,
            "equity": self.initial_equity,
            "positions": list(self.positions.values()),
            "symbols": list(self.sym_states.keys()),
            "ws": self.ws_status,
            "logs": list(self.logs)[:40],
        }

    async def resolve_universe(self):
        mode = self.cfg["universe_mode"]
        if mode == "custom":
            return [s.strip().upper() for s in self.cfg["custom_symbols"].split(",") if s.strip()]
        tickers = self.rest.get_tickers()
        majors = {"BTCUSDT", "ETHUSDT"}
        usdt = [t for t in tickers if t["symbol"].endswith("USDT")]
        if mode == "volatile":
            usdt = [t for t in usdt if float(t.get("turnover24h") or 0) >= self.cfg["min_turnover"]]
            if self.cfg["exclude_majors"]:
                usdt = [t for t in usdt if t["symbol"] not in majors]
            usdt.sort(key=lambda t: abs(float(t.get("price24hPcnt") or 0)), reverse=True)
        else:
            usdt.sort(key=lambda t: float(t.get("turnover24h") or 0), reverse=True)
        return [t["symbol"] for t in usdt[:self.cfg["top_n"]]]

    async def ensure_filters(self, symbol):
        if symbol not in self.filters:
            inst = self.rest.get_instrument(symbol)
            if not inst:
                return None
            lot = inst["lotSizeFilter"]
            pf = inst["priceFilter"]
            self.filters[symbol] = {
                "qty_step": float(lot["qtyStep"]),
                "min_qty": float(lot.get("minOrderQty") or 0),
                "tick": float(pf["tickSize"]),
            }
        if symbol not in self.levered:
            self.rest.set_leverage(symbol, self.cfg["leverage"])
            self.levered.add(symbol)
        return self.filters[symbol]

    def round_step(self, value, step):
        if not step:
            return value
        d = Decimal(str(step))
        return float((Decimal(str(value)) / d).to_integral_value(rounding=ROUND_DOWN) * d)

    async def chase(self, symbol, side, qty, tick, purpose):
        state = self.sym_states.get(symbol)
        if not state:
            return {"filled": False}
        max_a = self.cfg["chase_max_attempts"]
        interval = self.cfg["chase_interval_ms"] / 1000
        order_id = None
        quoted = None
        for attempt in range(1, max_a + 1):
            if not self.running and purpose == "entry":
                if order_id:
                    self.rest.cancel(symbol, order_id)
                return {"filled": False}
            best = state.best_bid() if side == "Buy" else state.best_ask()
            if not best:
                await asyncio.sleep(interval)
                continue
            price = self.round_step(best, tick)
            if order_id is None or price != quoted:
                if order_id:
                    self.rest.cancel(symbol, order_id)
                try:
                    order_id = self.rest.place_limit(symbol, side, qty, price, reduce_only=(purpose == "exit"))
                    quoted = price
                    self.say(f"{purpose.upper()} {symbol}: PostOnly {side} @ {price} (попытка {attempt}/{max_a})")
                except Exception as e:
                    self.say(f"{symbol}: лимитка отклонена ({e}), перевыставляю", "warn")
                    order_id = None
                    await asyncio.sleep(interval)
                    continue
            await asyncio.sleep(interval)
            if order_id and not self.rest.is_open(symbol, order_id):
                return {"filled": True, "price": quoted}
        if order_id:
            self.rest.cancel(symbol, order_id)
        self.say(f"{symbol}: {purpose} не исполнился за {max_a} попыток PostOnly", "warn")
        return {"filled": False}

    async def open_position(self, symbol, side, score):
        filters = await self.ensure_filters(symbol)
        if not filters:
            return
        state = self.sym_states[symbol]
        price = state.mid_price()
        if not price:
            return
        notional = (self.cfg["deposit"] * self.cfg["leverage"]) / self.cfg["max_trades"]
        try:
            avail = self.rest.get_available()
        except Exception as e:
            self.say(f"{symbol}: баланс недоступен ({e})", "err")
            return
        max_n = avail * self.cfg["leverage"] * 0.95
        if notional > max_n:
            notional = max_n
        qty = self.round_step(notional / price, filters["qty_step"])
        if qty < filters["min_qty"]:
            self.say(f"{symbol}: не хватает маржи на минимальный объём", "err")
            return
        res = await self.chase(symbol, "Buy" if side == "long" else "Sell", qty, filters["tick"], "entry")
        if res.get("filled"):
            self.positions[symbol] = {
                "symbol": symbol, "side": side, "qty": qty,
                "entry": res["price"], "opened": time.time(),
            }
            self.say(f"ВХОД {side.upper()} {symbol} @ {res['price']} score={score:.2f} qty={qty}", "ok")

    async def close_position(self, symbol, reason):
        pos = self.positions.get(symbol)
        if not pos:
            return
        filters = await self.ensure_filters(symbol)
        if not filters:
            return
        close_side = "Sell" if pos["side"] == "long" else "Buy"
        res = await self.chase(symbol, close_side, pos["qty"], filters["tick"], "exit")
        if not res.get("filled"):
            self.say(f"{symbol}: закрытие ({reason}) не удалось через chase", "warn")
            return
        entry = pos["entry"]
        exit_p = res["price"]
        pnl = ((exit_p - entry) / entry * 100) if pos["side"] == "long" else ((entry - exit_p) / entry * 100)
        self.say(f"ВЫХОД {symbol} ({reason}) @ {exit_p} PnL~{pnl:.2f}%", "ok" if pnl >= 0 else "err")
        del self.positions[symbol]

    async def manage_positions(self):
        now = time.time()
        for symbol, pos in list(self.positions.items()):
            state = self.sym_states.get(symbol)
            if not state:
                continue
            price = state.mid_price()
            if not price:
                continue
            pnl = ((price - pos["entry"]) / pos["entry"] * 100) if pos["side"] == "long" else ((pos["entry"] - price) / pos["entry"] * 100)
            held = (now - pos["opened"]) / 60
            sc = state.score(self.cfg["ob_depth"], self.cfg["ob_weight"])
            against = (pos["side"] == "long" and sc < -self.cfg["exit_threshold"]) or (pos["side"] == "short" and sc > self.cfg["exit_threshold"])
            if pnl >= self.cfg["tp_pct"]:
                await self.close_position(symbol, "take-profit")
            elif pnl <= -self.cfg["sl_pct"]:
                await self.close_position(symbol, "stop-loss")
            elif held >= self.cfg["max_hold_min"]:
                await self.close_position(symbol, "время")
            elif against:
                await self.close_position(symbol, "разворот сигнала")

    async def scan(self):
        free = self.cfg["max_trades"] - len(self.positions)
        if free <= 0:
            return
        cands = []
        pb_ms = self.cfg["pullback_window_sec"] * 1000
        for symbol, state in self.sym_states.items():
            if symbol in self.positions:
                continue
            sc = state.score(self.cfg["ob_depth"], self.cfg["ob_weight"])
            if self.cfg["short_only"] and sc > 0:
                continue
            if abs(sc) < self.cfg["entry_threshold"]:
                continue
            if sc < 0 and self.cfg["require_pullback_for_short"]:
                if not state.has_pulled_back_up(pb_ms, self.cfg["pullback_pct"]):
                    continue
            cands.append((symbol, sc))
        cands.sort(key=lambda x: abs(x[1]), reverse=True)
        for symbol, sc in cands[:free]:
            side = "long" if sc > 0 else "short"
            await self.open_position(symbol, side, sc)

    async def ws_loop(self, symbols):
        url = "wss://stream.bybit.com/v5/public/linear" if self.cfg["env"] == "mainnet" else "wss://stream-testnet.bybit.com/v5/public/linear"
        topics = []
        for s in symbols:
            topics += [f"orderbook.50.{s}", f"publicTrade.{s}"]
        while self.running:
            try:
                self.ws_status = "connecting"
                async with websockets.connect(url, ping_interval=20) as ws:
                    self.ws_status = "connected"
                    self.say("WebSocket подключён")
                    for i in range(0, len(topics), 10):
                        await ws.send(json.dumps({"op": "subscribe", "args": topics[i:i+10]}))
                    while self.running:
                        raw = await asyncio.wait_for(ws.recv(), timeout=30)
                        msg = json.loads(raw)
                        if msg.get("topic"):
                            parts = msg["topic"].split(".")
                            symbol = parts[-1]
                            st = self.sym_states.get(symbol)
                            if not st:
                                continue
                            if msg["topic"].startswith("orderbook."):
                                st.apply_orderbook(msg.get("data", {}), msg.get("type") == "snapshot")
                            elif msg["topic"].startswith("publicTrade."):
                                st.apply_trades(msg.get("data", []), self.cfg["tape_window_sec"]*1000, self.cfg["pullback_window_sec"]*1000)
            except Exception as e:
                self.ws_status = "reconnecting"
                self.say(f"WS ошибка: {e}, переподключение...", "warn")
                await asyncio.sleep(5)

    async def main_loop(self):
        self.rest.sync_time()
        symbols = await self.resolve_universe()
        if not symbols:
            self.say("Список монет пуст", "err")
            return
        self.say(f"Сканирую: {', '.join(symbols)}")
        for s in symbols:
            self.sym_states[s] = SymbolState(s)
        try:
            self.initial_equity = self.rest.get_equity()
            avail = self.rest.get_available()
            self.say(f"Эквити: {self.initial_equity:.2f} USDT, доступно: {avail:.2f}")
        except Exception as e:
            self.say(f"Ошибка баланса: {e}", "err")
            return
        self.running = True
        ws_task = asyncio.create_task(self.ws_loop(symbols))
        try:
            while self.running:
                try:
                    await self.manage_positions()
                    await self.scan()
                except Exception as e:
                    self.say(f"Ошибка цикла: {e}", "err")
                await asyncio.sleep(3)
        finally:
            self.running = False
            ws_task.cancel()
            for s in list(self.positions):
                await self.close_position(s, "остановка")

ENGINE = Engine(CFG)

# ============ простой веб-интерфейс ============
PAGE = """<!doctype html>
<html lang="ru"><head><meta charset="utf-8"><title>Order Flow Bot</title>
<style>
body{font-family:system-ui;background:#0b0e12;color:#e7eaee;margin:0;padding:20px}
.panel{background:#12161c;border:1px solid #242a33;border-radius:10px;padding:16px;margin-bottom:16px}
h1{margin:0 0 8px} .status{font-family:monospace}
table{width:100%;border-collapse:collapse;font-size:13px}
th,td{padding:6px 8px;border-bottom:1px solid #242a33;text-align:left}
.log{font-family:monospace;font-size:12px;max-height:300px;overflow:auto;background:#0d1015;padding:10px;border-radius:6px}
button{padding:10px 16px;border:none;border-radius:6px;cursor:pointer;font-weight:600}
#start{background:#3b82f6;color:#fff} #stop{background:transparent;color:#f0454f;border:1px solid #f0454f}
</style></head><body>
<h1>Bybit Order Flow Bot <span class="status" id="st">—</span></h1>
<div class="panel">
<button id="start">СТАРТ</button> <button id="stop">СТОП</button>
<div id="info" style="margin-top:10px;font-size:13px;color:#8b94a3"></div>
</div>
<div class="panel"><h3>Позиции</h3><table id="pos"><thead><tr><th>Символ</th><th>Сторона</th><th>Вход</th><th>Время</th></tr></thead><tbody></tbody></table></div>
<div class="panel"><h3>Лог</h3><div class="log" id="log"></div></div>
<script>
async function poll(){
  const s = await (await fetch('/api/state')).json();
  document.getElementById('st').textContent = s.running ? 'работает' : 'остановлен';
  document.getElementById('info').textContent = `Символов: ${s.symbols.length} · WS: ${s.ws} · Эквити: ${s.equity||'—'}`;
  const tb = document.querySelector('#pos tbody');
  tb.innerHTML = s.positions.map(p=>`<tr><td>${p.symbol}</td><td>${p.side}</td><td>${p.entry}</td><td>${((Date.now()/1000-p.opened)/60).toFixed(1)} мин</td></tr>`).join('') || '<tr><td colspan=4>нет</td></tr>';
  document.getElementById('log').innerHTML = s.logs.map(l=>`<div>${l}</div>`).join('');
}
document.getElementById('start').onclick = ()=>fetch('/api/start',{method:'POST'});
document.getElementById('stop').onclick = ()=>fetch('/api/stop',{method:'POST'});
setInterval(poll, 2000); poll();
</script></body></html>"""

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def _send(self, code, body, ctype="application/json"):
        data = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)
    def do_GET(self):
        if self.path == "/":
            self._send(200, PAGE.encode(), "text/html; charset=utf-8")
        elif self.path == "/api/state":
            self._send(200, ENGINE.state())
        else:
            self._send(404, {"error": "not found"})
    def do_POST(self):
        if self.path == "/api/start" and not ENGINE.running:
            threading.Thread(target=lambda: asyncio.run(ENGINE.main_loop()), daemon=True).start()
            self._send(200, {"ok": True})
        elif self.path == "/api/stop":
            ENGINE.running = False
            self._send(200, {"ok": True})
        else:
            self._send(404, {"error": "not found"})

def main():
    if not CFG["api_key"] or not CFG["api_secret"]:
        log.error("Задайте BYBIT_API_KEY и BYBIT_API_SECRET")
        return
    srv = ThreadingHTTPServer(("0.0.0.0", CFG["port"]), Handler)
    log.info(f"Бот запущен на порту {CFG['port']} (env={CFG['env']})")
    srv.serve_forever()

if __name__ == "__main__":
    main()