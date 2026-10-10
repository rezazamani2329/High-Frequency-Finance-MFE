"""
RIT ALGO2e - regime-adaptive trading bot (v2, built from the live tape)
=======================================================================

Why this design
---------------
The first live run of ALGO2e showed that plain two-sided market making loses:
at a 1c spread with 10-30k shares queued, resting orders are filled mostly
just before the price runs through them.  The full trade tape of that run
(10.7k CNR / 31k RY / 19.8k AC trades) was replayed tick by tick, and every
stock turned out to have its own, very stable personality:

    CNR  moves CONTINUE  (1-tick autocorrelation +0.34, both halves)
    AC   moves continue  (+0.16..+0.27), more mixed
    RY   moves REVERSE   (-0.44, both halves; it chops ~10c a tick and
                          goes nowhere)

So each stock gets the strategy that won in the replay - scored on its WORSE
half of the run and with extra slippage, so it is not a lucky fit:

  * MEAN REVERSION (MR), passive  - fade moves >= th cents away from a
    10-tick EMA with a resting order at the touch, exit back at the EMA.
    Replay, RY at 10k shares: +$38-41k, max drawdown < $2k.
  * MOMENTUM (MOM), taker         - when the move over the last L ticks is
    >= th cents, take liquidity in its direction (marketable limit, <=1c
    through the touch); hold until the signal flips or a trailing stop
    (give-back from the best price) fires, then sit out one tick.
    Replay, CNR at 9-10k: +$20-23k; AC at 5k: +$6.5-9.6k.

Which regime each stock is in is MEASURED LIVE (rolling 30-tick
autocorrelation of tick-to-tick price changes): > +0.10 -> MOM, < -0.10 ->
MR, in between -> the prior from the replay.  So if the next run's stocks
behave differently, the bot follows them.

Risk, measured not timid:
  * per-stock position sizes (defaults RY 10,000 / CNR 9,000 / AC 5,000);
  * worst-case shared gross/net check before every order (25,000 limit,
    $1/share fines): every resting order is assumed to fill;
  * one order in flight per stock, and the next action on that stock waits
    until RIT shows the result (RIT's lists lag - this is what produced
    double orders before);
  * trailing stop on momentum positions; MR positions are flattened when
    the price returns to the mean.

Run ``python rit_algo2e.py --help``; see README_ALGO2e.md.
"""

from __future__ import annotations

import argparse
import collections
import csv
import math
import os
import signal
import statistics
import sys
import time
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter

BUY, SELL = "BUY", "SELL"


# ----------------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------------

# Per-stock strategy defaults, from the replay of the live tape.
# size: shares at the reference volatility `sigma` (per-tick sd, cents, from the
# replayed run).  trust: whether the prior mode may be used before the stock's
# behaviour has been measured.  Three live runs showed only RY keeps its
# character (mean-reverting every time); CNR and AC flipped between trending
# and reverting and between 1.5c and 32c per-tick volatility, so they wait to
# be measured.
PRIORS = {
    "RY":  dict(mode="MR",  size=10000, sigma=5.2, trust=True),
    "CNR": dict(mode="MOM", size=9000,  sigma=4.2, trust=False),
    "AC":  dict(mode="MOM", size=5000,  sigma=4.8, trust=False),
}
# Thresholds in units of the stock's CURRENT per-tick sd (the cent values are
# the replay's settings at the reference sigma, used until sd is known).
MOM_PARAMS = {          # L ticks, entry th_k x sd, trailing stop trail_k x sd
    "CNR": dict(L=3, th=2, trail=3, th_k=0.5, trail_k=0.7),
    "AC":  dict(L=2, th=2, trail=10, th_k=0.4, trail_k=2.0),
    "_":   dict(L=2, th=2, trail=6, th_k=0.5, trail_k=1.5),
}
MR_PARAMS = {           # EMA span ticks, entry cents, exit band cents,
                        # stop: max(stop cents, stop_k x per-tick sd) adverse move,
                        # trend guard: |10-tick move| >= trend_k x sd of the stock's own
                        #   recent 10-tick moves (measured directly: for a stock that
                        #   snaps back, sqrt-time scaling overstates them ~2x and the
                        #   guard never fired in replay),
                        # pause: ticks to sit out after either fires
    "RY": dict(span=10, th=2, exit=2, th_k=0.4, stop=5, stop_k=2.5, trend_k=2.5, trend_win=10, pause=10),
    "_":  dict(span=10, th=2, exit=2, th_k=0.4, stop=5, stop_k=2.5, trend_k=2.5, trend_win=10, pause=10),
}


@dataclass
class Config:
    host: str = "localhost"
    port: int = 49120
    api_key: str = "MRQX7PCH"
    http_timeout: float = 2.0
    tickers: Tuple[str, ...] = ()

    size_override: Dict[str, int] = field(default_factory=dict)
    mode_override: Dict[str, str] = field(default_factory=dict)   # MOM / MR / OFF
    adaptive: bool = True             # pick MOM/MR per stock from live data
    regime_window: int = 30
    regime_min: int = 15
    regime_band: float = 0.15         # |autocorr| needed to pick MOM / MR
    regime_keep: float = 0.05         # ... and to stay in it once chosen (hysteresis)
    vol_scale: bool = True            # thresholds and size scale with live volatility
    size_mult: float = 1.0

    max_order: int = 5000
    take_slip_ticks: int = 1          # how far through the touch a take may go
    cooldown_ticks: float = 1.0       # sit out after a trailing stop

    gross_limit: int = 25000
    net_limit: int = 25000
    limit_buffer: int = 500

    flatten_end: bool = False         # flatten in the final ticks (off: marked to market)
    mr_stop: bool = True              # stop-loss on mean-reversion positions
    mr_trend_guard: bool = True       # stop fading a stock that is trending hard
    flatten_ticks: int = 2

    max_reads: float = 180.0
    max_writes: float = 80.0
    poll_interval: float = 0.08
    case_poll_interval: float = 0.25
    inflight_timeout: float = 1.5

    status_every: float = 5.0
    log_path: Optional[str] = "algo2e_v2_log.csv"
    verbose: bool = True
    keep_running: bool = False

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}/v1"


# ----------------------------------------------------------------------------
# REST client
# ----------------------------------------------------------------------------

class RitError(RuntimeError):
    pass


class RateLimited(RitError):
    def __init__(self, wait: float):
        super().__init__(f"rate limited, wait {wait:.3f}s")
        self.wait = wait


class Rejected(RitError):
    def __init__(self, status: int, text: str):
        super().__init__(f"HTTP {status}: {text}")
        self.status = status


def _num(d, key, default=0.0):
    v = d.get(key) if isinstance(d, dict) else None
    return default if v is None else v


class Window:
    """Never more than `rate` events in any `window` seconds."""

    def __init__(self, rate: float, window: float = 1.0):
        self.rate = max(int(rate), 1)
        self.window = window
        self.stamps: Deque[float] = collections.deque()
        self.blocked_until = 0.0

    def remaining(self) -> int:
        now = time.monotonic()
        if now < self.blocked_until:
            return 0
        while self.stamps and self.stamps[0] <= now - self.window:
            self.stamps.popleft()
        return max(self.rate - len(self.stamps), 0)

    def take(self) -> bool:
        if self.remaining() <= 0:
            return False
        self.stamps.append(time.monotonic())
        return True

    def wait(self) -> None:
        while not self.take():
            time.sleep(0.003)

    def block(self, seconds: float) -> None:
        self.blocked_until = max(self.blocked_until,
                                 time.monotonic() + min(max(seconds, 0.05), 2.0))


class RitClient:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.s = requests.Session()
        self.s.headers.update({"X-API-Key": cfg.api_key})
        self.s.mount("http://", HTTPAdapter(pool_connections=4, pool_maxsize=4, max_retries=0))
        self.reads = Window(cfg.max_reads, 1.0)
        self.calls = 0

    def _req(self, method: str, path: str, params=None, retries: int = 1):
        url = self.cfg.base_url + path
        last = None
        for attempt in range(retries + 1):
            if method == "GET":
                self.reads.wait()
            self.calls += 1
            try:
                r = self.s.request(method, url, params=params, timeout=self.cfg.http_timeout)
            except requests.RequestException as exc:
                last = exc
                time.sleep(0.02 * (attempt + 1))
                continue
            if r.status_code == 200:
                try:
                    return r.json()
                except ValueError as exc:
                    raise RitError(f"{path}: malformed JSON") from exc
            if r.status_code == 429:
                wait = 0.2
                try:
                    wait = float(r.headers.get("Retry-After") or r.json().get("wait", 0.2))
                except Exception:
                    pass
                raise RateLimited(wait)
            if r.status_code == 401:
                raise RitError("401 Unauthorized - wrong API key or RIT client not logged in "
                               "(click the API icon in RIT for port/key)")
            if 400 <= r.status_code < 500:
                raise Rejected(r.status_code, r.text[:200])
            last = RitError(f"{method} {path} -> {r.status_code}")
            time.sleep(0.02 * (attempt + 1))
        raise RitError(f"{method} {path} failed: {last}")

    def case(self):
        return self._req("GET", "/case")

    def trader(self):
        return self._req("GET", "/trader")

    def limits(self):
        return self._req("GET", "/limits")

    def securities(self):
        return self._req("GET", "/securities")

    def book(self, t, limit=40):
        return self._req("GET", "/securities/book", {"ticker": t, "limit": limit})

    def history(self, t, limit=60):
        return self._req("GET", "/securities/history", {"ticker": t, "limit": limit})

    def orders(self, status="OPEN"):
        return self._req("GET", "/orders", {"status": status})

    def limit_order(self, t, action, qty, price):
        return self._req("POST", "/orders", {"ticker": t, "type": "LIMIT", "quantity": qty,
                                              "action": action, "price": price}, retries=0)

    def cancel(self, oid):
        return self._req("DELETE", f"/orders/{oid}", retries=0)

    def cancel_all(self):
        return self._req("POST", "/commands/cancel", {"all": 1}, retries=0)


# ----------------------------------------------------------------------------
# State
# ----------------------------------------------------------------------------

@dataclass
class MyOrder:
    oid: int
    action: str
    price: int
    remaining: int


@dataclass
class Stock:
    t: str
    decimals: int = 2
    fee: float = 0.0
    rebate: float = 0.0
    max_order: int = 5000
    tradeable: bool = True
    min_price_t: Optional[int] = None
    max_price_t: Optional[int] = None
    closes: "collections.OrderedDict[int, float]" = field(default_factory=collections.OrderedDict)
    ema: Optional[float] = None        # EMA of completed ticks' closes
    ema_tick: int = -1
    ema_live: Optional[float] = None
    # live state
    pos: int = 0
    bid: Optional[int] = None          # full-book best bid (incl. ours)
    ask: Optional[int] = None
    cbid: Optional[int] = None         # competitor best (excl. ours)
    cask: Optional[int] = None
    mine: List[MyOrder] = field(default_factory=list)
    target: int = 0
    mode: str = "OFF"
    rho: Optional[float] = None
    entry: Optional[float] = None      # price (ticks) we got into the position
    best: Optional[float] = None       # best price since entry, in our favour
    cool_until_tick: float = -1
    mr_pause_until: float = -1
    force_take: bool = False           # exit by crossing (stop / trend guard)
    inflight_until: float = 0.0
    inflight_pos: Optional[int] = None

    def mid(self) -> Optional[float]:
        b, a = self.cbid, self.cask
        if b is None and a is None:
            return None
        if b is None:
            return float(a)
        if a is None:
            return float(b)
        return (b + a) / 2.0


# ----------------------------------------------------------------------------
# Bot
# ----------------------------------------------------------------------------

class Bot:

    def __init__(self, cfg: Config, client: Optional[RitClient], paper: bool = False):
        self.cfg, self.client, self.paper = cfg, client, paper
        self.stop = False
        self.stocks: Dict[str, Stock] = {}
        self.order: List[str] = []
        self.trader_id = None
        self.writes = Window(cfg.max_writes, 1.05)
        self.recent_ids: Deque[int] = collections.deque(maxlen=3000)
        self.pending: Dict[int, Tuple[str, MyOrder, float]] = {}
        self.tick = 0
        self.tpp = 300
        self.gross_limit, self.net_limit = cfg.gross_limit, cfg.net_limit
        self.posts = self.cancels = self.rejects = self.errors = self.stops = 0
        self.max_gross = 0
        self.started = time.monotonic()
        self._last_status = 0.0
        self._last_limits = 0.0
        self._log = self._logf = None
        if cfg.log_path:
            try:
                new = not os.path.exists(cfg.log_path)
                self._logf = open(cfg.log_path, "a", newline="", encoding="utf-8")
                self._log = csv.writer(self._logf)
                if new:
                    self._log.writerow(["wall", "tick", "ticker", "mode", "rho", "pos", "target",
                                        "bid", "ask", "ema", "nlv"])
            except OSError as exc:
                print(f"WARNING: cannot write {cfg.log_path} ({exc}); no CSV log", flush=True)
                self._log = self._logf = None

    # -- helpers ----------------------------------------------------------------

    def say(self, msg):
        if self.cfg.verbose:
            print(f"[{time.monotonic() - self.started:7.2f}s t{self.tick:>3}] {msg}", flush=True)

    def ticks(self, s: Stock, price) -> int:
        return int(round(float(price) * 10 ** s.decimals))

    def px(self, s: Stock, t: int) -> str:
        return f"{t / 10 ** s.decimals:.{s.decimals}f}"

    def size_of(self, s: Stock) -> int:
        """Shares to hold.  In MOMENTUM mode, shrinks (never grows) when the
        stock is more volatile than in the replayed run, so the dollars at
        risk per tick from a trend reversal stay put.  In MEAN-REVERSION mode
        this does NOT apply: higher volatility there means a bigger reversion
        profit per round-trip, not bigger risk (risk is already capped by the
        MR stop-loss / trend guard) - shrinking size there only throws away
        the extra edge.  Confirmed by replay: un-scaled MR size beat scaled
        on a tape where a normally-trending stock was forced to mean-revert.
        """
        base = self.cfg.size_override.get(s.t, PRIORS.get(s.t, {}).get("size", 5000))
        if self.cfg.vol_scale and s.mode != "MR":
            v = self.vol_ticks(s)
            ref = PRIORS.get(s.t, {}).get("sigma", 5.0)
            if v and v > ref:
                base = base * ref / v
        return max(int(base * self.cfg.size_mult) // 100 * 100, 0)

    def scaled(self, s: Stock, cents: float, k: Optional[float], floor: float) -> float:
        """A threshold: k x current per-tick sd if known, else the cent value."""
        if self.cfg.vol_scale and k is not None:
            v = self.vol_ticks(s)
            if v:
                return max(floor, k * v)
        return cents

    @property
    def gross_cap(self):
        return max(self.gross_limit - self.cfg.limit_buffer, 0)

    @property
    def net_cap(self):
        return max(self.net_limit - self.cfg.limit_buffer, 0)

    # -- static data -------------------------------------------------------------

    def load_securities(self, secs):
        for r in secs or []:
            t = r.get("ticker")
            if not t:
                continue
            if self.cfg.tickers and t not in self.cfg.tickers:
                continue
            if not self.cfg.tickers and str(r.get("type", "STOCK")).upper() != "STOCK":
                continue
            s = self.stocks.get(t)
            if s is None:
                s = self.stocks[t] = Stock(t)
                self.order.append(t)
            s.decimals = int(_num(r, "quoted_decimals", 2))
            s.fee = float(_num(r, "trading_fee", 0.0))
            s.rebate = float(_num(r, "limit_order_rebate", 0.0))
            mts = int(_num(r, "max_trade_size", 0))
            s.max_order = min(mts, self.cfg.max_order) if mts > 0 else self.cfg.max_order
            s.tradeable = bool(r.get("is_tradeable", True))
            lo, hi = _num(r, "min_price", 0.0), _num(r, "max_price", 0.0)
            s.min_price_t = self.ticks(s, lo) if lo and lo > 0 else None
            s.max_price_t = self.ticks(s, hi) if hi and hi > 0 else None
            s.pos = int(round(float(_num(r, "position", 0.0))))

    def load_limits(self):
        try:
            g = n = None
            for lim in self.client.limits() or []:
                gl, nl = float(_num(lim, "gross_limit", 0)), float(_num(lim, "net_limit", 0))
                if gl > 0:
                    g = gl if g is None else min(g, gl)
                if nl > 0:
                    n = nl if n is None else min(n, nl)
            if g:
                self.gross_limit = int(g)
            if n:
                self.net_limit = int(n)
        except RitError as exc:
            self.say(f"limits refresh failed: {exc}")
        self._last_limits = time.monotonic()

    def seed_history(self):
        for t in self.order:
            s = self.stocks[t]
            try:
                rows = self.client.history(t, 60)
            except RitError:
                continue
            pts = sorted((int(r["tick"]), float(self.ticks(s, r["close"])))
                         for r in rows or [] if r.get("tick") is not None and r.get("close"))
            s.closes.clear()
            for tk, c in pts:
                if tk <= self.tick:
                    s.closes[tk] = c
            s.ema = None
            s.ema_tick = -1
            for tk, c in s.closes.items():
                self.update_ema(s, tk, c)
            if pts:
                self.say(f"{t}: loaded {len(s.closes)} ticks of history")

    # -- market data ---------------------------------------------------------------

    def snapshot(self) -> bool:
        try:
            orders = self.client.orders("OPEN")
            secs = self.client.securities()
            books = {t: self.client.book(t) for t in self.order}
        except RateLimited as exc:
            time.sleep(min(exc.wait, 1.0))
            return False
        except RitError as exc:
            self.errors += 1
            self.say(f"snapshot failed: {exc}")
            time.sleep(0.1)
            return False
        self.load_securities(secs)
        own = set(self.recent_ids)
        listed = set()
        mine = {t: [] for t in self.order}
        for o in orders or []:
            oid = o.get("order_id")
            if oid is not None:
                own.add(oid)
                listed.add(oid)
            t = o.get("ticker")
            if t not in mine or o.get("price") is None or o.get("action") not in (BUY, SELL):
                continue
            rem = int(_num(o, "quantity", 0)) - int(_num(o, "quantity_filled", 0))
            if rem > 0:
                mine[t].append(MyOrder(oid, o["action"], self.ticks(self.stocks[t], o["price"]), rem))
        now = time.monotonic()
        for oid, (t, mo, sent) in list(self.pending.items()):
            if oid in listed or now - sent > 2.0 or t not in mine:
                del self.pending[oid]
            else:
                mine[t].append(MyOrder(mo.oid, mo.action, mo.price, mo.remaining))
        for t in self.order:
            s = self.stocks[t]
            s.mine = mine[t]
            b = books.get(t) or {}
            s.bid = s.ask = s.cbid = s.cask = None
            for key, side in (("bids", BUY), ("asks", SELL)):
                for e in (b.get(key) or []):
                    if e.get("price") is None:
                        continue
                    if int(_num(e, "quantity", 0)) - int(_num(e, "quantity_filled", 0)) <= 0:
                        continue
                    p = self.ticks(s, e["price"])
                    ours = e.get("order_id") in own or (self.trader_id is not None
                                                         and e.get("trader_id") == self.trader_id)
                    if side == BUY:
                        s.bid = p if s.bid is None else max(s.bid, p)
                        if not ours:
                            s.cbid = p if s.cbid is None else max(s.cbid, p)
                    else:
                        s.ask = p if s.ask is None else min(s.ask, p)
                        if not ours:
                            s.cask = p if s.cask is None else min(s.cask, p)
            m = s.mid()
            if m is not None:
                if s.closes and next(reversed(s.closes)) > self.tick:
                    s.closes.clear()                    # new run
                    s.ema, s.ema_tick = None, -1
                s.closes[self.tick] = m
                while len(s.closes) > 400:
                    s.closes.popitem(last=False)
                self.update_ema(s, self.tick, m)
            if s.inflight_pos is not None and (s.pos != s.inflight_pos or now > s.inflight_until):
                s.inflight_pos = None
        g = sum(abs(self.stocks[t].pos) for t in self.order)
        self.max_gross = max(self.max_gross, g)
        return True

    def update_ema(self, s: Stock, tk: int, c: float):
        span = MR_PARAMS.get(s.t, MR_PARAMS["_"])["span"]
        if s.ema is None:
            s.ema, s.ema_tick = c, tk
            s.ema_live = c
            return
        if tk > s.ema_tick:
            # fold the previous tick's close in once per tick
            prev = s.closes.get(s.ema_tick, c)
            s.ema = s.ema + (prev - s.ema) * 2.0 / (span + 1)
            s.ema_tick = tk
        s.ema_live = s.ema + (c - s.ema) * 2.0 / (span + 1)

    def move_sd(self, s: Stock, w: int) -> Optional[float]:
        """Std-dev of the stock's own recent w-tick moves (last 60 completed ticks)."""
        vals = [v for k, v in s.closes.items() if k < self.tick][-61:]
        moves = [vals[i] - vals[i - w] for i in range(w, len(vals))]
        if len(moves) < 15:
            return None
        return statistics.pstdev(moves)

    def vol_ticks(self, s: Stock) -> Optional[float]:
        """Std-dev of tick-to-tick close changes (ticks), last 30 completed ticks."""
        vals = [v for k, v in s.closes.items() if k < self.tick][-31:]
        d = [b - a for a, b in zip(vals, vals[1:])]
        if len(d) < 10:
            return None
        return statistics.pstdev(d)

    # -- regime -------------------------------------------------------------------

    def regime(self, s: Stock) -> str:
        forced = self.cfg.mode_override.get(s.t)
        if forced:
            return forced
        pr = PRIORS.get(s.t, {})
        prior = pr.get("mode", "OFF")
        if not self.cfg.adaptive:
            return prior
        # Always default to the prior mode (never OFF) while unmeasured or
        # ambiguous - a strong, measured signal can still override it below.
        # An earlier "untrusted stocks sit out until measured" version lost
        # the first regime_min ticks of every run plus every ambiguous tick
        # for CNR/AC, which cost ~$10-15k of real trading time on the actual
        # recorded run (confirmed by replay) for a benefit that only showed
        # up on an adversarial synthetic tape - and that adversarial case is
        # still caught fine by the rho thresholds below, since they measure
        # well past the band required to flip mode.
        unmeasured = prior
        ks = list(s.closes.keys())
        vals = [s.closes[k] for k in ks if k < self.tick][-(self.cfg.regime_window + 1):]
        d = [b - a for a, b in zip(vals, vals[1:])]
        if len(d) < self.cfg.regime_min:
            s.rho = None
            return unmeasured
        m = statistics.mean(d)
        den = sum((x - m) ** 2 for x in d)
        if den <= 0:
            s.rho = 0.0
            return unmeasured
        s.rho = sum((d[i] - m) * (d[i + 1] - m) for i in range(len(d) - 1)) / den
        # hysteresis: keep the current style while it is still on its side
        if s.mode == "MOM" and s.rho > self.cfg.regime_keep:
            return "MOM"
        if s.mode == "MR" and s.rho < -self.cfg.regime_keep:
            return "MR"
        if s.rho > self.cfg.regime_band:
            return "MOM"
        if s.rho < -self.cfg.regime_band:
            return "MR"
        # no clear behaviour: a trusted prior keeps trading, others sit out
        return prior if pr.get("trust") else "OFF"

    # -- targets ------------------------------------------------------------------

    def decide(self, s: Stock) -> None:
        s.mode = self.regime(s)
        N = self.size_of(s)
        now = s.mid()
        if now is None or not s.tradeable or s.mode == "OFF" or N <= 0:
            s.target = 0 if s.mode == "OFF" else s.target
            return
        if self.tick < s.cool_until_tick:
            s.target = 0
            return
        if s.mode == "MOM":
            p = MOM_PARAMS.get(s.t, MOM_PARAMS["_"])
            th = self.scaled(s, p["th"], p.get("th_k"), 1.0)
            trail = self.scaled(s, p["trail"], p.get("trail_k"), 2.0)
            ref = s.closes.get(self.tick - p["L"])
            if ref is not None:
                move = now - ref
                if move >= th:
                    s.target = N
                elif move <= -th:
                    s.target = -N
                elif abs(move) < 0.5:
                    s.target = 0                 # the move stalled: step aside
            # trailing stop: give-back from the best price since entry
            if s.pos != 0 and s.entry is not None:
                fav = now if s.pos > 0 else -now
                s.best = fav if s.best is None else max(s.best, fav)
                if s.best - fav >= trail:
                    self.stops += 1
                    self.say(f"{s.t}: trailing stop ({trail:.1f}c give-back) - flattening")
                    s.target = 0
                    s.cool_until_tick = self.tick + self.cfg.cooldown_ticks
        elif s.mode == "MR":
            p = MR_PARAMS.get(s.t, MR_PARAMS["_"])
            if self.tick < s.mr_pause_until:
                s.target = 0
                s.force_take = s.pos != 0
                return
            v = self.vol_ticks(s) or 0.0
            # Trend guard: a mean-reverting stock that starts trending hard is
            # no longer one - stop fading it, get out at market, sit out.
            sdw = self.move_sd(s, p["trend_win"])
            if sdw is None and v > 0:
                sdw = v * math.sqrt(p["trend_win"])          # early in the run: rough bound
            if self.cfg.mr_trend_guard and sdw:
                ref = s.closes.get(self.tick - p["trend_win"])
                if ref is not None:
                    mv = now - ref
                    if abs(mv) >= max(p["trend_k"] * sdw, 3 * p["th"]):
                        if s.pos != 0 or s.target != 0:
                            self.say(f"{s.t}: trend guard ({mv:+.1f}c over {p['trend_win']} ticks) "
                                     f"- not fading it, flattening")
                        self.stops += 1 if s.pos != 0 else 0
                        s.target = 0
                        s.force_take = s.pos != 0
                        s.mr_pause_until = self.tick + p["pause"]
                        return
            # Stop-loss: the fade has gone the wrong way by more than normal noise.
            if self.cfg.mr_stop and s.pos != 0 and s.entry is not None:
                adverse = (s.entry - now) if s.pos > 0 else (now - s.entry)
                stop = max(p["stop"], p["stop_k"] * v)
                if adverse >= stop:
                    self.stops += 1
                    self.say(f"{s.t}: mean-reversion stop ({adverse:.1f}c against, limit "
                             f"{stop:.1f}c) - flattening")
                    s.target = 0
                    s.force_take = True
                    s.mr_pause_until = self.tick + p["pause"]
                    return
            ema = s.ema                          # completed ticks only (as replayed)
            th = self.scaled(s, p["th"], p.get("th_k"), 1.0)
            ex = th if p["exit"] >= p["th"] else self.scaled(s, p["exit"], p.get("th_k"), 1.0)
            if ema is not None and len(s.closes) >= p["span"] // 2:
                dev = now - ema
                if dev >= th:
                    s.target = -N
                elif dev <= -th:
                    s.target = N
                elif abs(dev) <= ex:
                    s.target = 0
        if self.cfg.flatten_end and self.tpp - self.tick <= self.cfg.flatten_ticks:
            s.target = 0

    # -- risk ---------------------------------------------------------------------

    def worst(self, extra=None) -> Tuple[int, int]:
        g = hi = lo = 0
        for t in self.order:
            s = self.stocks[t]
            b = sum(o.remaining for o in s.mine if o.action == BUY)
            a = sum(o.remaining for o in s.mine if o.action == SELL)
            if extra and extra[0] == t:
                if extra[1] == BUY:
                    b += extra[2]
                else:
                    a += extra[2]
            g += max(abs(s.pos + b), abs(s.pos - a))
            hi += s.pos + b
            lo += s.pos - a
        return g, max(abs(hi), abs(lo))

    def fit(self, t, side, q) -> int:
        g0, n0 = self.worst()

        def ok(x):
            g, n = self.worst((t, side, x))
            return (g <= self.gross_cap or g <= g0) and (n <= self.net_cap or n <= n0)

        if ok(q):
            return q
        lo, hi = 0, q
        while hi - lo > 1:
            mid = (lo + hi) // 2
            if ok(mid):
                lo = mid
            else:
                hi = mid
        return lo

    def trim_to_caps(self) -> None:
        """Invariant: if every resting order filled, gross and net stay under
        the caps.  An order that was exempt when placed (it reduced a
        position) can become exposure-ADDING after other fills, so this is
        re-checked every loop and adding orders are pulled, largest first."""
        g, n = self.worst()
        if g <= self.gross_cap and n <= self.net_cap:
            return
        adders = []
        for t in self.order:
            s = self.stocks[t]
            for o in s.mine:
                if (o.action == BUY and s.pos >= 0) or (o.action == SELL and s.pos <= 0):
                    adders.append((s, o))
        for s, o in sorted(adders, key=lambda x: -x[1].remaining):
            if g <= self.gross_cap and n <= self.net_cap:
                break
            if self.cancel(s, o):
                g, n = self.worst()

    # -- orders -------------------------------------------------------------------

    def cancel(self, s: Stock, o: MyOrder) -> bool:
        if not self.writes.take():
            return False
        try:
            self.client.cancel(o.oid)
            self.cancels += 1
        except RateLimited as exc:
            self.writes.block(exc.wait)
            return False
        except Rejected:
            # "not open": it may have FILLED a moment ago.  Until RIT shows the
            # true position, place nothing more on this stock - re-placing on
            # the assumption that it did not fill is how a stock overshoots
            # its size (seen in replay: RY at 15,000 vs a 10,000 target).
            s.inflight_pos = s.pos
            s.inflight_until = time.monotonic() + self.cfg.inflight_timeout
        except RitError as exc:
            self.errors += 1
            self.say(f"cancel failed: {exc}")
            return False
        self.pending.pop(o.oid, None)
        s.mine = [x for x in s.mine if x.oid != o.oid]
        return True

    def send(self, s: Stock, side: str, qty: int, price: int, taking: bool) -> Optional[dict]:
        if qty <= 0 or not self.writes.take():
            return None
        try:
            o = self.client.limit_order(s.t, side, qty, self.px(s, price))
        except RateLimited as exc:
            self.writes.block(exc.wait)
            return None
        except Rejected as exc:
            self.rejects += 1
            self.say(f"{s.t} order rejected ({side} {qty} @ {self.px(s, price)}): {exc}")
            return None
        except RitError as exc:
            self.errors += 1
            self.say(f"{s.t} order failed ({side} {qty}): {exc}")
            s.inflight_until = time.monotonic() + self.cfg.inflight_timeout
            s.inflight_pos = s.pos
            return None
        self.posts += 1
        oid = o.get("order_id")
        if oid is not None:
            self.recent_ids.append(oid)
        filled = int(_num(o, "quantity_filled", 0))
        rem = int(_num(o, "quantity", qty)) - filled
        if o.get("status", "OPEN") == "OPEN" and rem > 0 and oid is not None:
            if taking:
                # a take must not rest: pull the unfilled remainder
                self.cancel(s, MyOrder(oid, side, price, rem))
            else:
                mo = MyOrder(oid, side, price, rem)
                self.pending[oid] = (s.t, mo, time.monotonic())
                s.mine.append(mo)
        if filled > 0:
            s.inflight_pos = s.pos
            s.inflight_until = time.monotonic() + self.cfg.inflight_timeout
            prev = s.pos
            s.pos += filled if side == BUY else -filled
            if prev == 0 or (prev > 0) != (s.pos > 0):
                s.entry = float(price)
                s.best = None
        return o

    def act(self, s: Stock) -> None:
        """Move the position toward the target in the stock's style."""
        if self.paper:
            return
        if s.inflight_pos is not None:
            return                       # last action not visible yet
        if s.pos == 0:
            s.entry, s.best = None, None
            s.force_take = False
        elif s.entry is None:
            s.entry = s.mid()
        need = s.target - s.pos
        style = "PASSIVE" if (s.mode == "MR" and not s.force_take) else "TAKE"
        if need == 0:
            for o in list(s.mine):
                self.cancel(s, o)
            return
        side = BUY if need > 0 else SELL
        reducing = (s.pos > 0 and side == SELL) or (s.pos < 0 and side == BUY)
        # flips go through flat: first close, then open on the next pass
        qty = min(abs(need), abs(s.pos) if reducing and abs(need) > abs(s.pos) else abs(need),
                  s.max_order)
        if style == "TAKE":
            for o in list(s.mine):
                self.cancel(s, o)
            if s.mine or s.inflight_pos is not None:
                return                               # wait until cancels land / fills show
            if side == BUY:
                if s.cask is None:
                    return
                price = s.cask + self.cfg.take_slip_ticks
            else:
                if s.cbid is None:
                    return
                price = s.cbid - self.cfg.take_slip_ticks
            if not reducing:
                qty = self.fit(s.t, side, qty)
            qty = (qty // 100) * 100 if qty >= 100 else qty
            if qty <= 0:
                return
            self.send(s, side, qty, price, taking=True)
            return
        # PASSIVE: one resting order at the touch on the needed side
        touch = s.bid if side == BUY else s.ask
        if touch is None:
            return
        # never cross the competitor side
        if side == BUY and s.cask is not None:
            touch = min(touch, s.cask - 1)
        if side == SELL and s.cbid is not None:
            touch = max(touch, s.cbid + 1)
        keep = None
        for o in list(s.mine):
            if o.action == side and o.price == touch and keep is None and o.remaining <= qty:
                keep = o
            else:
                self.cancel(s, o)
        if s.inflight_pos is not None:
            return                                   # a cancelled order may have filled
        if keep is not None and not reducing:
            # re-check the kept order as if it were new
            s.mine = [x for x in s.mine if x.oid != keep.oid]
            ok_qty = self.fit(s.t, side, keep.remaining)
            s.mine.append(keep)
            if ok_qty < keep.remaining:
                self.cancel(s, keep)
                keep = None
        have = keep.remaining if keep else 0
        add = qty - have
        if add < 100:
            return
        if not reducing:
            add = self.fit(s.t, side, add)
        add = (add // 100) * 100
        if add <= 0:
            return
        self.send(s, side, add, touch, taking=False)

    # -- loop -------------------------------------------------------------------

    def step(self):
        if not self.snapshot():
            return
        g = sum(abs(self.stocks[t].pos) for t in self.order)
        n = abs(sum(self.stocks[t].pos for t in self.order))
        for t in self.order:
            self.decide(self.stocks[t])
        if g > self.gross_limit or n > self.net_limit:
            # over the limit: shrink the largest positions' targets toward flat
            for t in sorted(self.order, key=lambda x: -abs(self.stocks[x].pos)):
                self.stocks[t].target = 0
                break
        if not self.paper:
            self.trim_to_caps()
        # exits / reducing first, then entries
        for t in sorted(self.order, key=lambda x: abs(self.stocks[x].target) >= abs(self.stocks[x].pos)):
            try:
                self.act(self.stocks[t])
            except RitError as exc:
                self.errors += 1
                self.say(f"{t}: {exc}")

    def status(self, force=False):
        now = time.monotonic()
        if not force and now - self._last_status < self.cfg.status_every:
            return
        self._last_status = now
        nlv = None
        try:
            nlv = self.client.trader().get("nlv")
        except RitError:
            pass
        parts = []
        for t in self.order:
            s = self.stocks[t]
            rho = "-" if s.rho is None else f"{s.rho:+.2f}"
            parts.append(f"{t} {s.mode}({rho}) pos {s.pos:+,}/{s.target:+,} "
                         f"{self.px(s, s.cbid) if s.cbid else '-'}/{self.px(s, s.cask) if s.cask else '-'}")
            if self._log:
                try:
                    self._log.writerow([f"{time.time():.2f}", self.tick, t, s.mode, rho, s.pos,
                                        s.target, s.cbid, s.cask,
                                        None if s.ema is None else round(s.ema, 2), nlv])
                except OSError:
                    self._log = None
        if self._logf and self._log:
            try:
                self._logf.flush()
            except OSError:
                self._log = None
        g = sum(abs(self.stocks[t].pos) for t in self.order)
        nl = "-" if nlv is None else f"{nlv:,.0f}"
        self.say(f"NLV {nl}  gross {g:,} | " + " | ".join(parts))

    def connect(self) -> bool:
        while not self.stop:
            try:
                self.trader_id = self.client.trader().get("trader_id")
                self.load_securities(self.client.securities())
                if not self.order:
                    raise RitError("no stocks found in this case")
                self.load_limits()
                return True
            except RitError as exc:
                self.say(f"waiting for RIT client: {exc}")
                time.sleep(2.0)
        return False

    def run(self):
        if not self.connect():
            return
        print("-" * 100)
        print(f"trader {self.trader_id}  limit gross {self.gross_limit:,}/net {self.net_limit:,} "
              f"(cap {self.gross_cap:,})  mode {'PAPER' if self.paper else 'LIVE'}  "
              f"adaptive regimes {'on' if self.cfg.adaptive else 'off'}")
        for t in self.order:
            s = self.stocks[t]
            pr = PRIORS.get(t, {})
            print(f"  {t:<5} prior {self.cfg.mode_override.get(t, pr.get('mode', 'OFF')):<4} size "
                  f"{self.size_of(s):,}  fee {s.fee:+.4f} rebate {s.rebate:+.4f}")
        print("-" * 100)
        was_active, last_status, case, cstamp, note = False, None, {}, 0.0, 0.0
        while not self.stop:
            now = time.monotonic()
            if now - cstamp >= self.cfg.case_poll_interval:
                try:
                    case = self.client.case()
                    cstamp = now
                except RitError as exc:
                    self.say(f"case poll failed: {exc}")
                    time.sleep(0.5)
                    continue
            status = case.get("status")
            self.tick = int(_num(case, "tick", 0))
            self.tpp = int(_num(case, "ticks_per_period", 300)) or 300
            if status != last_status:
                self.say(f"case status -> {status}")
                last_status = status
            if status != "ACTIVE":
                if status == "STOPPED" and was_active:
                    if not self.cfg.keep_running:
                        break
                    self.say("case over - waiting for the next run")
                    self.status(force=True)
                    was_active = False
                    for s in self.stocks.values():
                        s.closes.clear()
                        s.ema, s.ema_tick, s.target = None, -1, 0
                        s.cool_until_tick = -1
                        s.mr_pause_until = -1
                        s.force_take = False
                if now - note > 10:
                    self.say("waiting for the case to start (Ctrl-C to quit) ...")
                    note = now
                time.sleep(0.25)
                continue
            if not was_active:
                was_active = True
                self.load_limits()
                self.seed_history()
                self.say("case ACTIVE - trading")
            if now - self._last_limits > 5:
                self.load_limits()
            try:
                self.step()
                self.status()
            except Exception as exc:                        # noqa: BLE001
                self.errors += 1
                self.say(f"unexpected error (continuing): {exc!r}")
                time.sleep(0.2)
            time.sleep(self.cfg.poll_interval)
        self.shutdown()

    def shutdown(self):
        self.say("shutting down - cancelling open orders")
        if not self.paper:
            try:
                self.client.cancel_all()
            except RitError:
                pass
        self.status(force=True)
        print("=" * 100)
        print(f"orders {self.posts:,} cancels {self.cancels:,} rejected {self.rejects:,} "
              f"errors {self.errors:,} trailing stops {self.stops:,}  max gross {self.max_gross:,}")
        try:
            tr = self.client.trader()
            print(f"trader NLV {tr.get('nlv')}  fines {tr.get('total_fines')}")
        except RitError:
            pass
        print("=" * 100)
        if self._logf:
            try:
                self._logf.close()
            except OSError:
                pass


# ----------------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------------

def _kv(items, cast=int):
    out = {}
    for it in items or []:
        k, v = it.split("=", 1)
        out[k.strip().upper()] = cast(v.strip().upper() if cast is str else v)
    return out


def preflight(cfg) -> int:
    c = RitClient(cfg)
    try:
        case, tr, secs, lim = c.case(), c.trader(), c.securities(), c.limits()
    except RitError as exc:
        print(f"FAILED: {exc}  ({cfg.base_url})")
        return 1
    print(f"OK  {cfg.base_url}  trader {tr.get('trader_id')}  NLV {tr.get('nlv')}")
    print(f"    case {case.get('name')}  {case.get('status')}  tick {case.get('tick')}/{case.get('ticks_per_period')}")
    for s in secs:
        print(f"    {s.get('ticker'):<5} last {s.get('last')}  pos {s.get('position')}  fee "
              f"{s.get('trading_fee')}  rebate {s.get('limit_order_rebate')}")
    for l in lim:
        print(f"    limit {l.get('name')}: gross {l.get('gross')}/{l.get('gross_limit')} "
              f"net {l.get('net')}/{l.get('net_limit')}")
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="RIT ALGO2e regime-adaptive bot",
                                formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=Config.port)
    p.add_argument("--key", default=None)
    p.add_argument("--tickers", nargs="*", default=[])
    p.add_argument("--size", nargs="*", default=[], metavar="TICKER=SHARES",
                   help="position size per stock (defaults RY=10000 CNR=9000 AC=5000)")
    p.add_argument("--size-mult", type=float, default=1.0,
                   help="scale every stock's size (e.g. 1.2 = 20%% bigger)")
    p.add_argument("--mode", nargs="*", default=[], metavar="TICKER=MOM|MR|OFF",
                   help="force a strategy for a stock (default: measured live)")
    p.add_argument("--no-vol-scale", action="store_true",
                   help="fixed cent thresholds and full size regardless of volatility")
    p.add_argument("--no-adaptive", action="store_true",
                   help="use the replay priors only (RY=MR, CNR/AC=MOM)")
    p.add_argument("--flatten-end", action="store_true", help="flatten in the last 2 ticks")
    p.add_argument("--no-mr-stop", action="store_true",
                   help="no stop-loss on mean-reversion positions")
    p.add_argument("--no-mr-trend-guard", action="store_true",
                   help="keep fading a mean-reversion stock even when it trends hard")
    p.add_argument("--limit-buffer", type=int, default=Config.limit_buffer)
    p.add_argument("--max-reads", type=float, default=Config.max_reads)
    p.add_argument("--max-writes", type=float, default=Config.max_writes)
    p.add_argument("--interval", type=float, default=Config.poll_interval)
    p.add_argument("--keep-running", action="store_true")
    p.add_argument("--log", default=Config.log_path)
    p.add_argument("--no-log", action="store_true")
    p.add_argument("--quiet", action="store_true")
    p.add_argument("--paper", action="store_true")
    p.add_argument("--check", action="store_true")
    a = p.parse_args(argv)
    cfg = Config()
    cfg.host, cfg.port = a.host, a.port
    cfg.api_key = a.key or os.environ.get("RIT_API_KEY") or cfg.api_key
    cfg.tickers = tuple(x.upper() for x in a.tickers)
    cfg.size_override = _kv(a.size)
    cfg.mode_override = _kv(a.mode, str)
    cfg.size_mult = a.size_mult
    cfg.adaptive = not a.no_adaptive
    cfg.vol_scale = not a.no_vol_scale
    cfg.flatten_end = a.flatten_end
    cfg.mr_stop = not a.no_mr_stop
    cfg.mr_trend_guard = not a.no_mr_trend_guard
    cfg.limit_buffer = a.limit_buffer
    cfg.max_reads, cfg.max_writes = a.max_reads, a.max_writes
    cfg.poll_interval, cfg.keep_running = a.interval, a.keep_running
    cfg.log_path = None if a.no_log else a.log
    cfg.verbose = not a.quiet
    if a.check:
        return preflight(cfg)
    bot = Bot(cfg, RitClient(cfg), paper=a.paper)

    def _stop(_s, _f):
        print("\ninterrupt - cancelling orders and stopping", flush=True)
        bot.stop = True

    signal.signal(signal.SIGINT, _stop)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, _stop)
    try:
        bot.run()
    except Exception as exc:                                # noqa: BLE001
        print(f"FATAL: {exc!r}", file=sys.stderr)
        try:
            bot.shutdown()
        except Exception:
            pass
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
