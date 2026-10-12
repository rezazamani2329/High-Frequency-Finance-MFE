"""
RIT ALGO2 - Combined market maker
=================================

Author : Reza Zamani
Course : MFE230X High-Frequency Finance, Fall 2026 - RIT ALGO2 (individual)
Stock  : ALGO (single security), position limit 25,000 shares


Strategy
--------
The bot is a PASSIVE two-sided market maker.  It keeps a bid and an ask
resting in the book and earns the limit-order rebate every time one of them
is hit.  A round trip on two passive fills earns

        (ask - bid) + 2 x rebate   per share,

so the income is fill VOLUME at the front of the queue, and crossing the
spread (which pays the fee instead of earning the rebate) is avoided except
to cut risk.  The only thing that can lose more than the rebates earn is
inventory in a trending market, so the strategy has five parts:

1. Pricing - be the best passive price
   * If the spread is 3 ticks or wider, improve the best competitor bid and
     ask by 1 tick ("pennying") to be first in the queue.
   * With a 2-tick spread only the side that reduces inventory improves.
   * With a 1-tick spread, join the best price.
   * Every quote is checked to be strictly passive (a bid below the best
     ask, an ask above the best bid) before it is sent.
   * A resting order is kept if it is at the target price OR still tied with
     the best competitor ("join" price): a 1-tick wobble costs neither queue
     priority nor order budget.

2. Inventory control - stay close to flat
   * Skew: the side that would ADD to inventory backs off 1 tick per 2,000
     shares held (max 3 ticks), so it gets filled less; the side that
     REDUCES inventory stays at the best price.
   * Size: the adding side shrinks linearly with inventory (5,000 shares
     when flat, zero at the 5,000-share soft cap); the reducing side keeps
     full size.
   * Hard rule before every order:
         |position| + everything resting on that side <= limit - 500,
     so even if every resting order fills at once the limit is never hit.

3. Trend filter
   * If the mid has moved 4+ ticks over the last 10 case ticks, the bot
     never adds inventory against the move (no selling into a rally, no
     buying into a sell-off), and lets inventory that is with the move ride
     2 ticks longer.

4. Stop-losses (each crosses with a limit order at most 3 ticks through the
   touch; any unfilled rest is cancelled at once)
   * Average-cost stop : losing >= 6 ticks/share vs the position's average
                         price -> cut the position.
   * Time stop         : a position held on one side for more than 30 s
                         -> cut it.
   * Trend stop        : holding the wrong side of a trend -> cut it.
   * Emergency brake   : |position| >= 20,000 -> cross back in 2,000-share
                         chunks (the fee is far cheaper than the fine).
   * Limit breach      : over the limit -> cross back under it at once.
   * After a stop, new entries in that direction pause for 8 s; the pause
     doubles with each further stop the same way (max 60 s), so a trend
     cannot keep refilling the losing side.

5. End of case
   * Last 20 ticks : the soft inventory cap shrinks to 2,500 shares.
   * Last 10 ticks : only the inventory-reducing side is quoted, sized to
                     what is held.
   * Optional (--flatten-market): cross to flat in the last 3 ticks.
     Off by default, because positions are marked to market at the close
     while crossing costs the spread plus the fee.


Process (what happens every loop, about every 50 ms)
-------------------------------------------------------
  0. Start-up: connect, read the rebate, fee, max order size, order rate and
     position limit from RIT (never hard-coded), then wait for ACTIVE.
  1. Snapshot: read our open orders, then position + average cost, then the
     book - in that order, so a fill between reads can only over-state our
     exposure, never under-state it.  Our own orders are removed from the
     book to get the competitor best bid / ask.
  2. Bookkeeping: log any position change (fill), record the mid for the
     trend filter, start / reset the clock of the one-sided position.
  3. Risk check, in priority order:
        limit breach > emergency brake > flatten at the bell >
        average-cost stop > time stop > trend stop.
     If one fires: cancel all quotes, re-read the position, cross to cut it,
     start the pause, and skip quoting this loop.
  4. Plan: compute the target bid and ask (price, join price, size) from
     steps 1-2 of the strategy.
  5. Cancel: pull orders that are no longer wanted, at a wrong price, too
     big, or that would break the hard limit.  The inventory-reducing side
     is handled first and gets the order budget first.
  6. Re-read the book if anything was cancelled or the book is stale, and
     re-plan.
  7. Quote: post what is missing on each side, only if strictly passive,
     never crossing our own other side, within the hard limit, and within
     the order rate (sliding 1-second window; widened if RIT still says 429).
  8. Every 5 s: print a status line and append it to reza_rit_algo2_log.csv.
  At the end of the case (or Ctrl-C): cancel all orders and print a summary
  (orders, cancels, stops, crossed shares, max position, NLV, fines).


How to use it
-------------
In the RIT client click the API icon in the status bar: it shows the port
and the API key.  Then:

    pip install requests
    python reza_rit_algo2.py --check --port 9999 --key YOURKEY   # test + case settings
    python reza_rit_algo2.py --paper --port 9999 --key YOURKEY   # quotes only, no orders
    python reza_rit_algo2.py --port 9999 --key YOURKEY           # trade

The key can also come from the environment variable RIT_API_KEY.  The
script can be started before the case; it waits for ACTIVE.  Every
parameter has a flag (see --help), e.g.

    --size 5000 --max-inv 5000      quote size per side, soft inventory cap
    --stop-loss 6                   average-cost stop in ticks (0 = off)
    --max-age 30                    time stop in seconds       (0 = off)
    --no-trend-stop                 turn the trend stop off
    --hard-flatten 20000            emergency brake level      (0 = off)
    --flatten-market                cross to flat in the final ticks
    --keep-running                  stay up for several heats
"""

from __future__ import annotations

import argparse
import collections
import csv
import os
import signal
import sys
import time
from dataclasses import dataclass
from typing import Deque, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter

BUY, SELL = "BUY", "SELL"
SIGN = {BUY: 1, SELL: -1}


# ============================================================================
# Configuration
# ============================================================================

@dataclass
class Config:
    # --- connection -------------------------------------------------------
    host: str = "localhost"
    port: int = 9999
    api_key: str = ""
    http_timeout: float = 2.0
    ticker: str = "ALGO"

    # --- quoting (Saurabh / Hrafnhildur / Paraj) ----------------------------
    quote_size: int = 5000          # shares per side when flat (= max order in ALGO2)
    max_side_size: int = 5000       # cap on the inventory-reducing side
    max_inventory: int = 5000       # soft cap; Saurabh's sweep: 5k beat 7.5k/10k/15k
    skew_step: int = 2000           # Hrafnhildur: adding side backs off 1 tick per this many shares
    max_skew_ticks: int = 3
    penny: bool = True              # improve the best competitor price when the spread allows
    keep_join: bool = True          # Hrafnhildur: keep an order resting at the join price
    linear_size: bool = True        # Paraj: adding side shrinks linearly with inventory
    min_order: int = 100
    topup_frac: float = 0.5         # top a side up when less than this fraction is resting
    default_spread_ticks: int = 10  # assumed width when one side of the book is empty
    max_book_age: float = 0.05      # re-read the book if it is older than this before posting
    post_hold: float = 1.5          # after an unclear POST failure, hold new orders this long
    pending_ttl: float = 2.0        # sent orders count as live this long even if not listed

    # --- trend (Saurabh) ----------------------------------------------------
    trend_ticks: int = 2            # back-off of the side trading against a trend (0 = filter off)
    trend_window: int = 10          # case ticks over which the trend is measured
    trend_threshold: int = 4        # mid move in ticks that counts as a trend
    trend_stop: bool = True         # cut a position held against the trend

    # --- stop-losses --------------------------------------------------------
    stop_loss_ticks: int = 6        # Hrafnhildur: loss per share vs average cost (0 = off)
    max_age: float = 30.0           # Daisy: seconds a one-sided position may live (0 = off)
    stop_pause: float = 8.0         # Daisy: pause same-direction entries after a stop ...
    stop_pause_max: float = 60.0    # ... doubling per repeated stop, capped here
    stop_cooldown: float = 1.0      # seconds between forced (crossing) orders

    # --- risk ---------------------------------------------------------------
    position_limit: int = 25000     # fallback; the real value comes from /limits
    limit_buffer: int = 500         # hard cap = limit - buffer
    max_order_size: int = 5000      # fallback; the real value comes from /securities
    hard_flatten: int = 20000       # Paraj: emergency brake at this |position|
    emergency_chunk: int = 2000     # Paraj: shares crossed per emergency order
    cross_slip_ticks: int = 3       # max ticks paid through the touch when crossing

    # --- end of case (Saurabh wind-down + Hrafnhildur unwind) ----------------
    wind_down_ticks: int = 20       # final ticks: soft cap shrinks to end_inventory
    end_inventory: int = 2500
    reduce_only_ticks: int = 10     # final ticks: only the reducing side is quoted
    flatten_market: bool = False    # cross to flat over the final flatten_ticks
    flatten_ticks: int = 3

    # --- pacing ---------------------------------------------------------------
    orders_per_second: float = 5.0  # fallback; real value from /securities
    rate_window: float = 1.05
    poll_interval: float = 0.05
    case_poll_interval: float = 0.25
    limits_poll_interval: float = 5.0

    # --- output ---------------------------------------------------------------
    status_every: float = 5.0
    log_path: Optional[str] = "reza_rit_algo2_log.csv"
    verbose: bool = True
    keep_running: bool = False

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}/v1"


# ============================================================================
# REST client
# ============================================================================

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


class RitClient:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.session = requests.Session()
        self.session.headers.update({"X-API-Key": cfg.api_key})
        self.session.mount("http://", HTTPAdapter(pool_connections=4, pool_maxsize=4,
                                                  max_retries=0))
        self.calls = 0

    def _request(self, method, path, params=None, retries=1):
        url = self.cfg.base_url + path
        last = None
        for attempt in range(retries + 1):
            self.calls += 1
            try:
                r = self.session.request(method, url, params=params,
                                         timeout=self.cfg.http_timeout)
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
                # Paraj: take the wait from the server and recalibrate
                wait = 0.2
                for src in (r.headers.get("Retry-After"), r.headers.get("X-Wait-Until")):
                    if src:
                        try:
                            wait = float(src)
                            break
                        except ValueError:
                            pass
                else:
                    try:
                        wait = float(r.json().get("wait", wait))
                    except Exception:
                        pass
                raise RateLimited(max(wait, 0.0))
            if r.status_code == 401:
                raise RitError("401 Unauthorized - wrong API key, or the RIT client is "
                               "not logged in (check the API icon in the RIT status bar)")
            if 400 <= r.status_code < 500:
                raise Rejected(r.status_code, r.text[:200])
            last = RitError(f"{method} {path} -> {r.status_code}")
            time.sleep(0.02 * (attempt + 1))
        raise RitError(f"{method} {path} failed: {last}")

    def case(self):
        return self._request("GET", "/case")

    def trader(self):
        return self._request("GET", "/trader")

    def limits(self):
        return self._request("GET", "/limits")

    def security(self, ticker):
        res = self._request("GET", "/securities", {"ticker": ticker})
        if isinstance(res, list):
            for s in res:
                if s.get("ticker") == ticker:
                    return s
            raise RitError(f"ticker {ticker} not found in this case")
        return res

    def all_securities(self):
        return self._request("GET", "/securities")

    def book(self, ticker, limit=40):
        return self._request("GET", "/securities/book", {"ticker": ticker, "limit": limit})

    def orders(self, status="OPEN"):
        return self._request("GET", "/orders", {"status": status})

    def post_limit(self, ticker, action, qty, price):
        # not idempotent: never retried - the next /orders read tells the truth
        return self._request("POST", "/orders", {"ticker": ticker, "type": "LIMIT",
                                                 "quantity": qty, "action": action,
                                                 "price": price}, retries=0)

    def cancel(self, order_id):
        return self._request("DELETE", f"/orders/{order_id}", retries=0)

    def cancel_all(self, ticker):
        return self._request("POST", "/commands/cancel", {"ticker": ticker}, retries=0)


class OrderLimiter:
    """At most per_sec order submissions in any sliding window of `window` s."""

    def __init__(self, per_sec, window):
        self.per_sec = max(int(per_sec), 1)
        self.window = window
        self.stamps: Deque[float] = collections.deque()
        self.blocked_until = 0.0
        self.count_cancels = False      # switched on if a DELETE ever gets a 429

    def set_rate(self, per_sec):
        if per_sec and per_sec > 0:
            self.per_sec = max(int(per_sec), 1)

    def remaining(self):
        now = time.monotonic()
        if now < self.blocked_until:
            return 0
        while self.stamps and self.stamps[0] <= now - self.window:
            self.stamps.popleft()
        return max(self.per_sec - len(self.stamps), 0)

    def record(self):
        self.stamps.append(time.monotonic())

    def block(self, seconds):
        self.blocked_until = max(self.blocked_until,
                                 time.monotonic() + min(max(seconds, 0.05), 2.0))
        self.window = min(self.window + 0.05, 1.5)


# ============================================================================
# State
# ============================================================================

@dataclass
class MyOrder:
    order_id: int
    action: str
    price: int          # integer ticks
    remaining: int


@dataclass
class Snapshot:
    tick: int
    pos: int
    avg_cost: Optional[float]   # position average price, in ticks
    last: Optional[int]
    comp_bid: Optional[int]     # best competitor prices, our own orders excluded
    comp_ask: Optional[int]
    mine: Dict[str, List[MyOrder]]
    stamp: float = 0.0


@dataclass
class Quote:
    price: int          # where a new order goes
    join: int           # least aggressive price an existing order may keep (Hrafnhildur)
    target: int         # shares we want resting on this side

    def fits(self, side, price):
        if side == BUY:
            return self.join <= price <= self.price
        return self.price <= price <= self.join


def lots(x, lot=100):
    return max(int(x) // lot * lot, 0)


# ============================================================================
# The market maker
# ============================================================================

class MarketMaker:

    def __init__(self, cfg: Config, client: RitClient, paper=False):
        self.cfg, self.client, self.paper = cfg, client, paper
        self.stop = False

        self.decimals = 2
        self.max_order = cfg.max_order_size
        self.min_price_t: Optional[int] = None
        self.max_price_t: Optional[int] = None
        self.fee = self.rebate = 0.0
        self.limit_units = 1.0
        self.limit_names: List[str] = []
        self.position_limit = cfg.position_limit

        self.trader_id = None
        self.recent_ids: Deque[int] = collections.deque(maxlen=1000)
        self.own_ids: set = set()
        self.pending: Dict[int, tuple] = {}
        self.limiter = OrderLimiter(cfg.orders_per_second, cfg.rate_window)

        self.ticks_per_period = 300
        self.tick = 0
        self.mid_hist: Deque = collections.deque(maxlen=4000)
        self.last_snapshot: Optional[Snapshot] = None
        self.last_plan: Dict[str, Optional[Quote]] = {BUY: None, SELL: None}

        # stop-loss state
        self.next_force = 0.0                     # cooldown between crossing orders
        self.paused = {1: 0.0, -1: 0.0}           # direction -> entries paused until
        self.streak_dir, self.streak, self.last_stop_at = 0, 0, -1e9
        self.side_since: Optional[float] = None   # when the current one-sided position began
        self.side_dir = 0
        self.last_pos: Optional[int] = None
        self._post_hold_until = 0.0

        # stats
        self.c = collections.Counter()
        self.max_abs_pos = 0
        self.started = time.monotonic()
        self._last_status = self._last_limits = 0.0

        self._log_file = self._log = None
        if cfg.log_path:
            new = not os.path.exists(cfg.log_path)
            try:
                self._log_file = open(cfg.log_path, "a", newline="", encoding="utf-8")
                self._log = csv.writer(self._log_file)
                if new:
                    self._log.writerow(["wall_time", "tick", "position", "avg_cost",
                                        "comp_bid", "comp_ask", "my_bid", "bid_qty",
                                        "my_ask", "ask_qty", "nlv", "orders", "cancels",
                                        "stops", "note"])
            except OSError as exc:
                print(f"WARNING: cannot write {cfg.log_path} ({exc}); no CSV log", flush=True)
                self._log_file = self._log = None

    # -- helpers ----------------------------------------------------------------

    def say(self, msg):
        if self.cfg.verbose:
            print(f"[{time.monotonic() - self.started:7.2f}s t{self.tick:>3}] {msg}", flush=True)

    def to_ticks(self, price) -> int:
        return int(round(float(price) * 10 ** self.decimals))

    def to_price(self, t) -> str:
        return f"{t / 10 ** self.decimals:.{self.decimals}f}"

    def fmt(self, t):
        return "-" if t is None else self.to_price(t)

    @property
    def hard_cap(self):
        return max(self.position_limit - self.cfg.limit_buffer, 0)

    # -- static data --------------------------------------------------------------

    def load_security(self, sec):
        self.decimals = int(_num(sec, "quoted_decimals", 2))
        mts = int(_num(sec, "max_trade_size", 0))
        self.max_order = min(mts, self.cfg.max_order_size) if mts > 0 else self.cfg.max_order_size
        lo, hi = _num(sec, "min_price", 0.0), _num(sec, "max_price", 0.0)
        self.min_price_t = self.to_ticks(lo) if lo and lo > 0 else None
        self.max_price_t = self.to_ticks(hi) if hi and hi > 0 else None
        self.fee = float(_num(sec, "trading_fee", 0.0))
        self.rebate = float(_num(sec, "limit_order_rebate", 0.0))
        self.limiter.set_rate(float(_num(sec, "api_orders_per_second", 0.0)))
        names, units = [], 1.0
        for lim in sec.get("limits") or []:
            if lim.get("name") is not None:
                names.append(lim["name"])
                units = max(units, float(_num(lim, "units", 1.0)) or 1.0)
        self.limit_names, self.limit_units = names, units

    def refresh_limits(self):
        self._last_limits = time.monotonic()
        try:
            payload = self.client.limits()
        except RitError as exc:
            self.say(f"limits refresh failed: {exc}")
            return
        best = None
        for lim in payload or []:
            if self.limit_names and lim.get("name") not in self.limit_names:
                continue
            for key in ("gross_limit", "net_limit"):
                v = float(_num(lim, key, 0.0))
                if v > 0:
                    shares = int(v / self.limit_units)
                    best = shares if best is None else min(best, shares)
        if best is not None:
            self.position_limit = best

    # -- market data ----------------------------------------------------------------

    def snapshot(self) -> Optional[Snapshot]:
        """Open orders, then position, then book - in that order, so a fill
        between reads can only over-state our exposure, never under-state it."""
        t = self.cfg.ticker
        try:
            orders = self.client.orders("OPEN")
            sec = self.client.security(t)
            stamp = time.monotonic()
            book = self.client.book(t)
        except RateLimited as exc:
            time.sleep(min(exc.wait, 1.0))
            return None
        except RitError as exc:
            self.c["errors"] += 1
            self.say(f"snapshot failed: {exc}")
            time.sleep(0.1)
            return None

        self.load_security(sec)
        mine: Dict[str, List[MyOrder]] = {BUY: [], SELL: []}
        self.own_ids = set(self.recent_ids)
        for o in orders or []:
            oid = o.get("order_id")
            if oid is not None:
                self.own_ids.add(oid)
            if o.get("ticker") != t or o.get("status", "OPEN") != "OPEN":
                continue
            if o.get("price") is None or o.get("action") not in (BUY, SELL):
                continue
            rem = int(_num(o, "quantity", 0)) - int(_num(o, "quantity_filled", 0))
            if rem > 0:
                mine[o["action"]].append(MyOrder(oid, o["action"], self.to_ticks(o["price"]), rem))

        now = time.monotonic()
        listed = {o.get("order_id") for o in orders or []}
        for oid, (mo, sent) in list(self.pending.items()):
            if oid in listed or now - sent > self.cfg.pending_ttl:
                del self.pending[oid]
            else:
                mine[mo.action].append(MyOrder(mo.order_id, mo.action, mo.price, mo.remaining))

        snap = self._make_snapshot(sec, book, mine, stamp)
        self.last_snapshot = snap
        return snap

    def _make_snapshot(self, sec, book, mine, stamp) -> Snapshot:
        own = self.own_ids | set(self.recent_ids)

        def is_own(e):
            if e.get("order_id") in own:
                return True
            return self.trader_id is not None and e.get("trader_id") == self.trader_id

        cb = ca = None
        for key, side in (("bids", BUY), ("asks", SELL)):
            for e in (book or {}).get(key) or []:
                if is_own(e) or e.get("price") is None:
                    continue
                if int(_num(e, "quantity", 0)) - int(_num(e, "quantity_filled", 0)) <= 0:
                    continue
                p = self.to_ticks(e["price"])
                if side == BUY:
                    cb = p if cb is None else max(cb, p)
                else:
                    ca = p if ca is None else min(ca, p)

        last = _num(sec, "last", 0.0)
        pos = int(round(float(_num(sec, "position", 0.0))))
        vwap = _num(sec, "vwap", 0.0)
        self.max_abs_pos = max(self.max_abs_pos, abs(pos))
        return Snapshot(tick=self.tick, pos=pos,
                        avg_cost=float(vwap) * 10 ** self.decimals if vwap and pos else None,
                        last=self.to_ticks(last) if last and last > 0 else None,
                        comp_bid=cb, comp_ask=ca, mine=mine, stamp=stamp)

    def refresh_market(self, live) -> Optional[Snapshot]:
        try:
            sec = self.client.security(self.cfg.ticker)
            stamp = time.monotonic()
            book = self.client.book(self.cfg.ticker)
        except RitError:
            return None
        snap = self._make_snapshot(sec, book, live, stamp)
        self.last_snapshot = snap
        return snap

    def refresh_position(self) -> Optional[int]:
        try:
            sec = self.client.security(self.cfg.ticker)
        except RitError:
            return None
        pos = int(round(float(_num(sec, "position", 0.0))))
        self.max_abs_pos = max(self.max_abs_pos, abs(pos))
        return pos

    # -- bookkeeping of the position -------------------------------------------------

    def note_position(self, s: Snapshot):
        """Hrafnhildur's fill log + Daisy's clock on how long we have been one-sided."""
        if self.last_pos is not None and s.pos != self.last_pos:
            cost = f"  avg cost {self.to_price(round(s.avg_cost))}" if s.avg_cost else ""
            self.say(f"fill {s.pos - self.last_pos:+,}  ->  position {s.pos:+,}  "
                     f"book {self.fmt(s.comp_bid)}/{self.fmt(s.comp_ask)}{cost}")
        self.last_pos = s.pos
        d = (s.pos > 0) - (s.pos < 0)
        if d == 0:
            self.side_dir, self.side_since = 0, None
        elif d != self.side_dir:
            self.side_dir, self.side_since = d, time.monotonic()

    def note_mid(self, s: Snapshot):
        if s.comp_bid is not None and s.comp_ask is not None:
            self.mid_hist.append((s.tick, s.comp_bid + s.comp_ask))

    def trend(self) -> int:
        """+1 rally, -1 sell-off, 0 neither (Saurabh)."""
        if self.cfg.trend_ticks <= 0 or not self.mid_hist:
            return 0
        now_tick, now_mid = self.mid_hist[-1]
        if self.mid_hist[0][0] > now_tick - self.cfg.trend_window:
            return 0                                   # not enough history yet
        past = next(m for t, m in self.mid_hist if t >= now_tick - self.cfg.trend_window)
        move = now_mid - past                          # half-ticks (mid x 2)
        if move >= 2 * self.cfg.trend_threshold:
            return 1
        if move <= -2 * self.cfg.trend_threshold:
            return -1
        return 0

    def soft_cap(self, tick) -> int:
        cap = min(self.cfg.max_inventory, self.hard_cap)
        if self.ticks_per_period - tick <= self.cfg.wind_down_ticks:
            cap = min(cap, self.cfg.end_inventory)
        return max(cap, 0)

    # -- the quote model ---------------------------------------------------------------

    def plan(self, s: Snapshot) -> Dict[str, Optional[Quote]]:
        none = {BUY: None, SELL: None}
        cb, ca = s.comp_bid, s.comp_ask
        width = max(self.cfg.default_spread_ticks, 2)
        if cb is None and ca is None:
            if s.last is None:
                return none
            cb, ca = s.last - width // 2, s.last + (width - width // 2)
        elif cb is None:
            cb = ca - width
        elif ca is None:
            ca = cb + width
        spread = ca - cb
        if spread < 1:
            return none                                # locked / crossed: sit out
        pos = s.pos

        # price: penny when there is room (Saurabh / Hrafnhildur), else join.
        # With a 2-tick spread only the side that reduces inventory pennies.
        bid, ask = cb, ca
        if self.cfg.penny:
            if spread >= 3:
                bid, ask = cb + 1, ca - 1
            elif spread == 2:
                if pos > 0:
                    ask -= 1
                else:
                    bid += 1
        jbid, jask = (cb, ca) if self.cfg.keep_join else (bid, ask)

        # skew (Hrafnhildur): the adding side backs off 1 tick per skew_step shares
        back = min(abs(pos) // max(self.cfg.skew_step, 1), self.cfg.max_skew_ticks)
        bid_off = -back if pos > 0 else 0
        ask_off = back if pos < 0 else 0

        # trend (Saurabh): never add against it; let inventory WITH it ride
        tr = self.trend()
        no_bid = no_ask = False
        if tr > 0:
            if pos > 0:
                ask_off += self.cfg.trend_ticks
            else:
                no_ask = True
        elif tr < 0:
            if pos < 0:
                bid_off -= self.cfg.trend_ticks
            else:
                no_bid = True
        bid, jbid = bid + bid_off, jbid + bid_off
        ask, jask = ask + ask_off, jask + ask_off

        # sizes: Paraj's linear shrink of the adding side + Saurabh's soft cap
        cap = self.soft_cap(s.tick)
        q = self.cfg.quote_size
        inv = abs(pos)
        add = min(q, cap - inv)
        if self.cfg.linear_size and cap > 0:
            add = min(add, int(q * max(0.0, 1.0 - inv / cap)))
        reduce = min(self.cfg.max_side_size, q + inv)
        if pos > 0:
            bid_tgt, ask_tgt = add, reduce
        elif pos < 0:
            bid_tgt, ask_tgt = reduce, add
        else:
            bid_tgt = ask_tgt = min(q, cap)
        if no_bid:
            bid_tgt = 0
        if no_ask:
            ask_tgt = 0

        # Daisy: after a stop, no new entries in that direction for a while
        now = time.monotonic()
        if now < self.paused[1] and pos >= 0:
            bid_tgt = 0
        if now < self.paused[-1] and pos <= 0:
            ask_tgt = 0

        # end of case: reduce only, and only what we hold (Hrafnhildur / Saurabh)
        if self.ticks_per_period - s.tick <= self.cfg.reduce_only_ticks:
            bid_tgt = min(bid_tgt, -pos) if pos < 0 else 0
            ask_tgt = min(ask_tgt, pos) if pos > 0 else 0

        # guards: strictly passive, inside the price band, our bid below our ask
        bid, jbid = min(bid, ca - 1), min(jbid, ca - 1)
        ask, jask = max(ask, cb + 1), max(jask, cb + 1)
        if bid >= ask:
            if pos > 0:
                bid = ask - 1
            else:
                ask = bid + 1
        jbid, jask = min(jbid, bid), max(jask, ask)
        if bid < 1 or (self.min_price_t is not None and bid < self.min_price_t):
            bid_tgt = 0
        if self.max_price_t is not None and ask > self.max_price_t:
            ask_tgt = 0

        bid_tgt, ask_tgt = lots(bid_tgt), lots(ask_tgt)
        return {BUY: Quote(bid, jbid, bid_tgt) if bid_tgt >= self.cfg.min_order else None,
                SELL: Quote(ask, jask, ask_tgt) if ask_tgt >= self.cfg.min_order else None}

    # -- order primitives ------------------------------------------------------------

    def _cancel(self, o: MyOrder) -> Optional[bool]:
        """True = gone, False = still live, None = unknown (treat as live)."""
        if self.limiter.count_cancels:
            if self.limiter.remaining() <= 0:
                return False
            self.limiter.record()
        try:
            self.client.cancel(o.order_id)
        except RateLimited as exc:
            self.limiter.count_cancels = True
            self.limiter.block(exc.wait)
            self.c["throttled"] += 1
            return False
        except Rejected:
            return True        # already filled / cancelled; the position re-read shows fills
        except RitError as exc:
            self.c["errors"] += 1
            self.say(f"cancel {o.order_id} failed: {exc}")
            return None
        self.c["cancels"] += 1
        self.pending.pop(o.order_id, None)
        return True

    def _post(self, action, qty, price_t) -> Optional[MyOrder]:
        if self.limiter.remaining() <= 0 or time.monotonic() < self._post_hold_until:
            return None
        self.limiter.record()
        try:
            o = self.client.post_limit(self.cfg.ticker, action, qty, self.to_price(price_t))
        except RateLimited as exc:
            self.limiter.block(exc.wait)
            self.c["throttled"] += 1
            return None
        except Rejected as exc:
            self.c["rejects"] += 1
            self.say(f"order rejected ({action} {qty} @ {self.to_price(price_t)}): {exc}")
            return None
        except RitError as exc:
            # outcome unknown: hold new orders until it can show up in /orders
            self.c["errors"] += 1
            self._post_hold_until = time.monotonic() + self.cfg.post_hold
            self.say(f"order post failed ({action} {qty}): {exc}")
            raise
        self.c["orders"] += 1
        oid = o.get("order_id")
        if oid is not None:
            self.recent_ids.append(oid)
        filled = int(_num(o, "quantity_filled", 0))
        if filled > 0:
            self.c["crossed_shares"] += filled
        rem = int(_num(o, "quantity", qty)) - filled
        if o.get("status", "OPEN") == "OPEN" and rem > 0:
            placed = MyOrder(oid, action, price_t, rem)
            if oid is not None:
                self.pending[oid] = (placed, time.monotonic())
            return placed
        return MyOrder(oid, action, price_t, 0)

    def _pull_all(self, s: Snapshot):
        try:
            self.client.cancel_all(self.cfg.ticker)       # one request for everything
            self.c["cancels"] += 1
            self.pending.clear()
        except RitError:
            for side in (BUY, SELL):
                for o in s.mine[side]:
                    self._cancel(o)

    def _cross(self, s: Snapshot, action, qty, note):
        """Marketable limit at most cross_slip_ticks through the touch; the
        unfilled rest is cancelled at once - an aggressive order never rests."""
        ref = s.comp_ask if action == BUY else s.comp_bid
        if ref is None:
            ref = s.last
        qty = lots(min(qty, self.max_order))
        if ref is None or qty < self.cfg.min_order or self.paper:
            return
        slip = self.cfg.cross_slip_ticks
        price = ref + slip if action == BUY else max(ref - slip, 1)
        # a stop must not be dropped because quoting used up this second's
        # order budget: wait (at most ~1 s) for a free slot
        give_up = time.monotonic() + 1.2
        while self.limiter.remaining() <= 0 and time.monotonic() < give_up:
            time.sleep(0.02)
        self.say(f"{note}: {action} {qty:,} @ {self.to_price(price)} (pos {s.pos:+,})")
        try:
            o = self._post(action, qty, price)
        except RitError:
            self.next_force = 0.0              # outcome unknown: re-check next loop
            return
        if o is None:
            self.say("  ! forced order not sent (rate limit) - retrying next loop")
            self.next_force = 0.0
            return
        if o.remaining > 0:
            self._cancel(o)

    def _forced(self, s: Snapshot, qty_fn, note, stop_dir=0) -> bool:
        """Pull our quotes, re-read the position, cross qty_fn(pos) back to flat.
        stop_dir != 0 also starts Daisy's doubling pause for that direction."""
        now = time.monotonic()
        if now < self.next_force:
            return True                    # still cooling down: do not quote meanwhile
        self.next_force = now + self.cfg.stop_cooldown
        self._pull_all(s)
        pos = self.refresh_position()
        if pos is None or pos == 0:
            return True
        s.pos = pos
        if stop_dir:
            d = 1 if pos > 0 else -1
            if d == self.streak_dir and now - self.last_stop_at < 3 * self.cfg.stop_pause:
                self.streak += 1
            else:
                self.streak_dir, self.streak = d, 1
            self.last_stop_at = now
            self.paused[d] = now + min(self.cfg.stop_pause * 2 ** (self.streak - 1),
                                       self.cfg.stop_pause_max)
            self.c["stops"] += 1
            self.side_since = now          # the age clock restarts after a cut
        self._cross(s, SELL if pos > 0 else BUY, qty_fn(pos), note)
        return True

    # -- risk: every stop in one place ----------------------------------------------

    def risk(self, s: Snapshot) -> bool:
        """True if this loop was spent on a forced trade (or its cooldown)."""
        pos, cfg = s.pos, self.cfg
        if pos == 0:
            return False
        left = self.ticks_per_period - s.tick
        now = time.monotonic()

        # 1. over the position limit: fines accrue every tick (all scripts)
        if abs(pos) > self.position_limit:
            self.next_force = 0.0
            return self._forced(s, lambda p: abs(p) - self.hard_cap, "LIMIT BREACH")

        # 2. Paraj's emergency brake
        if cfg.hard_flatten and abs(pos) >= cfg.hard_flatten:
            return self._forced(s, lambda p: min(cfg.emergency_chunk,
                                                 abs(p) - self.soft_cap(s.tick)),
                                "EMERGENCY FLATTEN")

        # 3. Hrafnhildur's flatten at the bell (optional)
        if cfg.flatten_market and left <= cfg.flatten_ticks:
            return self._forced(s, abs, "FLATTEN")

        if abs(pos) < cfg.min_order:
            return False

        # 4. Hrafnhildur's stop-loss against the average cost
        if cfg.stop_loss_ticks > 0 and s.avg_cost is not None:
            exit_px = s.comp_bid if pos > 0 else s.comp_ask
            if exit_px is not None:
                loss = (s.avg_cost - exit_px) if pos > 0 else (exit_px - s.avg_cost)
                if loss >= cfg.stop_loss_ticks:
                    return self._forced(s, abs, f"STOP-LOSS losing {loss / 10 ** self.decimals:.3f}"
                                                f"/share vs avg cost", stop_dir=1)

        # 5. Daisy's age stop: a one-sided position that will not come back
        if cfg.max_age > 0 and self.side_since is not None \
                and now - self.side_since >= cfg.max_age:
            return self._forced(s, abs, f"AGE STOP held {now - self.side_since:.0f}s",
                                stop_dir=1)

        # 6. Saurabh's trend stop: on the wrong side of a trend
        tr = self.trend()
        if cfg.trend_stop and tr != 0 and pos * tr < 0:
            return self._forced(s, abs, f"TREND {'UP' if tr > 0 else 'DOWN'} STOP",
                                stop_dir=1)
        return False

    # -- one loop -------------------------------------------------------------------

    def step(self):
        s = self.snapshot()
        if s is None:
            return
        self.note_mid(s)
        self.note_position(s)

        if self.paper:
            self.last_plan = self.plan(s)
            return
        if self.risk(s):
            return

        plan = self.plan(s)
        self.last_plan = plan
        live = {BUY: list(s.mine[BUY]), SELL: list(s.mine[SELL])}
        order = [SELL, BUY] if s.pos > 0 else [BUY, SELL]     # reducing side first

        # phase 1: decide what to pull
        budget = self.limiter.remaining()
        cancel: List[MyOrder] = []
        for side in order:
            q, orders = plan[side], live[side]
            if q is None:
                cancel.extend(orders)
                continue
            at = [o for o in orders if q.fits(side, o.price)]       # Hrafnhildur: keep queue
            off = [o for o in orders if not q.fits(side, o.price)]
            newest_first = off + sorted(at, key=lambda x: -(x.order_id or 0))

            # hard invariant: even if everything resting fills, stay under the cap
            exposure = SIGN[side] * s.pos + sum(o.remaining for o in orders)
            urgent: List[MyOrder] = []
            for o in newest_first:
                if exposure <= self.hard_cap:
                    break
                urgent.append(o)
                exposure -= o.remaining
            # size cap: more resting than target (+ half an order of slack)
            excess = sum(o.remaining for o in orders if o not in urgent) \
                - (q.target + self.cfg.quote_size // 2)
            for o in newest_first:
                if excess <= 0:
                    break
                if o not in urgent:
                    urgent.append(o)
                    excess -= o.remaining
            cancel.extend(urgent)

            # reprice only when the replacement can be sent now
            reprice = [o for o in off if o not in urgent]
            if reprice:
                need = 1 + (len(reprice) if self.limiter.count_cancels else 0)
                if budget >= need:
                    cancel.extend(reprice)
                    budget -= need
            elif not at:
                budget -= 1

        # phase 2: pull
        cancelled = False
        for o in cancel:
            if self._cancel(o):
                cancelled = True
                live[o.action] = [x for x in live[o.action] if x.order_id != o.order_id]

        if not any(plan[side] is not None for side in order):
            return
        if cancelled or time.monotonic() - s.stamp > self.cfg.max_book_age:
            s2 = self.refresh_market(live)       # Paraj: re-quote off the fresh book
            if s2 is None:
                return
            s = s2
            if abs(s.pos) > self.position_limit:
                return
            plan = self.plan(s)
            self.last_plan = plan

        # phase 3: (re)quote
        for side in order:
            q = plan[side]
            if q is None:
                continue
            side_total = sum(o.remaining for o in live[side])
            at_total = sum(o.remaining for o in live[side] if q.fits(side, o.price))
            deficit = q.target - at_total
            if deficit < max(self.cfg.min_order, int(self.cfg.topup_frac * q.target)):
                continue
            room = self.hard_cap - SIGN[side] * s.pos - side_total
            qty = lots(min(deficit, q.target - side_total, room, self.max_order))
            if qty < self.cfg.min_order:
                continue
            if side == BUY and s.comp_ask is not None and q.price >= s.comp_ask:
                continue
            if side == SELL and s.comp_bid is not None and q.price <= s.comp_bid:
                continue
            other = live[SELL if side == BUY else BUY]          # never trade with ourselves
            if side == BUY and other and q.price >= min(o.price for o in other):
                continue
            if side == SELL and other and q.price <= max(o.price for o in other):
                continue
            try:
                placed = self._post(side, qty, q.price)
            except RitError:
                return
            if placed is None:
                if self.limiter.remaining() <= 0:
                    break
                continue
            if placed.remaining > 0:
                live[side].append(placed)

    # -- reporting -------------------------------------------------------------------

    def status(self, force=False):
        now = time.monotonic()
        if not force and now - self._last_status < self.cfg.status_every:
            return
        self._last_status = now
        s = self.last_snapshot
        if s is None:
            return
        nlv = None
        try:
            nlv = self.client.trader().get("nlv")
        except RitError:
            pass
        bq, aq = self.last_plan.get(BUY), self.last_plan.get(SELL)
        bid_rest = sum(o.remaining for o in s.mine[BUY])
        ask_rest = sum(o.remaining for o in s.mine[SELL])
        cost = self.to_price(round(s.avg_cost)) if s.avg_cost else "-"
        self.say(f"pos {s.pos:+7,} (avg {cost})  mkt {self.fmt(s.comp_bid)}/{self.fmt(s.comp_ask)}  "
                 f"quote {self.fmt(bq.price if bq else None)} x {bid_rest:,} / "
                 f"{self.fmt(aq.price if aq else None)} x {ask_rest:,}  "
                 f"NLV {'-' if nlv is None else f'{nlv:,.2f}'}  orders {self.c['orders']} "
                 f"cxl {self.c['cancels']} stops {self.c['stops']} 429 {self.c['throttled']}")
        if self._log:
            try:
                self._log.writerow([f"{time.time():.3f}", s.tick, s.pos, cost,
                                    self.fmt(s.comp_bid), self.fmt(s.comp_ask),
                                    self.fmt(bq.price if bq else None), bid_rest,
                                    self.fmt(aq.price if aq else None), ask_rest, nlv,
                                    self.c["orders"], self.c["cancels"], self.c["stops"],
                                    "paper" if self.paper else ""])
                self._log_file.flush()
            except OSError:
                self._log = None

    # -- lifecycle -------------------------------------------------------------------

    def connect(self) -> bool:
        while not self.stop:
            try:
                self.trader_id = self.client.trader().get("trader_id")
                self.load_security(self.client.security(self.cfg.ticker))
                self.refresh_limits()
                return True
            except RitError as exc:
                self.say(f"waiting for RIT client: {exc}")
                time.sleep(2.0)
        return False

    def print_setup(self):
        cfg = self.cfg
        print("-" * 78)
        print(f"ticker          : {cfg.ticker}   trader {self.trader_id}")
        print(f"rebate / fee    : {self.rebate} passive / {self.fee} active per share")
        print(f"max order       : {self.max_order:,}   order rate {self.limiter.per_sec}/s")
        print(f"position limit  : {self.position_limit:,}  (hard cap {self.hard_cap:,}, "
              f"soft cap {min(cfg.max_inventory, self.hard_cap):,}, "
              f"emergency at {cfg.hard_flatten:,})")
        print(f"quoting         : {cfg.quote_size:,}/side, penny {'on' if cfg.penny else 'off'}, "
              f"skew 1 tick / {cfg.skew_step:,} sh, keep-join {'on' if cfg.keep_join else 'off'}")
        print(f"stops           : avg-cost {cfg.stop_loss_ticks or 'off'} ticks | "
              f"age {cfg.max_age or 'off'} s | trend {'on' if cfg.trend_stop else 'off'} | "
              f"pause {cfg.stop_pause}s doubling")
        print(f"mode            : {'PAPER (no orders sent)' if self.paper else 'LIVE'}")
        if self.rebate <= 0:
            print("  NOTE: no passive rebate reported - check this is the ALGO2 case")
        print("-" * 78)

    def run(self):
        if not self.connect():
            return
        self.print_setup()
        was_active, last_status, case, case_stamp, note_at = False, None, {}, 0.0, 0.0
        while not self.stop:
            now = time.monotonic()
            if now - case_stamp >= self.cfg.case_poll_interval:
                try:
                    case = self.client.case()
                    case_stamp = now
                except RateLimited as exc:
                    time.sleep(min(exc.wait, 1.0))
                    continue
                except RitError as exc:
                    self.say(f"case poll failed: {exc}")
                    time.sleep(0.5)
                    continue
            status = case.get("status")
            self.tick = int(_num(case, "tick", 0))
            self.ticks_per_period = int(_num(case, "ticks_per_period", 300)) or 300
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
                    self.mid_hist.clear()
                    self.last_snapshot, self.last_pos = None, None
                    self.paused = {1: 0.0, -1: 0.0}
                if now - note_at > 10.0:
                    self.say("waiting for the case to start (Ctrl-C to quit) ...")
                    note_at = now
                time.sleep(0.25)
                continue

            if not was_active:
                was_active = True
                self.refresh_limits()
                self.say("case ACTIVE - quoting")
            self.c["loops"] += 1
            if now - self._last_limits >= self.cfg.limits_poll_interval:
                self.refresh_limits()
            try:
                self.step()
                self.status()
            except Exception as exc:                 # noqa: BLE001 - keep trading
                self.c["errors"] += 1
                self.say(f"loop error (continuing): {exc!r}")
                time.sleep(0.2)
            time.sleep(self.cfg.poll_interval)
        self.shutdown()

    def shutdown(self):
        self.say("shutting down - cancelling open orders")
        if not self.paper:
            try:
                self.client.cancel_all(self.cfg.ticker)
            except RitError:
                try:
                    for o in self.client.orders("OPEN"):
                        if o.get("ticker") == self.cfg.ticker:
                            self.client.cancel(o["order_id"])
                except RitError:
                    pass
        self.status(force=True)
        c, elapsed = self.c, time.monotonic() - self.started
        print("=" * 78)
        print(f"loops           : {c['loops']:,}  API calls {self.client.calls:,} "
              f"({self.client.calls / max(elapsed, 1):.1f}/s)")
        print(f"orders sent     : {c['orders']:,}   cancels {c['cancels']:,}   rejected "
              f"{c['rejects']:,}   throttled {c['throttled']:,}   errors {c['errors']:,}")
        print(f"stops           : {c['stops']:,}   crossed shares {c['crossed_shares']:,}")
        print(f"max |position|  : {self.max_abs_pos:,}  (limit {self.position_limit:,})")
        try:
            tr = self.client.trader()
            print(f"trader NLV      : {tr.get('nlv')}   fines {tr.get('total_fines')}")
        except RitError:
            pass
        print("=" * 78)
        if self._log_file:
            self._log_file.close()
            self._log_file = self._log = None


# ============================================================================
# Entry point
# ============================================================================

def preflight(cfg: Config) -> int:
    client = RitClient(cfg)
    try:
        case, trader = client.case(), client.trader()
        secs, limits = client.all_securities(), client.limits()
    except RitError as exc:
        print(f"FAILED: {exc}\n  tried {cfg.base_url}")
        print("  Check: RIT client running and logged in, API icon green, and the "
              "port/key match the API dialog in the RIT status bar.")
        return 1
    print(f"OK  connected to {cfg.base_url}")
    print(f"    trader : {trader.get('trader_id')}  NLV {trader.get('nlv')}")
    print(f"    case   : {case.get('name')}  status {case.get('status')}  "
          f"tick {case.get('tick')}/{case.get('ticks_per_period')}")
    tickers = sorted(s.get("ticker") for s in secs)
    print(f"    tickers: {tickers}")
    if cfg.ticker not in tickers:
        print(f"    !! {cfg.ticker} not in this case - re-run with --ticker <one of the above>")
        return 1
    s = next(x for x in secs if x.get("ticker") == cfg.ticker)
    print(f"    {cfg.ticker}: bid {s.get('bid')} / ask {s.get('ask')}  pos {s.get('position')}  "
          f"max_order {s.get('max_trade_size')}  fee {s.get('trading_fee')}  "
          f"rebate {s.get('limit_order_rebate')}  rate {s.get('api_orders_per_second')}/s  "
          f"decimals {s.get('quoted_decimals')}")
    for lim in limits:
        print(f"    limit '{lim.get('name')}': gross {lim.get('gross')}/{lim.get('gross_limit')}  "
              f"net {lim.get('net')}/{lim.get('net_limit')}")
    return 0


def main(argv=None) -> int:
    d = Config()
    p = argparse.ArgumentParser(description="RIT ALGO2 combined market maker",
                                formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--host", default=d.host)
    p.add_argument("--port", type=int, default=d.port, help="REST port from the RIT API icon")
    p.add_argument("--key", default=None, help="API key (or env RIT_API_KEY)")
    p.add_argument("--ticker", default=d.ticker)
    p.add_argument("--size", type=int, default=d.quote_size, help="shares per side when flat")
    p.add_argument("--max-side", type=int, default=d.max_side_size,
                   help="max shares on the inventory-reducing side")
    p.add_argument("--max-inv", type=int, default=d.max_inventory, help="soft inventory cap")
    p.add_argument("--skew-step", type=int, default=d.skew_step,
                   help="shares of inventory per 1 tick of skew")
    p.add_argument("--no-penny", action="store_true", help="join the best price, never improve it")
    p.add_argument("--no-keep-join", action="store_true",
                   help="always move resting orders to the exact target price")
    p.add_argument("--no-linear-size", action="store_true",
                   help="do not shrink the adding side with inventory")
    p.add_argument("--stop-loss", type=int, default=d.stop_loss_ticks,
                   help="stop-loss in ticks per share vs average cost (0 = off)")
    p.add_argument("--max-age", type=float, default=d.max_age,
                   help="seconds a one-sided position may live (0 = off)")
    p.add_argument("--stop-pause", type=float, default=d.stop_pause,
                   help="seconds same-direction entries pause after a stop (doubles)")
    p.add_argument("--no-trend-stop", action="store_true", help="turn the trend stop off")
    p.add_argument("--trend-ticks", type=int, default=d.trend_ticks,
                   help="back-off of the side trading against a trend (0 = filter off)")
    p.add_argument("--trend-window", type=int, default=d.trend_window)
    p.add_argument("--trend-threshold", type=int, default=d.trend_threshold)
    p.add_argument("--hard-flatten", type=int, default=d.hard_flatten,
                   help="emergency brake at this |position| (0 = off)")
    p.add_argument("--limit-buffer", type=int, default=d.limit_buffer)
    p.add_argument("--wind-down", type=int, default=d.wind_down_ticks)
    p.add_argument("--end-inv", type=int, default=d.end_inventory)
    p.add_argument("--reduce-only", type=int, default=d.reduce_only_ticks,
                   help="final ticks in which only the reducing side is quoted")
    p.add_argument("--flatten-market", action="store_true",
                   help="cross to flat in the final --flatten-ticks")
    p.add_argument("--flatten-ticks", type=int, default=d.flatten_ticks)
    p.add_argument("--interval", type=float, default=d.poll_interval, help="seconds between loops")
    p.add_argument("--keep-running", action="store_true", help="stay up for several heats")
    p.add_argument("--log", default=d.log_path)
    p.add_argument("--no-log", action="store_true")
    p.add_argument("--quiet", action="store_true")
    p.add_argument("--paper", action="store_true", help="compute quotes, send no orders")
    p.add_argument("--check", action="store_true", help="connection/case preflight, then exit")
    a = p.parse_args(argv)

    cfg = Config(host=a.host, port=a.port,
                 api_key=a.key or os.environ.get("RIT_API_KEY", ""), ticker=a.ticker,
                 quote_size=a.size, max_side_size=max(a.max_side, a.size),
                 max_inventory=a.max_inv, skew_step=a.skew_step, penny=not a.no_penny,
                 keep_join=not a.no_keep_join, linear_size=not a.no_linear_size,
                 stop_loss_ticks=a.stop_loss, max_age=a.max_age, stop_pause=a.stop_pause,
                 trend_stop=not a.no_trend_stop, trend_ticks=a.trend_ticks,
                 trend_window=a.trend_window, trend_threshold=a.trend_threshold,
                 hard_flatten=a.hard_flatten, limit_buffer=a.limit_buffer,
                 wind_down_ticks=a.wind_down, end_inventory=a.end_inv,
                 reduce_only_ticks=a.reduce_only, flatten_market=a.flatten_market,
                 flatten_ticks=a.flatten_ticks, poll_interval=a.interval,
                 keep_running=a.keep_running, log_path=None if a.no_log else a.log,
                 verbose=not a.quiet)
    if not cfg.api_key:
        print("No API key: pass --key YOURKEY or set RIT_API_KEY "
              "(the key is shown under the API icon in the RIT client).")
        return 1
    if a.check:
        return preflight(cfg)

    mm = MarketMaker(cfg, RitClient(cfg), paper=a.paper)

    def _halt(*_):
        print("\ninterrupt - cancelling orders and stopping", flush=True)
        mm.stop = True

    signal.signal(signal.SIGINT, _halt)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, _halt)
    try:
        mm.run()
    except Exception as exc:                          # noqa: BLE001
        print(f"FATAL: {exc!r}", file=sys.stderr)
        try:
            mm.shutdown()
        except Exception:
            pass
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
