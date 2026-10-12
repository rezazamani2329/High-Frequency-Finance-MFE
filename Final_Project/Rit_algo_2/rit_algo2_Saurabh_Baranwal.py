"""
RIT ALGO2 - Algorithmic Market Making
=====================================

Passive two-sided market maker for the single stock ALGO.

Economics of this case
----------------------
Read live from the RIT client (``python rit_mm.py --check`` prints them), not
from the brief, because the client is what actually charges you:

    limit_order_rebate = 0.015 / share  on every PASSIVE fill (our resting order hit)
    trading_fee        = 0.010 / share  on every ACTIVE fill  (our order crosses)

So a round trip bought on our bid and sold on our ask earns

    (ask - bid) + 2 x rebate  =  spread + 3c / share

Even a round trip at ZERO spread earns 3c, and one that loses a cent on price
still earns 2c.  Crossing the spread flips that around: it costs the spread
plus 1c.  Every design decision below follows from those two lines:

  1. Be the best passive price as often as possible.  Fill *volume* is the
     income; priority in the queue is what buys volume.  When the spread
     allows we improve the best competitor price by one tick ("pennying").
  2. Never cross the spread in normal operation.  Every quote is checked to
     be strictly passive against the competitor book before it is sent.
  3. Keep inventory near flat (brief rule 2).  Inventory is the one thing
     that can lose more than the rebates earn - a trending market walks all
     over a maker that keeps absorbing one side.  Two levers:
       * price skew: the side that would ADD to inventory backs off the top
         of book one tick at a time as inventory grows, so it stops getting
         filled, while the side that REDUCES inventory stays at the best
         passive price and is sized up;
       * size cap: the adding side is sized so that position + everything we
         have resting on that side can never exceed a soft inventory cap.
  4. Never breach the 25,000 share limit.  Hard invariant, checked before
     every order: |position| + all resting orders on the same side can never
     exceed limit - buffer, even if every resting order fills at once.
  5. Always have a bid and an ask in the market (brief rule 1).  Rather than
     the brief's "cancel the survivor and resubmit the pair" reset, quotes are
     re-centred continuously on the live book, which is the same idea done
     every loop instead of once per round trip.
  6. Respect the 5 orders/second limit.  A sliding-window limiter spends that
     budget where it matters: the inventory-reducing side first, and a stale
     quote is only pulled when there is budget to replace it (a stale quote
     still earns the rebate; an empty side earns nothing).

Run ``python rit_mm.py --help`` for usage, and see README_ALGO2.md.
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


# ----------------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------------

@dataclass
class Config:
    # --- connection -------------------------------------------------------
    host: str = "localhost"
    port: int = 49120                 # shown under the API icon in the RIT client
    api_key: str = "MRQX7PCH"         # shown under the API icon in the RIT client
    http_timeout: float = 2.0
    ticker: str = "ALGO"

    # --- quoting ----------------------------------------------------------
    # Shares we try to keep resting on each side when flat.  5,000 is the
    # per-order cap in ALGO2, so one full-size order per side.
    quote_size: int = 5000
    # The side that REDUCES inventory may be sized up to quote_size + |position|,
    # capped here.  Tested at 10,000: in a trend the extra size overshoots
    # through flat into the wrong-way position, so the default is one order.
    max_side_size: int = 5000
    # Soft inventory cap.  The adding side is sized so position + its resting
    # orders stay at or under this.  The hard limit is enforced separately.
    # Swept 5k/7.5k/10k/15k in simulation: 5k had the best average P&L AND by
    # far the best worst case, because trend losses scale with inventory while
    # rebate income barely depends on it.
    max_inventory: int = 5000
    # How far (in ticks) the adding side backs off the best price at full
    # inventory.  0..cap/3 -> at best, cap/3..2cap/3 -> one tick back, ...
    skew_ticks: int = 3
    # Improve the best competitor price by one tick when the spread allows.
    penny: bool = True
    # Never quote our own bid and ask closer than this many ticks (0 = off).
    min_width: int = 0
    # Never send an order smaller than this; it would waste rate budget.
    min_order: int = 100
    # Top a side up once what is left resting at the right price falls below
    # (1 - topup_frac) of the target size.
    topup_frac: float = 0.5
    # Width assumed when one side of the book is empty (ticks).
    default_spread_ticks: int = 10
    # A quote priced off a book older than this is re-checked against a fresh
    # read before it is sent, so a market that moved cannot turn our passive
    # order into one that crosses (and pays the fee instead of the rebate).
    max_book_age: float = 0.05
    # After an order request fails without a clear answer, send no new orders
    # for this long (seconds) - see _post().
    post_hold: float = 1.5
    # Go back to waiting after a case ends instead of exiting (several heats).
    keep_running: bool = False
    # Trend filter.  If the competitor mid has moved at least trend_threshold
    # ticks over the last trend_window case ticks, the side that would trade
    # AGAINST the move (selling into a rally, buying into a sell-off) backs off
    # trend_ticks.  0 disables it.
    trend_ticks: int = 2
    trend_window: int = 10
    trend_threshold: int = 4
    # Trend stop-loss.  Holding inventory AGAINST a trend is how a market
    # maker loses: in a rally the short never gets bought back passively,
    # because nobody sells.  While a trend is on and we hold the wrong side,
    # we cross to cut it (one order at most every cut_cooldown seconds)
    # rather than wait.  Crossing costs ~2c/share; a trend costs 1c/share
    # on the whole position for every tick it runs.
    trend_stop: bool = True
    cut_cooldown: float = 1.0
    # RIT's open-order list can lag a freshly sent order by a few hundred
    # milliseconds.  Orders we sent are counted as live for this long even if
    # the list does not show them yet, so they are never sent twice.
    pending_ttl: float = 2.0

    # --- risk -------------------------------------------------------------
    position_limit: int = 25000       # fallback; the real value comes from /limits
    limit_buffer: int = 500           # hard cap = limit - buffer
    max_order_size: int = 5000        # fallback; the real value comes from /securities

    # --- end of case ------------------------------------------------------
    # Positions are marked to market at the close, so holding inventory at the
    # bell costs nothing in expectation while crossing to flatten costs the
    # spread + 1c.  We therefore unwind passively: over the last
    # wind_down_ticks the soft cap shrinks to end_inventory, and over the last
    # reduce_only_ticks only the inventory-reducing side is quoted.
    wind_down_ticks: int = 20
    end_inventory: int = 3000
    reduce_only_ticks: int = 3
    # Optionally flatten with marketable orders over the final ticks.
    flatten_market: bool = False
    flatten_ticks: int = 2
    # Ticks we may pay through the touch when we are forced to cross (limit
    # breach or --flatten-market).  Keeps a forced trade from walking the book.
    cross_slip_ticks: int = 3

    # --- API pacing -------------------------------------------------------
    orders_per_second: float = 5.0    # fallback; real value from /securities
    rate_window: float = 1.05         # seconds; >1 so we never brush the limit
    poll_interval: float = 0.10       # seconds between loops
    case_poll_interval: float = 0.25
    limits_poll_interval: float = 5.0

    # --- output -----------------------------------------------------------
    status_every: float = 5.0         # seconds between status lines
    log_path: Optional[str] = "algo2_log.csv"
    verbose: bool = True

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}/v1"


# ----------------------------------------------------------------------------
# REST client
# ----------------------------------------------------------------------------

class RitError(RuntimeError):
    """Any API failure we could not recover from."""


class RateLimited(RitError):
    def __init__(self, wait: float):
        super().__init__(f"rate limited, wait {wait:.3f}s")
        self.wait = wait


class Rejected(RitError):
    """A 4xx other than 401/429: the request itself was refused."""

    def __init__(self, status: int, text: str):
        super().__init__(f"HTTP {status}: {text}")
        self.status = status


def _num(d: dict, key: str, default=0.0):
    v = d.get(key) if isinstance(d, dict) else None
    return default if v is None else v


class RitClient:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.session = requests.Session()
        self.session.headers.update({"X-API-Key": cfg.api_key})
        adapter = HTTPAdapter(pool_connections=4, pool_maxsize=4, max_retries=0)
        self.session.mount("http://", adapter)
        self.calls = 0

    def _request(self, method: str, path: str, params: Optional[dict] = None,
                 retries: int = 1):
        url = self.cfg.base_url + path
        last: Optional[Exception] = None
        for attempt in range(retries + 1):
            self.calls += 1
            try:
                resp = self.session.request(method, url, params=params,
                                            timeout=self.cfg.http_timeout)
            except requests.RequestException as exc:
                last = exc
                time.sleep(0.02 * (attempt + 1))
                continue
            if resp.status_code == 200:
                try:
                    return resp.json()
                except ValueError as exc:
                    raise RitError(f"{path}: malformed JSON") from exc
            if resp.status_code == 429:
                raise RateLimited(self._retry_after(resp))
            if resp.status_code == 401:
                raise RitError(
                    "401 Unauthorized - wrong API key, or the RIT client is not "
                    "logged in.  Click the API icon in the RIT status bar for "
                    "the current port and key.")
            if 400 <= resp.status_code < 500:
                raise Rejected(resp.status_code, resp.text[:200])
            last = RitError(f"{method} {path} -> {resp.status_code}")
            time.sleep(0.02 * (attempt + 1))
        raise RitError(f"{method} {path} failed: {last}")

    @staticmethod
    def _retry_after(resp: requests.Response) -> float:
        hdr = resp.headers.get("Retry-After")
        if hdr:
            try:
                return max(float(hdr), 0.0)
            except ValueError:
                pass
        try:
            return max(float(resp.json().get("wait", 0.2)), 0.0)
        except Exception:
            return 0.2

    # -- endpoints --------------------------------------------------------

    def case(self) -> dict:
        return self._request("GET", "/case")

    def trader(self) -> dict:
        return self._request("GET", "/trader")

    def limits(self) -> list:
        return self._request("GET", "/limits")

    def security(self, ticker: str) -> dict:
        res = self._request("GET", "/securities", {"ticker": ticker})
        if isinstance(res, list):
            for s in res:
                if s.get("ticker") == ticker:
                    return s
            raise RitError(f"ticker {ticker} not found in this case")
        return res

    def all_securities(self) -> list:
        return self._request("GET", "/securities")

    def book(self, ticker: str, limit: int = 40) -> dict:
        return self._request("GET", "/securities/book",
                             {"ticker": ticker, "limit": limit})

    def orders(self, status: str = "OPEN") -> list:
        return self._request("GET", "/orders", {"status": status})

    def post_limit(self, ticker: str, action: str, qty: int, price: str) -> dict:
        # Not idempotent: a timeout may mean the order landed.  Never retry;
        # the next /orders snapshot tells us the truth.
        return self._request("POST", "/orders",
                             {"ticker": ticker, "type": "LIMIT", "quantity": qty,
                              "action": action, "price": price}, retries=0)

    def cancel(self, order_id: int) -> dict:
        return self._request("DELETE", f"/orders/{order_id}", retries=0)

    def cancel_all(self, ticker: Optional[str] = None) -> dict:
        params = {"ticker": ticker} if ticker else {"all": 1}
        return self._request("POST", "/commands/cancel", params, retries=0)


# ----------------------------------------------------------------------------
# Order rate limiter
# ----------------------------------------------------------------------------

class OrderLimiter:
    """Sliding-window limiter for order submissions.

    A token bucket lets through capacity + rate x window in any window, which
    breaks a hard "N per second" rule on bursts.  A sliding window of the last
    N timestamps cannot.  The window is slightly longer than a second, and
    grows a little every time the server still says 429, so our clock and the
    server's never disagree for long.
    """

    def __init__(self, per_sec: float, window: float):
        self.per_sec = max(int(per_sec), 1)
        self.window = window
        self.stamps: Deque[float] = collections.deque()
        self.blocked_until = 0.0
        self.count_cancels = False        # switched on if a DELETE ever gets a 429

    def set_rate(self, per_sec: float) -> None:
        if per_sec and per_sec > 0:
            self.per_sec = max(int(per_sec), 1)

    def _trim(self, now: float) -> None:
        while self.stamps and self.stamps[0] <= now - self.window:
            self.stamps.popleft()

    def remaining(self) -> int:
        now = time.monotonic()
        if now < self.blocked_until:
            return 0
        self._trim(now)
        return max(self.per_sec - len(self.stamps), 0)

    def record(self) -> None:
        self.stamps.append(time.monotonic())

    def block(self, seconds: float) -> None:
        self.blocked_until = max(self.blocked_until,
                                 time.monotonic() + min(max(seconds, 0.05), 2.0))
        self.window = min(self.window + 0.05, 1.5)


# ----------------------------------------------------------------------------
# State
# ----------------------------------------------------------------------------

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
    last: Optional[int]
    comp_bid: Optional[int]      # best competitor prices, OUR orders excluded
    comp_ask: Optional[int]
    mine: Dict[str, List[MyOrder]]
    stamp: float = 0.0           # monotonic time the book was read


@dataclass
class Quote:
    price: int          # integer ticks
    target: int         # shares we want resting at that price


# ----------------------------------------------------------------------------
# The market maker
# ----------------------------------------------------------------------------

class MarketMaker:

    def __init__(self, cfg: Config, client: RitClient, paper: bool = False):
        self.cfg = cfg
        self.client = client
        self.paper = paper
        self.stop = False

        # static-ish security parameters (refreshed from /securities)
        self.decimals = 2
        self.max_order = cfg.max_order_size
        self.min_price_t: Optional[int] = None
        self.max_price_t: Optional[int] = None
        self.fee = 0.0
        self.rebate = 0.0
        self.limit_units = 1.0
        self.limit_names: List[str] = []
        self.position_limit = cfg.position_limit

        self.trader_id: Optional[str] = None
        self.recent_ids: Deque[int] = collections.deque(maxlen=1000)
        self.own_ids: set = set()
        self.limiter = OrderLimiter(cfg.orders_per_second, cfg.rate_window)

        self.ticks_per_period = 300
        self.tick = 0
        self.mid_hist: Deque = collections.deque(maxlen=4000)   # (tick, mid x2)
        self.last_snapshot: Optional[Snapshot] = None
        self.last_plan: Dict[str, Optional[Quote]] = {BUY: None, SELL: None}

        # stats
        self.posts = 0
        self.cancels = 0
        self.rejects = 0
        self.throttled = 0
        self.active_shares = 0
        self.errors = 0
        self.loops = 0
        self.max_abs_pos = 0
        self.started = time.monotonic()
        self._last_status = 0.0
        self._last_limits = 0.0

        self._post_hold_until = 0.0
        self._cut_until = 0.0
        self.cuts = 0
        # order_id -> (order as sent, monotonic time sent); see pending_ttl
        self.pending: Dict[int, tuple] = {}
        self._log_file = None
        self._log = None
        if cfg.log_path:
            new = not os.path.exists(cfg.log_path)
            try:
                self._log_file = open(cfg.log_path, "a", newline="", encoding="utf-8")
            except OSError as exc:
                # Typically the CSV is open in Excel, which locks it on Windows.
                # A missing log must never stop us trading.
                print(f"WARNING: cannot write {cfg.log_path} ({exc}); "
                      f"continuing without the CSV log", flush=True)
                new = False
            if self._log_file:
                self._log = csv.writer(self._log_file)
            if self._log and new:
                self._log.writerow(["wall_time", "tick", "position", "comp_bid",
                                    "comp_ask", "my_bid", "my_bid_qty", "my_ask",
                                    "my_ask_qty", "nlv", "posts", "cancels",
                                    "note"])

    # -- helpers ------------------------------------------------------------

    def say(self, msg: str) -> None:
        if self.cfg.verbose:
            print(f"[{time.monotonic() - self.started:7.2f}s t{self.tick:>3}] {msg}",
                  flush=True)

    def to_ticks(self, price: float) -> int:
        return int(round(float(price) * 10 ** self.decimals))

    def to_price(self, ticks: int) -> str:
        return f"{ticks / 10 ** self.decimals:.{self.decimals}f}"

    def fmt(self, ticks: Optional[int]) -> str:
        return "-" if ticks is None else self.to_price(ticks)

    @property
    def hard_cap(self) -> int:
        """Shares |position| + same-side resting orders may never exceed."""
        return max(self.position_limit - self.cfg.limit_buffer, 0)

    # -- static data --------------------------------------------------------

    def load_security(self, sec: dict) -> None:
        self.decimals = int(_num(sec, "quoted_decimals", 2))
        mts = int(_num(sec, "max_trade_size", 0))
        self.max_order = min(mts, self.cfg.max_order_size) if mts > 0 \
            else self.cfg.max_order_size
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
        self.limit_names = names
        self.limit_units = units

    def load_limits(self, payload: list) -> None:
        """Position limit in SHARES, the tightest of every limit ALGO counts in."""
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

    def refresh_limits(self) -> None:
        try:
            self.load_limits(self.client.limits())
        except RitError as exc:
            self.say(f"limits refresh failed: {exc}")
        self._last_limits = time.monotonic()

    # -- market data ----------------------------------------------------------

    def snapshot(self) -> Optional[Snapshot]:
        """Our open orders, then position, then the book - in that order.

        The order matters.  A fill that lands between two reads is then counted
        in the position AND still in the resting size, so the exposure we see
        can only ever be over-stated, never under-stated.
        """
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
            self.errors += 1
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
            price = o.get("price")
            if price is None or o.get("action") not in (BUY, SELL):
                continue
            rem = int(_num(o, "quantity", 0)) - int(_num(o, "quantity_filled", 0))
            if rem > 0:
                mine[o["action"]].append(
                    MyOrder(oid, o["action"], self.to_ticks(price), rem))

        # Orders we sent that the (lagging) list does not show yet are still
        # ours and still live - count them, or we would send them again.
        now = time.monotonic()
        listed = {o.get("order_id") for o in orders or []}
        for oid, (mo, sent) in list(self.pending.items()):
            if oid in listed or now - sent > self.cfg.pending_ttl:
                del self.pending[oid]
            else:
                mine[mo.action].append(MyOrder(mo.order_id, mo.action,
                                               mo.price, mo.remaining))

        snap = self._make_snapshot(sec, book, mine, stamp)
        self.last_snapshot = snap
        return snap

    def _make_snapshot(self, sec: dict, book: dict, mine: Dict[str, List[MyOrder]],
                       stamp: float) -> Snapshot:
        own_ids = self.own_ids | set(self.recent_ids)

        def is_own(e: dict) -> bool:
            if e.get("order_id") in own_ids:
                return True
            return self.trader_id is not None and e.get("trader_id") == self.trader_id

        comp_bid = comp_ask = None
        for key, side in (("bids", BUY), ("asks", SELL)):
            entries = book.get(key) if isinstance(book, dict) else None
            if entries is None and isinstance(book, dict):   # older key names
                entries = book.get("bid" if side == BUY else "ask")
            for e in entries or []:
                if is_own(e) or e.get("price") is None:
                    continue
                if int(_num(e, "quantity", 0)) - int(_num(e, "quantity_filled", 0)) <= 0:
                    continue
                p = self.to_ticks(e["price"])
                if side == BUY:
                    comp_bid = p if comp_bid is None else max(comp_bid, p)
                else:
                    comp_ask = p if comp_ask is None else min(comp_ask, p)

        last = _num(sec, "last", 0.0)
        pos = int(round(float(_num(sec, "position", 0.0))))
        self.max_abs_pos = max(self.max_abs_pos, abs(pos))
        return Snapshot(tick=self.tick, pos=pos,
                        last=self.to_ticks(last) if last and last > 0 else None,
                        comp_bid=comp_bid, comp_ask=comp_ask, mine=mine, stamp=stamp)

    def refresh_market(self, live: Dict[str, List[MyOrder]]) -> Optional[Snapshot]:
        """Position and book re-read, keeping our own order list as tracked."""
        t = self.cfg.ticker
        try:
            sec = self.client.security(t)
            stamp = time.monotonic()
            book = self.client.book(t)
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

    # -- quote model ----------------------------------------------------------

    def note_mid(self, s: Snapshot) -> None:
        if s.comp_bid is not None and s.comp_ask is not None:
            self.mid_hist.append((s.tick, s.comp_bid + s.comp_ask))

    def trend(self) -> int:
        """+1 rally, -1 sell-off, 0 neither (or the filter is off)."""
        if self.cfg.trend_ticks <= 0 or not self.mid_hist:
            return 0
        now_tick, now_mid = self.mid_hist[-1]
        past = None
        for t, m in self.mid_hist:
            if t >= now_tick - self.cfg.trend_window:
                past = m
                break
        if past is None or self.mid_hist[0][0] > now_tick - self.cfg.trend_window:
            return 0                              # not enough history yet
        move = now_mid - past                     # in half-ticks (mid x 2)
        if move >= 2 * self.cfg.trend_threshold:
            return 1
        if move <= -2 * self.cfg.trend_threshold:
            return -1
        return 0

    def soft_cap(self, tick: int) -> int:
        cap = min(self.cfg.max_inventory, self.hard_cap)
        if self.ticks_per_period - tick <= self.cfg.wind_down_ticks:
            cap = min(cap, self.cfg.end_inventory)
        return max(cap, 0)

    def plan(self, s: Snapshot) -> Dict[str, Optional[Quote]]:
        """Where each side should be, and how much should rest there."""
        none = {BUY: None, SELL: None}
        cb, ca = s.comp_bid, s.comp_ask
        width = max(self.cfg.default_spread_ticks, 2)
        if cb is None and ca is None:
            if s.last is None:
                return none                      # nothing to anchor a quote to
            cb, ca = s.last - width // 2, s.last + (width - width // 2)
        elif cb is None:
            cb = ca - width
        elif ca is None:
            ca = cb + width
        spread = ca - cb
        if spread < 1:
            return none                          # locked/crossed: sit it out

        pos = s.pos
        # Best strictly-passive prices.  Improving by one tick puts us first
        # in the queue, which is where the fills (and the rebates) are.  With
        # a two-tick spread only one side can improve without our bid and ask
        # meeting; it goes to the side that reduces inventory.
        bid, ask = cb, ca
        if self.cfg.penny:
            if spread >= 3:
                bid, ask = cb + 1, ca - 1
            elif spread == 2:
                if pos > 0:
                    ask = ca - 1
                else:
                    bid = cb + 1

        cap = self.soft_cap(s.tick)
        inv = abs(pos)
        back = (inv * self.cfg.skew_ticks // cap) if cap > 0 else self.cfg.skew_ticks
        if pos > 0:
            bid -= back
        elif pos < 0:
            ask += back

        # Trend: never ADD inventory against it (no short selling into a
        # rally, no buying into a sell-off), and let inventory that is WITH
        # it ride a little longer by backing its exit off trend_ticks.
        trend = self.trend()
        no_bid = no_ask = False
        if trend > 0:
            if pos > 0:
                ask += self.cfg.trend_ticks
            else:
                no_ask = True
        elif trend < 0:
            if pos < 0:
                bid -= self.cfg.trend_ticks
            else:
                no_bid = True

        # Sizes.  The adding side never takes position + resting past the soft
        # cap; the reducing side is sized to be able to take us back through
        # flat in one fill.
        q = self.cfg.quote_size
        if pos > 0:
            bid_tgt = min(q, cap - pos)
            ask_tgt = min(self.cfg.max_side_size, q + pos)
        elif pos < 0:
            bid_tgt = min(self.cfg.max_side_size, q - pos)
            ask_tgt = min(q, cap + pos)
        else:
            bid_tgt = ask_tgt = min(q, cap)
        if no_bid:
            bid_tgt = 0
        if no_ask:
            ask_tgt = 0

        left = self.ticks_per_period - s.tick
        if left <= self.cfg.reduce_only_ticks:
            if pos >= 0:
                bid_tgt = 0
            if pos <= 0:
                ask_tgt = 0
            # Near the bell the reducing side only needs to cover what we hold.
            if pos > 0:
                ask_tgt = min(ask_tgt, pos)
            elif pos < 0:
                bid_tgt = min(bid_tgt, -pos)

        # Minimum width between our own bid and ask.  When competing makers
        # collapse the spread, the fills left at the very top are mostly the
        # ones about to be run over; standing back lets them take those.  The
        # side that would add to inventory steps back first.
        gap = self.cfg.min_width - (ask - bid)
        if gap > 0:
            if pos > 0:
                bid -= gap
            elif pos < 0:
                ask += gap
            else:
                bid -= gap // 2
                ask += gap - gap // 2

        # Guards: strictly passive, inside the security's price band, and our
        # own bid below our own ask.
        bid = min(bid, ca - 1)
        ask = max(ask, cb + 1)
        if bid >= ask:
            if pos > 0:
                bid = ask - 1
            else:
                ask = bid + 1
        if self.min_price_t is not None and bid < self.min_price_t:
            bid_tgt = 0
        if bid < 1:
            bid_tgt = 0
        if self.max_price_t is not None and ask > self.max_price_t:
            ask_tgt = 0

        return {
            BUY: Quote(bid, bid_tgt) if bid_tgt >= self.cfg.min_order else None,
            SELL: Quote(ask, ask_tgt) if ask_tgt >= self.cfg.min_order else None,
        }

    # -- execution ------------------------------------------------------------

    def _side_order(self, pos: int) -> List[str]:
        """Inventory-reducing side first: it gets the rate budget if short."""
        return [SELL, BUY] if pos > 0 else [BUY, SELL]

    def _cancel(self, o: MyOrder) -> Optional[bool]:
        """True = gone, False = still live, None = unknown (treat as live)."""
        gone = self._cancel_raw(o)
        if gone:
            self.pending.pop(o.order_id, None)
        return gone

    def _cancel_raw(self, o: MyOrder) -> Optional[bool]:
        if self.limiter.count_cancels:
            if self.limiter.remaining() <= 0:
                return False
            self.limiter.record()
        try:
            res = self.client.cancel(o.order_id)
        except RateLimited as exc:
            # A 429 on a cancel means cancels count against the order budget.
            self.limiter.count_cancels = True
            self.limiter.block(exc.wait)
            self.throttled += 1
            return False
        except Rejected:
            # Already filled or already cancelled: either way it is gone and any
            # fill will show up in the position we re-read next.
            return True
        except RitError as exc:
            self.errors += 1
            self.say(f"cancel {o.order_id} failed: {exc}")
            return None
        self.cancels += 1
        if isinstance(res, dict) and res.get("success") is False:
            return True        # server says there was nothing to cancel
        return True

    def _post(self, action: str, qty: int, price_t: int) -> Optional[MyOrder]:
        if self.limiter.remaining() <= 0 or time.monotonic() < self._post_hold_until:
            return None
        self.limiter.record()
        try:
            o = self.client.post_limit(self.cfg.ticker, action, qty,
                                       self.to_price(price_t))
        except RateLimited as exc:
            self.limiter.block(exc.wait)
            self.throttled += 1
            return None
        except Rejected as exc:
            self.rejects += 1
            self.say(f"order rejected ({action} {qty} @ {self.to_price(price_t)}): {exc}")
            return None
        except RitError as exc:
            # Timeout or server error: the order may or may not exist, and on a
            # lagging server it can land AFTER our next snapshot.  Hold new
            # orders until it has had time to show up, so an order we cannot
            # see yet can never be duplicated.
            self.errors += 1
            self._post_hold_until = time.monotonic() + self.cfg.post_hold
            self.say(f"order post failed ({action} {qty}): {exc}")
            raise
        self.posts += 1
        oid = o.get("order_id")
        if oid is not None:
            self.recent_ids.append(oid)
        filled = int(_num(o, "quantity_filled", 0))
        if filled > 0:
            # The book moved between our read and our order: part of it crossed.
            self.active_shares += filled
        rem = int(_num(o, "quantity", qty)) - filled
        if o.get("status", "OPEN") == "OPEN" and rem > 0:
            placed = MyOrder(oid, action, price_t, rem)
            if oid is not None:
                self.pending[oid] = (placed, time.monotonic())
            return placed
        return MyOrder(oid, action, price_t, 0)

    def step(self) -> None:
        s = self.snapshot()
        if s is None:
            return
        self.note_mid(s)

        if self.paper:
            self.last_plan = self.plan(s)
            return
        if abs(s.pos) > self.position_limit:
            self.emergency(s)
            return
        left = self.ticks_per_period - s.tick
        if self.cfg.flatten_market and left <= self.cfg.flatten_ticks and s.pos != 0:
            self.flatten(s)
            return
        tr = self.trend()
        if (self.cfg.trend_stop and tr != 0 and s.pos * tr < 0
                and abs(s.pos) >= self.cfg.min_order
                and time.monotonic() >= self._cut_until):
            self.cut_against_trend(s, tr)
            return

        plan = self.plan(s)
        self.last_plan = plan

        live ={BUY: list(s.mine[BUY]), SELL: list(s.mine[SELL])}
        order = self._side_order(s.pos)

        # ---- phase 1: decide what to pull --------------------------------
        budget = self.limiter.remaining()
        cancel: List[MyOrder] = []
        for side in order:
            q = plan[side]
            orders = live[side]
            if q is None:
                cancel.extend(orders)                       # side not wanted
                continue
            off = [o for o in orders if o.price != q.price]
            at = [o for o in orders if o.price == q.price]

            # Hard limit: even if everything resting on this side fills,
            # |position| must stay under the cap.  Pull newest first.
            signed = 1 if side == BUY else -1
            exposure = signed * s.pos + sum(o.remaining for o in orders)
            urgent: List[MyOrder] = []
            for o in off + sorted(at, key=lambda x: -(x.order_id or 0)):
                if exposure <= self.hard_cap:
                    break
                urgent.append(o)
                exposure -= o.remaining

            # Size cap on EITHER side: more resting than this side's target
            # (plus half an order of slack, so partial fills do not cause
            # churn) is exposure we did not intend - e.g. a reducing order that
            # would carry us straight through flat and out the other side.
            # Pull the stale-priced ones first, then the newest.
            total = sum(o.remaining for o in orders if o not in urgent)
            excess = total - (q.target + self.cfg.quote_size // 2)
            for o in off + sorted(at, key=lambda x: -(x.order_id or 0)):
                if excess <= 0:
                    break
                if o not in urgent:
                    urgent.append(o)
                    excess -= o.remaining
            cancel.extend(urgent)

            # Repricing is only worth it if we can replace the quote now: a
            # stale quote still earns the rebate, an empty side earns nothing.
            reprice = [o for o in off if o not in urgent]
            if reprice:
                need = 1 + (len(reprice) if self.limiter.count_cancels else 0)
                if budget >= need:
                    cancel.extend(reprice)
                    budget -= need
            elif not at:
                budget -= 1                                 # reserve for a post

        # ---- phase 2: pull ----------------------------------------------
        cancelled_any = False
        for o in cancel:
            gone = self._cancel(o)
            if gone:
                cancelled_any = True
                live[o.action] = [x for x in live[o.action] if x.order_id != o.order_id]

        if not any(plan[side] is not None for side in order):
            return
        if cancelled_any or time.monotonic() - s.stamp > self.cfg.max_book_age:
            # Anything that filled before a cancel landed is in the position
            # now, and the book may have moved while we worked: re-read both
            # and re-plan before sizing and pricing new orders.
            s2 = self.refresh_market(live)
            if s2 is None:
                return
            s = s2
            if abs(s.pos) > self.position_limit:
                return
            plan = self.plan(s)
            self.last_plan = plan
        pos = s.pos

        # ---- phase 3: (re)quote -----------------------------------------
        for side in order:
            q = plan[side]
            if q is None:
                continue
            side_total = sum(o.remaining for o in live[side])
            at_total = sum(o.remaining for o in live[side] if o.price == q.price)
            deficit = q.target - at_total
            if deficit < max(self.cfg.min_order, int(self.cfg.topup_frac * q.target)):
                continue
            signed = 1 if side == BUY else -1
            room = self.hard_cap - signed * pos - side_total
            # The target caps everything resting on this side, at any price -
            # an old quote whose cancel did not go through still counts, so a
            # failed cancel can never let orders pile up on one side.
            qty = min(deficit, q.target - side_total, room, self.max_order)
            if qty < self.cfg.min_order:
                continue
            # Strictly passive against the book we just read.
            if side == BUY and s.comp_ask is not None and q.price >= s.comp_ask:
                continue
            if side == SELL and s.comp_bid is not None and q.price <= s.comp_bid:
                continue
            # Never trade with ourselves: our new bid must sit below every ask
            # we still have resting, and vice versa.
            other = live[SELL if side == BUY else BUY]
            if side == BUY and other and q.price >= min(o.price for o in other):
                continue
            if side == SELL and other and q.price <= max(o.price for o in other):
                continue
            try:
                placed = self._post(side, qty, q.price)
            except RitError:
                return                   # unknown outcome - re-read before more
            if placed is None:
                if self.limiter.remaining() <= 0:
                    break
                continue
            if placed.remaining > 0:
                live[side].append(placed)

    # -- forced crossing ------------------------------------------------------

    def _cross(self, s: Snapshot, action: str, qty: int, note: str) -> None:
        ref = s.comp_ask if action == BUY else s.comp_bid
        if ref is None:
            ref = s.last
        if ref is None or qty <= 0 or self.paper:
            return
        slip = self.cfg.cross_slip_ticks
        price = ref + slip if action == BUY else max(ref - slip, 1)
        self.say(f"{note}: {action} {qty:,} @ {self.to_price(price)} (pos {s.pos:+,})")
        try:
            o = self._post(action, qty, price)
        except RitError:
            return
        if o is not None and o.remaining > 0:
            # Anything that did not fill immediately must not rest - it would
            # be an unhedged order at a price we chose to be aggressive at.
            self._cancel(o)

    def _pull_all(self, s: Snapshot) -> None:
        for side in (BUY, SELL):
            for o in s.mine[side]:
                self._cancel(o)

    def emergency(self, s: Snapshot) -> None:
        """Over the position limit: fines accrue, so get back under it now."""
        self._pull_all(s)
        pos = self.refresh_position()
        if pos is None or abs(pos) <= self.position_limit:
            return
        s.pos = pos
        qty = min(abs(pos) - self.hard_cap, self.max_order)
        self._cross(s, SELL if pos > 0 else BUY, qty, "LIMIT BREACH")

    def cut_against_trend(self, s: Snapshot, trend: int) -> None:
        """Holding the wrong side of a trend: pull our orders, then cross to
        cut the position, one max-size order at a time."""
        self._cut_until = time.monotonic() + self.cfg.cut_cooldown
        self._pull_all(s)
        pos = self.refresh_position()
        if pos is None or pos * trend >= 0 or abs(pos) < self.cfg.min_order:
            return
        s.pos = pos
        self.cuts += 1
        self._cross(s, BUY if pos < 0 else SELL, min(abs(pos), self.max_order),
                    f"TREND {'UP' if trend > 0 else 'DOWN'} - cutting")

    def flatten(self, s: Snapshot) -> None:
        self._pull_all(s)
        pos = self.refresh_position()
        if pos is None or pos == 0:
            return
        s.pos = pos
        self._cross(s, SELL if pos > 0 else BUY, min(abs(pos), self.max_order),
                    "FLATTEN")

    # -- reporting ------------------------------------------------------------

    def status(self, force: bool = False) -> None:
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
        self.say(f"pos {s.pos:+7,}  mkt {self.fmt(s.comp_bid)}/{self.fmt(s.comp_ask)}  "
                 f"quote {self.fmt(bq.price if bq else None)} x {bid_rest:,} / "
                 f"{self.fmt(aq.price if aq else None)} x {ask_rest:,}  "
                 f"NLV {nlv if nlv is None else f'{nlv:,.2f}'}  "
                 f"orders {self.posts} cxl {self.cancels} 429 {self.throttled}")
        if self._log:
            try:
                self._log.writerow([f"{time.time():.3f}", s.tick, s.pos,
                                    self.fmt(s.comp_bid), self.fmt(s.comp_ask),
                                    self.fmt(bq.price if bq else None), bid_rest,
                                    self.fmt(aq.price if aq else None), ask_rest,
                                    nlv, self.posts, self.cancels,
                                    "paper" if self.paper else ""])
                self._log_file.flush()
            except OSError:
                self._log = None

    # -- lifecycle ------------------------------------------------------------

    def connect(self) -> bool:
        while not self.stop:
            try:
                tr = self.client.trader()
                self.trader_id = tr.get("trader_id")
                self.load_security(self.client.security(self.cfg.ticker))
                self.refresh_limits()
                return True
            except RitError as exc:
                self.say(f"waiting for RIT client: {exc}")
                time.sleep(2.0)
        return False

    def print_setup(self) -> None:
        print("-" * 78)
        print(f"ticker          : {self.cfg.ticker}   trader {self.trader_id}")
        print(f"rebate / fee    : {self.rebate} passive / {self.fee} active per share")
        print(f"max order       : {self.max_order:,}   "
              f"order rate {self.limiter.per_sec}/s")
        print(f"position limit  : {self.position_limit:,}  "
              f"(hard cap {self.hard_cap:,}, soft inventory cap "
              f"{min(self.cfg.max_inventory, self.hard_cap):,})")
        print(f"quote size      : {self.cfg.quote_size:,} per side  "
              f"(reducing side up to {self.cfg.max_side_size:,})")
        print(f"pennying        : {'on' if self.cfg.penny else 'off'}   "
              f"skew {self.cfg.skew_ticks} ticks at full inventory")
        print(f"mode            : {'PAPER (no orders sent)' if self.paper else 'LIVE'}")
        if self.rebate <= 0:
            print("  NOTE: no passive rebate reported - check this is the ALGO2 case")
        print("-" * 78)

    def run(self) -> None:
        if not self.connect():
            return
        self.print_setup()
        was_active = False
        last_status = None
        case: dict = {}
        case_stamp = 0.0
        waiting_note = 0.0

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
                        break                               # case is over
                    # Another heat will follow: report, reset, wait again.
                    self.say("case over - waiting for the next run")
                    self.status(force=True)
                    was_active = False
                    self.mid_hist.clear()
                    self.last_snapshot = None
                if now - waiting_note > 10.0:
                    self.say("waiting for the case to start (Ctrl-C to quit) ...")
                    waiting_note = now
                time.sleep(0.25)
                continue

            if not was_active:
                was_active = True
                self.refresh_limits()
                self.say("case ACTIVE - quoting")

            self.loops += 1
            if now - self._last_limits >= self.cfg.limits_poll_interval:
                self.refresh_limits()
            try:
                self.step()
                self.status()
            except RitError as exc:
                self.errors += 1
                self.say(f"loop error: {exc}")
                time.sleep(0.2)
            except Exception as exc:                 # noqa: BLE001
                # One malformed response must not end the case for us: log it,
                # re-read everything next loop, carry on.
                self.errors += 1
                self.say(f"unexpected error (continuing): {exc!r}")
                time.sleep(0.2)
            time.sleep(self.cfg.poll_interval)

        self.shutdown()

    def shutdown(self) -> None:
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
        elapsed = time.monotonic() - self.started
        print("=" * 78)
        print(f"loops           : {self.loops:,}  API calls {self.client.calls:,} "
              f"({self.client.calls / max(elapsed, 1):.1f}/s)")
        print(f"orders sent     : {self.posts:,}   cancels {self.cancels:,}   "
              f"rejected {self.rejects:,}   throttled {self.throttled:,}   "
              f"errors {self.errors:,}")
        print(f"max |position|  : {self.max_abs_pos:,}  (limit {self.position_limit:,})")
        if self.active_shares:
            print(f"crossed shares  : {self.active_shares:,} (paid the active fee)")
        try:
            tr = self.client.trader()
            print(f"trader NLV      : {tr.get('nlv')}   fines {tr.get('total_fines')}")
        except RitError:
            pass
        print("=" * 78)
        if self._log_file:
            self._log_file.close()
            self._log_file = None
            self._log = None


# ----------------------------------------------------------------------------
# Entry point
# ----------------------------------------------------------------------------

def preflight(cfg: Config) -> int:
    client = RitClient(cfg)
    try:
        case = client.case()
        trader = client.trader()
        secs = client.all_securities()
        limits = client.limits()
    except RitError as exc:
        print(f"FAILED: {exc}")
        print(f"  tried {cfg.base_url}")
        print("  Check: RIT client running and logged in, API icon green, and the "
              "port/key match the API dialog in the RIT status bar.")
        return 1
    print(f"OK  connected to {cfg.base_url}")
    print(f"    trader : {trader.get('trader_id')}  NLV {trader.get('nlv')}")
    print(f"    case   : {case.get('name')}  status {case.get('status')}  "
          f"tick {case.get('tick')}/{case.get('ticks_per_period')}  "
          f"limits enforced: {case.get('is_enforce_trading_limits')}")
    tickers = sorted(s.get("ticker") for s in secs)
    print(f"    tickers: {tickers}")
    if cfg.ticker not in tickers:
        print(f"    !! {cfg.ticker} not in this case - re-run with --ticker <one of the above>")
        return 1
    s = next(x for x in secs if x.get("ticker") == cfg.ticker)
    print(f"    {cfg.ticker}: bid {s.get('bid')} / ask {s.get('ask')}  pos {s.get('position')}  "
          f"max_order {s.get('max_trade_size')}  fee {s.get('trading_fee')}  "
          f"rebate {s.get('limit_order_rebate')}  rate {s.get('api_orders_per_second')}/s  "
          f"delay {s.get('execution_delay_ms')}ms  decimals {s.get('quoted_decimals')}")
    for lim in limits:
        print(f"    limit '{lim.get('name')}': gross {lim.get('gross')}/{lim.get('gross_limit')}  "
              f"net {lim.get('net')}/{lim.get('net_limit')}  "
              f"fines {lim.get('gross_fine')}/{lim.get('net_fine')}")
    return 0


def build_config(args: argparse.Namespace) -> Config:
    cfg = Config()
    cfg.host = args.host
    cfg.port = args.port
    cfg.api_key = args.key or os.environ.get("RIT_API_KEY") or cfg.api_key
    cfg.ticker = args.ticker
    cfg.quote_size = args.size
    cfg.max_side_size = max(args.max_side, args.size)
    cfg.max_inventory = args.max_inv
    cfg.skew_ticks = args.skew
    cfg.penny = not args.no_penny
    cfg.limit_buffer = args.limit_buffer
    cfg.wind_down_ticks = args.wind_down
    cfg.end_inventory = args.end_inv
    cfg.flatten_market = args.flatten_market
    cfg.poll_interval = args.interval
    cfg.trend_ticks = args.trend_ticks
    cfg.trend_window = args.trend_window
    cfg.trend_threshold = args.trend_threshold
    cfg.keep_running = args.keep_running
    cfg.min_width = args.min_width
    cfg.log_path = None if args.no_log else args.log
    cfg.verbose = not args.quiet
    return cfg


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        description="RIT ALGO2 passive market maker",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=Config.port,
                   help="REST port from the API icon in the RIT status bar")
    p.add_argument("--key", default=None,
                   help="API key (or set the RIT_API_KEY environment variable)")
    p.add_argument("--ticker", default=Config.ticker)
    p.add_argument("--size", type=int, default=Config.quote_size,
                   help="shares quoted per side when flat")
    p.add_argument("--max-side", type=int, default=Config.max_side_size,
                   help="max shares resting on the inventory-reducing side")
    p.add_argument("--max-inv", type=int, default=Config.max_inventory,
                   help="soft inventory cap (shares)")
    p.add_argument("--skew", type=int, default=Config.skew_ticks,
                   help="ticks the adding side backs off at full inventory")
    p.add_argument("--no-penny", action="store_true",
                   help="join the best price instead of improving it by a tick")
    p.add_argument("--limit-buffer", type=int, default=Config.limit_buffer,
                   help="shares kept clear of the position limit")
    p.add_argument("--wind-down", type=int, default=Config.wind_down_ticks,
                   help="final ticks over which inventory is cut to --end-inv")
    p.add_argument("--end-inv", type=int, default=Config.end_inventory,
                   help="soft inventory cap during the wind-down")
    p.add_argument("--flatten-market", action="store_true",
                   help="cross the spread to flatten in the final ticks")
    p.add_argument("--trend-ticks", type=int, default=Config.trend_ticks,
                   help="ticks the side trading against a trend backs off (0 = off)")
    p.add_argument("--trend-window", type=int, default=Config.trend_window,
                   help="case ticks over which the trend is measured")
    p.add_argument("--trend-threshold", type=int, default=Config.trend_threshold,
                   help="mid move in ticks (cents) that counts as a trend")
    p.add_argument("--min-width", type=int, default=Config.min_width,
                   help="never quote our bid and ask closer than this many ticks (0 = off)")
    p.add_argument("--keep-running", action="store_true",
                   help="after a case ends, wait for the next one instead of exiting")
    p.add_argument("--interval", type=float, default=Config.poll_interval,
                   help="seconds between loops")
    p.add_argument("--log", default=Config.log_path, help="CSV status log path")
    p.add_argument("--no-log", action="store_true")
    p.add_argument("--quiet", action="store_true")
    p.add_argument("--paper", action="store_true",
                   help="compute and print quotes but send no orders")
    p.add_argument("--check", action="store_true",
                   help="connectivity/config preflight, then exit")
    args = p.parse_args(argv)
    cfg = build_config(args)

    if args.check:
        return preflight(cfg)

    mm = MarketMaker(cfg, RitClient(cfg), paper=args.paper)

    def _sigint(_sig, _frm):
        print("\ninterrupt - cancelling orders and stopping", flush=True)
        mm.stop = True

    signal.signal(signal.SIGINT, _sigint)
    if hasattr(signal, "SIGBREAK"):                  # Ctrl-Break on Windows
        signal.signal(signal.SIGBREAK, _sigint)
    try:
        mm.run()
    except Exception as exc:                         # noqa: BLE001 - last resort
        print(f"FATAL: {exc!r}", file=sys.stderr)
        try:
            mm.shutdown()
        except Exception:
            pass
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
