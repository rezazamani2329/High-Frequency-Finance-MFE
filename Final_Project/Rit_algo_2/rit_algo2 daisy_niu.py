"""
RIT ALGO2 - Lot-pairing grid market maker
=========================================

Idea
----
The case brief describes market making as *pairs*: a bid fills, and the
trade is finished when a matching ask fills.  This strategy takes that
literally.  Every fill opens a **lot** (side, size, entry price, time), and
every lot owns an **exit order** priced off its own entry - not off the
current market.  Inventory is therefore a book of individual trades, each
with its own profit target, its own clock and its own stop.

Why that suits this case: a passive fill earns the limit-order rebate, so a
round trip on two passive fills earns  (exit - entry) + 2 x rebate.  A lot
can give back up to 2 x rebate on price and still break even.  That
"giveback budget" is what each exit decays towards as the lot ages
(brief, discussion question 5).

The four rules
--------------
1. ENTRY.  When flat, rest a bid and an offer a fixed half-width around the
   competitor mid, never behind the touch.  When holding lots, only the side
   that ADDS is quoted as an entry; the other side is covered by exits.

2. GRID.  A new entry on the side we already hold must be at least
   grid_step ticks better than the best entry we already have (a long adds
   only lower, a short only higher), and at most max_lots lots / max_inventory
   shares are open.  Inventory can only be added at better prices, a little
   at a time.

3. EXIT.  Each lot's exit starts target_ticks beyond its entry and walks
   one tick closer every decay_secs, down to the break-even floor
   (entry minus the rebate giveback).  Lots whose exits land on the same
   price share one order.

4. STOP.  A lot is closed by crossing the spread when the market has moved
   stop_ticks against its entry, or when it is older than max_age seconds.
   After a stop, entries in that direction pause for stop_pause seconds, and
   the pause doubles with each further stop in the same direction (capped
   at 60 s), a circuit breaker that keeps a trend from refilling the same
   losing side.

Bookkeeping
-----------
Fills are read per order: an increase in quantity_filled on an open order,
or the final state from GET /orders/{id} once an order leaves the open
list.  Each fill closes opposite lots first-in-first-out, and any remainder
opens a new lot.  The ledger is checked against the server position every
loop; a gap that persists for 3 loops is booked at the mid, so the ledger
can never drift.

Limits: |position| + everything resting on that side stays under the
position limit minus a buffer.  Orders are capped at the per-order maximum
and paced by a rolling one-second window.  Exits and stops get the order
budget before entries.

Usage
-----
    python algo2_lot_grid.py --check --port 9999 --key ABCD1234
    python algo2_lot_grid.py --port 9999 --key ABCD1234
"""

from __future__ import annotations

import argparse
import collections
import csv
import math
import os
import signal
import sys
import time
from dataclasses import dataclass
from typing import Deque, Dict, List, Optional, Tuple

import requests

BUY, SELL = "BUY", "SELL"
DIR = {BUY: 1, SELL: -1}


# ============================================================================
# Parameters
# ============================================================================

@dataclass
class Params:
    host: str = "localhost"
    port: int = 9999
    key: str = ""
    ticker: str = "ALGO"
    timeout: float = 2.0

    # entries
    entry_size: int = 2500        # shares per entry order / lot
    half_width: float = 1.5       # entry distance from mid, in ticks (never behind the touch)
    entry_hold: int = 1           # keep a resting entry up to this many ticks behind the new price
    grid_step: int = 3            # ticks a new same-side entry must improve on the best existing lot
    max_lots: int = 3
    max_inventory: int = 7500

    # exits
    target_ticks: int = 2         # starting profit target per lot
    decay_secs: float = 4.0       # exit moves one tick closer this often
    giveback: Optional[int] = None  # break-even floor in ticks; None = 2 x rebate
    max_exit_levels: int = 2      # distinct exit prices per side (orders cost rate budget)

    # stops
    stop_ticks: int = 6           # adverse move from entry that closes a lot
    max_age: float = 30.0         # seconds before a lot is closed regardless
    stop_pause: float = 8.0       # pause same-direction entries after a stop
    stop_slip: int = 3            # ticks paid through the touch when crossing
    stop_gap: float = 1.0         # seconds between stop orders

    # end of case (positions are marked to market at the close)
    no_entry_ticks: int = 20      # final ticks with no new entries; exits go to break-even

    # risk / pacing
    limit_fallback: int = 25000
    limit_buffer: int = 1500
    max_order_fallback: int = 5000
    rate_fallback: int = 5
    rate_window: float = 1.05
    min_order: int = 100
    listing_grace: float = 1.0    # a fresh order may be unknown to the server this long
    gap_secs: float = 1.0         # ledger/position gap older than this is booked
    poll: float = 0.08

    log: Optional[str] = "algo2_lot_grid_log.csv"
    status_secs: float = 5.0
    quiet: bool = False

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}/v1"


# ============================================================================
# HTTP layer
# ============================================================================

class ApiError(Exception):
    pass


class Throttled(ApiError):
    def __init__(self, wait: float):
        super().__init__(f"throttled {wait:.2f}s")
        self.wait = wait


class Refused(ApiError):
    pass


class Api:
    """Thin RIT REST wrapper.  No automatic retries: the main loop re-reads
    state every cycle, so a failed call is simply tried again next time."""

    def __init__(self, p: Params):
        self.base = p.url
        self.timeout = p.timeout
        self.http = requests.Session()
        self.http.headers["X-API-Key"] = p.key

    def _call(self, verb: str, path: str, **params):
        try:
            r = self.http.request(verb, self.base + path, params=params or None,
                                  timeout=self.timeout)
        except requests.RequestException as exc:
            raise ApiError(f"{verb} {path}: {exc}") from exc
        if r.status_code == 200:
            try:
                return r.json()
            except ValueError as exc:
                raise ApiError(f"{path}: not JSON") from exc
        if r.status_code == 429:
            wait = 0.25
            try:
                wait = float(r.headers.get("Retry-After") or r.json().get("wait", wait))
            except Exception:
                pass
            raise Throttled(wait)
        if r.status_code == 401:
            raise ApiError("401 - API key wrong or RIT client not logged in")
        if 400 <= r.status_code < 500:
            raise Refused(f"{r.status_code} {r.text[:160]}")
        raise ApiError(f"{verb} {path} -> {r.status_code}")

    def case(self):
        return self._call("GET", "/case")

    def trader(self):
        return self._call("GET", "/trader")

    def limits(self):
        return self._call("GET", "/limits")

    def security(self, ticker):
        res = self._call("GET", "/securities", ticker=ticker)
        if isinstance(res, list):
            res = next((s for s in res if s.get("ticker") == ticker), None)
            if res is None:
                raise ApiError(f"{ticker} not in case")
        return res

    def all_securities(self):
        return self._call("GET", "/securities")

    def book(self, ticker):
        return self._call("GET", "/securities/book", ticker=ticker, limit=20)

    def open_orders(self):
        return self._call("GET", "/orders", status="OPEN")

    def order(self, oid):
        return self._call("GET", f"/orders/{oid}")

    def submit(self, ticker, side, qty, price):
        return self._call("POST", "/orders", ticker=ticker, type="LIMIT",
                          quantity=qty, action=side, price=price)

    def cancel(self, oid):
        return self._call("DELETE", f"/orders/{oid}")

    def cancel_all(self, ticker):
        return self._call("POST", "/commands/cancel", ticker=ticker)


def val(d, k, default=0.0):
    v = d.get(k) if isinstance(d, dict) else None
    return default if v is None else v


class Pacer:
    """At most `cap` order submissions in any rolling window of `window` s."""

    def __init__(self, cap: int, window: float):
        self.cap, self.window = cap, window
        self.sent: Deque[float] = collections.deque()
        self.hold_until = 0.0
        self.cancels_count = False   # set if the server ever throttles a cancel

    def free(self) -> int:
        now = time.monotonic()
        if now < self.hold_until:
            return 0
        while self.sent and now - self.sent[0] >= self.window:
            self.sent.popleft()
        return max(self.cap - len(self.sent), 0)

    def use(self):
        self.sent.append(time.monotonic())

    def back_off(self, wait: float):
        self.hold_until = time.monotonic() + min(max(wait, 0.05), 2.0)
        self.window = min(self.window + 0.05, 1.5)


# ============================================================================
# Lots
# ============================================================================

@dataclass
class Lot:
    dir: int          # +1 long, -1 short
    qty: int
    entry: int        # ticks
    opened: float     # monotonic


class Ledger:
    """Open lots, all on one side at any time.  Fills close the opposite side
    first-in-first-out; any remainder opens a new lot."""

    def __init__(self):
        self.lots: Deque[Lot] = collections.deque()
        self.realized_ticks = 0      # sum of shares x ticks captured on price
        self.round_trips = 0

    @property
    def net(self) -> int:
        return sum(l.dir * l.qty for l in self.lots)

    @property
    def dir(self) -> int:
        return self.lots[0].dir if self.lots else 0

    def apply(self, signed_qty: int, price: int, now: float):
        d = 1 if signed_qty > 0 else -1
        left = abs(signed_qty)
        while left and self.lots and self.lots[0].dir == -d:
            lot = self.lots[0]
            take = min(left, lot.qty)
            self.realized_ticks += take * (price - lot.entry) * lot.dir
            lot.qty -= take
            left -= take
            if lot.qty == 0:
                self.lots.popleft()
                self.round_trips += 1
        if left:
            self.lots.append(Lot(d, left, price, now))


# ============================================================================
# Tracked orders
# ============================================================================

@dataclass
class Live:
    oid: int
    side: str
    price: int
    qty: int
    filled: int
    sent: float
    cancelling: bool = False

    @property
    def rest(self) -> int:
        return max(self.qty - self.filled, 0)


@dataclass
class Market:
    bid: Optional[int]     # competitor best, own orders excluded
    ask: Optional[int]
    pos: int
    last: Optional[int]

    @property
    def mid(self) -> Optional[float]:
        if self.bid is not None and self.ask is not None:
            return (self.bid + self.ask) / 2
        return float(self.last) if self.last else None


# ============================================================================
# Strategy
# ============================================================================

class LotGrid:

    def __init__(self, p: Params, api: Api):
        self.p, self.api = p, api
        self.stop = False
        self.dec = 2
        self.max_order = p.max_order_fallback
        self.limit = p.limit_fallback
        self.rebate = self.fee = 0.0
        self.trader_id = None
        self.pacer = Pacer(p.rate_fallback, p.rate_window)
        self.ledger = Ledger()
        self.live: Dict[int, Live] = {}
        self.tick = 0
        self.ticks_total = 300
        self.gap_since: Optional[float] = None
        self.next_stop = 0.0
        self.paused = {1: 0.0, -1: 0.0}     # direction -> entries paused until
        self.streak_dir, self.streak, self.last_stop_at = 0, 0, -1e9
        self.entry_px: Dict[str, Optional[int]] = {BUY: None, SELL: None}
        self.mkt: Optional[Market] = None
        self.count = collections.Counter()
        self.t0 = time.monotonic()
        self._status_at = 0.0
        self._limits_at = 0.0
        self._csv = self._fh = None
        if p.log:
            try:
                fresh = not os.path.exists(p.log)
                self._fh = open(p.log, "a", newline="", encoding="utf-8")
                self._csv = csv.writer(self._fh)
                if fresh:
                    self._csv.writerow(["time", "tick", "pos", "ledger", "lots", "bid",
                                        "ask", "realized_ticks", "round_trips", "stops",
                                        "nlv"])
            except OSError as exc:
                print(f"no CSV log ({exc})")

    # -- small helpers ------------------------------------------------------

    def log(self, msg):
        if not self.p.quiet:
            print(f"[{time.monotonic() - self.t0:6.1f}s t{self.tick:>3}] {msg}", flush=True)

    def ticks(self, price) -> int:
        return int(round(float(price) * 10 ** self.dec))

    def px(self, t: int) -> str:
        return f"{t / 10 ** self.dec:.{self.dec}f}"

    @property
    def cap(self) -> int:
        return max(self.limit - self.p.limit_buffer, 0)

    @property
    def giveback(self) -> int:
        if self.p.giveback is not None:
            return max(self.p.giveback, 0)
        return int(2 * self.rebate * 10 ** self.dec + 1e-9)

    @property
    def ending(self) -> bool:
        return self.ticks_total - self.tick <= self.p.no_entry_ticks

    # -- static data ----------------------------------------------------------

    def read_security_params(self, sec):
        self.dec = int(val(sec, "quoted_decimals", 2))
        mts = int(val(sec, "max_trade_size", 0))
        self.max_order = mts if mts > 0 else self.p.max_order_fallback
        self.rebate = float(val(sec, "limit_order_rebate", 0.0))
        self.fee = float(val(sec, "trading_fee", 0.0))
        rate = int(val(sec, "api_orders_per_second", 0))
        if rate > 0:
            self.pacer.cap = rate

    def read_limits(self):
        self._limits_at = time.monotonic()
        try:
            rows = self.api.limits()
        except ApiError:
            return
        found = [int(val(r, k, 0)) for r in rows or [] for k in ("gross_limit", "net_limit")
                 if val(r, k, 0) > 0]
        if found:
            self.limit = min(found)

    # -- state sync -------------------------------------------------------------

    def credit(self, lv: Live, filled_total: int, price: Optional[int] = None):
        """Book any new fills on a tracked order into the ledger."""
        new = filled_total - lv.filled
        if new <= 0:
            return
        lv.filled = filled_total
        self.ledger.apply(DIR[lv.side] * new, price if price is not None else lv.price,
                          time.monotonic())
        self.count["filled"] += new

    def sync(self) -> Optional[Market]:
        """Open orders -> fills, then position, then book."""
        t = self.p.ticker
        try:
            listed = self.api.open_orders()
            sec = self.api.security(t)
            book = self.api.book(t)
        except Throttled as exc:
            time.sleep(min(exc.wait, 1.0))
            return None
        except ApiError as exc:
            self.count["errors"] += 1
            self.log(f"read failed: {exc}")
            return None
        self.read_security_params(sec)
        now = time.monotonic()

        open_by_id = {o.get("order_id"): o for o in listed or []
                      if o.get("ticker") == t and o.get("status", "OPEN") == "OPEN"}
        for oid, lv in list(self.live.items()):
            o = open_by_id.get(oid)
            if o is not None:
                self.credit(lv, int(val(o, "quantity_filled", 0)))
                continue
            # gone from the list: filled, cancelled, or not listed yet - ask
            try:
                final = self.api.order(oid)
            except Refused:
                final = None
            except ApiError:
                continue                          # try again next cycle
            if final is None and now - lv.sent < self.p.listing_grace:
                continue                          # server may not know it yet
            if final is not None:
                if final.get("status") == "OPEN":
                    self.credit(lv, int(val(final, "quantity_filled", 0)))
                    continue                      # list was just lagging
                vw = final.get("vwap")
                self.credit(lv, int(val(final, "quantity_filled", 0)),
                            self.ticks(vw) if vw else None)
            del self.live[oid]

        # orders we do not know (e.g. left over from a restart): adopt them so
        # they are counted for exposure and cancelled if unwanted
        for oid, o in open_by_id.items():
            if oid not in self.live and o.get("price") is not None:
                self.live[oid] = Live(oid, o["action"], self.ticks(o["price"]),
                                      int(val(o, "quantity", 0)),
                                      int(val(o, "quantity_filled", 0)), now)

        own = set(self.live) | set(open_by_id)
        best = {BUY: None, SELL: None}
        for key, side in (("bids", BUY), ("asks", SELL)):
            for e in (book or {}).get(key) or []:
                if e.get("order_id") in own or e.get("price") is None:
                    continue
                if self.trader_id and e.get("trader_id") == self.trader_id:
                    continue
                if int(val(e, "quantity", 0)) - int(val(e, "quantity_filled", 0)) <= 0:
                    continue
                q = self.ticks(e["price"])
                b = best[side]
                if b is None or (q > b if side == BUY else q < b):
                    best[side] = q

        last = float(val(sec, "last", 0.0))
        m = Market(best[BUY], best[SELL], int(round(float(val(sec, "position", 0)))),
                   self.ticks(last) if last > 0 else None)
        self.count["max_pos"] = max(self.count["max_pos"], abs(m.pos))

        # ledger vs server position.  Fills between our reads make small
        # transient gaps; only a gap that persists past gap_secs is real (a
        # missed fill) and is booked at the mid.
        gap = m.pos - self.ledger.net
        if not gap:
            self.gap_since = None
        elif self.gap_since is None:
            self.gap_since = now
        elif now - self.gap_since >= self.p.gap_secs and m.mid is not None:
            self.ledger.apply(gap, int(round(m.mid)), now)
            self.count["ledger_fixes"] += 1
            self.gap_since = None
        self.mkt = m
        return m

    # -- order actions ------------------------------------------------------------

    def send(self, side: str, qty: int, price: int) -> Optional[Live]:
        if qty < self.p.min_order or self.pacer.free() <= 0:
            return None
        self.pacer.use()
        try:
            r = self.api.submit(self.p.ticker, side, qty, self.px(price))
        except Throttled as exc:
            self.pacer.back_off(exc.wait)
            self.count["throttled"] += 1
            return None
        except Refused as exc:
            self.count["refused"] += 1
            self.log(f"refused {side} {qty} @ {self.px(price)}: {exc}")
            return None
        except ApiError as exc:
            # the order may or may not exist; the next /orders read adopts it
            self.count["errors"] += 1
            self.log(f"submit failed: {exc}")
            return None
        self.count["orders"] += 1
        oid = r.get("order_id")
        lv = Live(oid, side, price, int(val(r, "quantity", qty)), 0, time.monotonic())
        vw = r.get("vwap")
        self.credit(lv, int(val(r, "quantity_filled", 0)), self.ticks(vw) if vw else None)
        if oid is not None:
            self.live[oid] = lv
        return lv

    def pull(self, lv: Live) -> bool:
        if lv.cancelling:
            return True
        if self.pacer.cancels_count:
            if self.pacer.free() <= 0:
                return False
            self.pacer.use()
        try:
            self.api.cancel(lv.oid)
        except Throttled as exc:
            self.pacer.cancels_count = True
            self.pacer.back_off(exc.wait)
            return False
        except Refused:
            pass                                  # already done; sync() reads its fills
        except ApiError:
            return False
        lv.cancelling = True                      # stays tracked until its final fill is read
        self.count["cancels"] += 1
        return True

    def cross(self, side: str, qty: int, why: str):
        m = self.mkt
        ref = (m.ask if side == BUY else m.bid) if m else None
        if ref is None and m is not None:
            ref = m.last
        if ref is None:
            return
        price = ref + self.p.stop_slip if side == BUY else max(ref - self.p.stop_slip, 1)
        self.log(f"{why}: {side} {qty:,} @ {self.px(price)} (pos {m.pos:+,})")
        lv = self.send(side, min(qty, self.max_order), price)
        if lv is not None and lv.rest > 0:
            self.pull(lv)                         # never leave an aggressive order resting

    # -- the plan ----------------------------------------------------------------

    def exit_price(self, lot: Lot, now: float) -> int:
        steps = int((now - lot.opened) // self.p.decay_secs)
        edge = self.p.target_ticks - steps
        edge = max(edge, -self.giveback)
        if self.ending:
            edge = -self.giveback
        return lot.entry + lot.dir * edge

    def entry_price(self, side: str, m: Market) -> Optional[int]:
        mid = m.mid
        if mid is None:
            return None
        bid = m.bid if m.bid is not None else int(math.floor(mid)) - 1
        ask = m.ask if m.ask is not None else int(math.ceil(mid)) + 1
        if side == BUY:
            target = max(bid, math.floor(mid - self.p.half_width))
            target = min(target, ask - 1)
            lots = [l.entry for l in self.ledger.lots if l.dir == 1]
            if lots:
                target = min(target, min(lots) - self.p.grid_step)
        else:
            target = min(ask, math.ceil(mid + self.p.half_width))
            target = max(target, bid + 1)
            lots = [l.entry for l in self.ledger.lots if l.dir == -1]
            if lots:
                target = max(target, max(lots) + self.p.grid_step)
        # hysteresis: keep the previous entry if it is at most entry_hold ticks
        # less aggressive than the new one, so a 1-tick wobble costs no orders
        prev = self.entry_px[side]
        if prev is not None:
            behind = (target - prev) if side == BUY else (prev - target)
            passive = prev < ask if side == BUY else prev > bid
            if 0 <= behind <= self.p.entry_hold and passive:
                target = prev
        self.entry_px[side] = target
        return target

    def plan(self, m: Market, now: float) -> Dict[str, Dict[int, Tuple[int, bool]]]:
        """side -> {price: (shares, is_exit)}"""
        want: Dict[str, Dict[int, Tuple[int, bool]]] = {BUY: {}, SELL: {}}

        # exits: one level per distinct exit price, merged down to max_exit_levels
        for d, side in ((1, SELL), (-1, BUY)):
            levels: Dict[int, int] = collections.defaultdict(int)
            for lot in self.ledger.lots:
                if lot.dir != d:
                    continue
                price = self.exit_price(lot, now)
                if side == SELL and m.bid is not None:
                    price = max(price, m.bid + 1)     # passive
                if side == BUY and m.ask is not None:
                    price = min(price, m.ask - 1)
                levels[price] += lot.qty
            # most aggressive first (lowest sell / highest buy); the tail is
            # merged into the last kept level, which only closes it sooner
            order = sorted(levels, reverse=(side == BUY))
            keep = order[:self.p.max_exit_levels]
            for extra in order[self.p.max_exit_levels:]:
                levels[keep[-1]] += levels.pop(extra)
            for price in keep:
                want[side][price] = (levels[price], True)

        # entry: only on the side that adds (both when flat)
        if not self.ending and m.mid is not None:
            held = self.ledger.dir
            n_lots = len(self.ledger.lots)
            room = self.p.max_inventory - abs(self.ledger.net)
            for d, side in ((1, BUY), (-1, SELL)):
                if held == -d:
                    continue                          # this side is covered by exits
                if n_lots >= self.p.max_lots or now < self.paused[d]:
                    continue
                size = min(self.p.entry_size, room, self.max_order)
                if size < self.p.min_order:
                    continue
                price = self.entry_price(side, m)
                if price is None or price < 1 or price in want[side]:
                    continue
                want[side][price] = (size, False)
        if self.ending:
            self.entry_px = {BUY: None, SELL: None}
        return want

    # -- execution --------------------------------------------------------------

    def act(self, want, m: Market):
        working = {s: [lv for lv in self.live.values() if lv.side == s and not lv.cancelling]
                   for s in (BUY, SELL)}

        # 1. cancel anything at an unwanted price, or beyond the wanted size
        for side in (BUY, SELL):
            for price_lv in sorted(working[side], key=lambda x: -(x.oid or 0)):
                lvl = want[side].get(price_lv.price)
                if lvl is None:
                    self.pull(price_lv)
            at: Dict[int, List[Live]] = collections.defaultdict(list)
            for lv in working[side]:
                if not lv.cancelling and lv.price in want[side]:
                    at[lv.price].append(lv)
            for price, lvs in at.items():
                need = want[side][price][0]
                lvs.sort(key=lambda x: x.oid or 0)        # oldest keep queue priority
                total = 0
                for lv in lvs:
                    if total + lv.rest > need + (0 if want[side][price][1] else need // 2):
                        self.pull(lv)
                    else:
                        total += lv.rest

        # 2. post what is missing: exits (most aggressive first), then entries
        jobs = []
        for side in (BUY, SELL):
            for price, (need, is_exit) in want[side].items():
                jobs.append((0 if is_exit else 1, side, price, need, is_exit))
        jobs.sort(key=lambda j: (j[0], -j[2] if j[1] == BUY else j[2]))
        for _, side, price, need, is_exit in jobs:
            resting_here = sum(lv.rest for lv in self.live.values()
                               if lv.side == side and lv.price == price and not lv.cancelling)
            gap = need - resting_here
            threshold = self.p.min_order if is_exit else max(self.p.min_order, need // 2)
            if gap < threshold:
                continue
            # passive against the latest book, never through our own other side
            if side == BUY and m.ask is not None and price >= m.ask:
                continue
            if side == SELL and m.bid is not None and price <= m.bid:
                continue
            other = [lv.price for lv in self.live.values()
                     if lv.side != side and lv.rest > 0]
            if other and ((side == BUY and price >= min(other)) or
                          (side == SELL and price <= max(other))):
                continue
            # hard cap counts cancel-pending orders too: they can still fill
            side_rest = sum(lv.rest for lv in self.live.values() if lv.side == side)
            room = self.cap - DIR[side] * m.pos - side_rest
            while gap >= self.p.min_order and room >= self.p.min_order:
                q = min(gap, room, self.max_order)
                lv = self.send(side, q, price)
                if lv is None:
                    return                        # out of order budget this cycle
                gap -= q
                room -= q

    # -- stops and limit breach -------------------------------------------------

    def risk(self, m: Market, now: float) -> bool:
        """True if this cycle was spent on a forced trade."""
        if abs(m.pos) > self.limit:
            for lv in list(self.live.values()):
                self.pull(lv)
            self.cross(SELL if m.pos > 0 else BUY, abs(m.pos) - self.cap, "LIMIT")
            return True
        if not self.ledger.lots or m.mid is None or now < self.next_stop:
            return False
        hit = 0
        for lot in self.ledger.lots:
            adverse = (lot.entry - m.mid) * lot.dir
            if adverse >= self.p.stop_ticks or now - lot.opened >= self.p.max_age:
                hit += lot.qty
        if not hit:
            return False
        d = self.ledger.dir
        self.next_stop = now + self.p.stop_gap
        # repeated stops in one direction mean a trend: each one doubles the
        # pause (8s, 16s, 32s, capped at 60s); a quiet spell resets it
        if d == self.streak_dir and now - self.last_stop_at < 3 * self.p.stop_pause:
            self.streak += 1
        else:
            self.streak_dir, self.streak = d, 1
        self.last_stop_at = now
        self.paused[d] = now + min(self.p.stop_pause * 2 ** (self.streak - 1), 60.0)
        self.count["stops"] += 1
        # pull everything: entries so the stop is not refilled, and exits so a
        # resting exit and the stop cannot both fill and overshoot through flat
        for lv in list(self.live.values()):
            self.pull(lv)
        self.cross(SELL if d == 1 else BUY, min(hit, abs(m.pos) or hit), "STOP")
        return True

    # -- loop ---------------------------------------------------------------------

    def cycle(self):
        m = self.sync()
        if m is None:
            return
        now = time.monotonic()
        if self.risk(m, now):
            return
        self.act(self.plan(m, now), m)

    def status(self, force=False):
        now = time.monotonic()
        if not force and now - self._status_at < self.p.status_secs:
            return
        self._status_at = now
        m = self.mkt
        if m is None:
            return
        try:
            nlv = self.api.trader().get("nlv")
        except ApiError:
            nlv = None
        lots = " ".join(f"{'L' if l.dir > 0 else 'S'}{l.qty}@{self.px(l.entry)}"
                        for l in self.ledger.lots) or "flat"
        self.log(f"pos {m.pos:+,}  mkt {self.px(m.bid) if m.bid else '-'}/"
                 f"{self.px(m.ask) if m.ask else '-'}  lots [{lots}]  "
                 f"trips {self.ledger.round_trips}  stops {self.count['stops']}  NLV {nlv}")
        if self._csv:
            try:
                self._csv.writerow([f"{time.time():.2f}", self.tick, m.pos, self.ledger.net,
                                    len(self.ledger.lots), m.bid, m.ask,
                                    self.ledger.realized_ticks, self.ledger.round_trips,
                                    self.count["stops"], nlv])
                self._fh.flush()
            except OSError:
                self._csv = None

    def start(self) -> bool:
        while not self.stop:
            try:
                self.trader_id = self.api.trader().get("trader_id")
                self.read_security_params(self.api.security(self.p.ticker))
                self.read_limits()
                return True
            except ApiError as exc:
                self.log(f"waiting for RIT: {exc}")
                time.sleep(2)
        return False

    def run(self):
        if not self.start():
            return
        print(f"{self.p.ticker}: rebate {self.rebate}  fee {self.fee}  max order "
              f"{self.max_order:,}  rate {self.pacer.cap}/s  limit {self.limit:,}  "
              f"break-even giveback {self.giveback} tick(s)")
        active = False
        while not self.stop:
            try:
                case = self.api.case()
            except ApiError as exc:
                self.log(f"case read failed: {exc}")
                time.sleep(0.5)
                continue
            self.tick = int(val(case, "tick", 0))
            self.ticks_total = int(val(case, "ticks_per_period", 300)) or 300
            if case.get("status") != "ACTIVE":
                if active and case.get("status") == "STOPPED":
                    break
                time.sleep(0.25)
                continue
            if not active:
                active = True
                self.log("case active")
                self.read_limits()
            if time.monotonic() - self._limits_at > 5:
                self.read_limits()
            try:
                self.cycle()
                self.status()
            except Exception as exc:              # noqa: BLE001 - keep trading
                self.count["errors"] += 1
                self.log(f"cycle error: {exc!r}")
                time.sleep(0.2)
            time.sleep(self.p.poll)
        self.finish()

    def finish(self):
        try:
            self.api.cancel_all(self.p.ticker)
        except ApiError:
            for lv in list(self.live.values()):
                try:
                    self.api.cancel(lv.oid)
                except ApiError:
                    pass
        self.status(force=True)
        c = self.count
        print("=" * 70)
        print(f"round trips {self.ledger.round_trips}  stops {c['stops']}  "
              f"orders {c['orders']}  cancels {c['cancels']}  throttled {c['throttled']}  "
              f"refused {c['refused']}  errors {c['errors']}")
        print(f"max |position| {c['max_pos']:,} / {self.limit:,}  "
              f"ledger fixes {c['ledger_fixes']}  "
              f"price P&L {self.ledger.realized_ticks / 10 ** self.dec:,.2f} (excl. rebates)")
        try:
            print(f"NLV {self.api.trader().get('nlv')}")
        except ApiError:
            pass
        print("=" * 70)
        if self._fh:
            self._fh.close()
            self._fh = None


# ============================================================================
# CLI
# ============================================================================

def check(p: Params) -> int:
    api = Api(p)
    try:
        case, trader, secs, limits = api.case(), api.trader(), api.all_securities(), api.limits()
    except ApiError as exc:
        print(f"FAILED {p.url}: {exc}\n  check the port/key under the RIT API icon")
        return 1
    print(f"connected {p.url}  trader {trader.get('trader_id')}  NLV {trader.get('nlv')}")
    print(f"case {case.get('name')}  {case.get('status')}  "
          f"tick {case.get('tick')}/{case.get('ticks_per_period')}")
    s = next((x for x in secs if x.get("ticker") == p.ticker), None)
    if s is None:
        print(f"{p.ticker} missing; case has {[x.get('ticker') for x in secs]}")
        return 1
    for k in ("bid", "ask", "position", "max_trade_size", "trading_fee",
              "limit_order_rebate", "api_orders_per_second", "quoted_decimals"):
        print(f"  {k:22} {s.get(k)}")
    for r in limits:
        print(f"  limit {r.get('name')}: gross {r.get('gross_limit')}  net {r.get('net_limit')}")
    return 0


def main(argv=None) -> int:
    d = Params()
    ap = argparse.ArgumentParser(description="RIT ALGO2 lot-pairing grid market maker",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    ap.add_argument("--host", default=d.host)
    ap.add_argument("--port", type=int, default=d.port)
    ap.add_argument("--key", default=None, help="API key (or env RIT_API_KEY)")
    ap.add_argument("--ticker", default=d.ticker)
    ap.add_argument("--entry-size", type=int, default=d.entry_size)
    ap.add_argument("--half-width", type=float, default=d.half_width)
    ap.add_argument("--grid-step", type=int, default=d.grid_step)
    ap.add_argument("--max-lots", type=int, default=d.max_lots)
    ap.add_argument("--max-inventory", type=int, default=d.max_inventory)
    ap.add_argument("--target", type=int, default=d.target_ticks)
    ap.add_argument("--decay", type=float, default=d.decay_secs)
    ap.add_argument("--giveback", type=int, default=None,
                    help="break-even floor in ticks (default: 2 x rebate)")
    ap.add_argument("--stop-ticks", type=int, default=d.stop_ticks)
    ap.add_argument("--max-age", type=float, default=d.max_age)
    ap.add_argument("--stop-pause", type=float, default=d.stop_pause)
    ap.add_argument("--log", default=d.log)
    ap.add_argument("--no-log", action="store_true")
    ap.add_argument("--quiet", action="store_true")
    ap.add_argument("--check", action="store_true", help="print case parameters and exit")
    a = ap.parse_args(argv)

    p = Params(host=a.host, port=a.port, key=a.key or os.environ.get("RIT_API_KEY", ""),
               ticker=a.ticker, entry_size=a.entry_size, half_width=a.half_width,
               grid_step=a.grid_step, max_lots=a.max_lots, max_inventory=a.max_inventory,
               target_ticks=a.target, decay_secs=a.decay, giveback=a.giveback,
               stop_ticks=a.stop_ticks, max_age=a.max_age, stop_pause=a.stop_pause,
               log=None if a.no_log else a.log, quiet=a.quiet)
    if a.check:
        return check(p)

    bot = LotGrid(p, Api(p))

    def halt(*_):
        print("\nstopping - cancelling orders", flush=True)
        bot.stop = True

    signal.signal(signal.SIGINT, halt)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, halt)
    try:
        bot.run()
    except Exception as exc:                      # noqa: BLE001
        print(f"FATAL {exc!r}", file=sys.stderr)
        try:
            bot.finish()
        except Exception:
            pass
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
