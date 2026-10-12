"""
RIT ALGO2 - market making (ALGO)
"""

import requests
import threading
import time
from concurrent.futures import ThreadPoolExecutor

API_KEY = {"X-API-key": "YOUR_API_KEY_HERE"}
BASE_URL = "http://localhost:9999/v1"   # confirm port via the API icon

TICKER = "ALGO"

# set this to your REAL position before a restart - the script has no
# way to know it otherwise and will skip all limit/skew/flatten logic
# until it's told. 0 = flat (the normal case for a fresh start).
STARTING_POSITION = 0

ORDER_SIZE_LIMIT = 5_000
GROSS_POSITION_LIMIT = 25_000       # fined 10c/share above this
COMMISSION_PER_SHARE = 0.01         # active liquidity (market, or marketable limit)
REBATE_PER_SHARE = 0.005            # passive liquidity (resting limit, filled later)

BASE_ORDER_SIZE = 1_000
BASE_SPREAD = 0.02       # half-spread off the midpoint
REPEG_THRESHOLD = 0.05   # re-peg once mid drifts this far from our quote center
SKEW_COEFFICIENT = 0.000004   # price shift per share of inventory

SOFT_POSITION_LIMIT = 15_000   # beyond this: stop quoting the side that adds exposure
HARD_POSITION_LIMIT = 20_000   # beyond this: flatten at market (commission beats the fine)
EMERGENCY_FLATTEN_QTY = 2_000

RATE_LIMIT_INITIAL_GUESS = 2   # orders/sec, self-corrects off the first real 429
MAX_CACHE_AGE = 0.5
TICK_SIZE = 0.01

session = requests.Session()
session.headers.update(API_KEY)
session.mount("http://", requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10))
executor = ThreadPoolExecutor(max_workers=4)

position = 0
pos_lock = threading.Lock()

session_realized_pnl = 0.0
pnl_lock = threading.Lock()

rate_state = {"min_interval": 1.0 / RATE_LIMIT_INITIAL_GUESS, "next_allowed": 0.0}
rate_lock = threading.Lock()

shutdown = False

quote_cache = {"bid": None, "bid_size": 0, "ask": None, "ask_size": 0, "ts": 0.0}
quote_cache_lock = threading.Lock()

case_cache = {"tick": None, "status": None, "ts": 0.0}
case_cache_lock = threading.Lock()

# at most one bid, one ask resting at a time
tracked_orders = {"bid": None, "ask": None}    # order_id or None
tracked_prices = {"bid": None, "ask": None}    # price we quoted at
tracked_sizes = {"bid": None, "ask": None}     # size we quoted at (for the NOT_FOUND fallback)
tracked_lock = threading.Lock()

order_status_cache = {}   # order_id -> {status, quantity_filled, vwap, ts}
order_status_lock = threading.Lock()


# API

def api_request(method, endpoint, params=None):
    url = f"{BASE_URL}/{endpoint}"
    resp = session.get(url, params=params) if method == "GET" else session.delete(url, params=params)

    if resp.ok:
        try:
            return resp.json()
        except ValueError:
            return {}

    global shutdown
    try:
        body = resp.json()
    except ValueError:
        body = {}

    if resp.status_code == 401:
        print("401 - bad API key, stopping.")
        shutdown = True
    elif resp.status_code == 404 and endpoint.startswith("case"):
        print("no case running, stopping.")
        shutdown = True
    else:
        print(f"request failed ({resp.status_code}, {body.get('code', '')}): {resp.text}")
    return None


def get_case_status():
    case = api_request("GET", "case")
    return (case.get("tick"), case.get("status")) if case else (None, None)


def get_quote():
    book = api_request("GET", "securities/book", params={"ticker": TICKER})
    if not book or not book.get("bids") or not book.get("asks"):
        return None
    bid, ask = book["bids"][0], book["asks"][0]
    return bid["price"], bid["quantity"], ask["price"], ask["quantity"]


def get_order_detail(order_id):
    """dict on success, 'NOT_FOUND' on a 404 (orders can vanish from
    lookup shortly after they resolve), None on a transient failure -
    callers must not treat None as resolved."""
    resp = session.get(f"{BASE_URL}/orders/{order_id}")
    if resp.status_code == 404:
        return "NOT_FOUND"
    if resp.ok:
        try:
            return resp.json()
        except ValueError:
            return None
    global shutdown
    if resp.status_code == 401:
        print("401 - bad API key, stopping.")
        shutdown = True
    else:
        print(f"request failed ({resp.status_code}) fetching order {order_id}: {resp.text}")
    return None


def cancel_order(order_id):
    result = api_request("DELETE", f"orders/{order_id}")
    return result is not None and result.get("success", True)


def submit_order(action, quantity, price=None, order_type="LIMIT"):
    """returns (order_id, quantity_filled, vwap, status), or (None, 0, None, None) on rejection."""
    url = f"{BASE_URL}/orders"
    params = {"ticker": TICKER, "type": order_type, "quantity": quantity, "action": action}
    if order_type == "LIMIT":
        params["price"] = price

    while True:
        with rate_lock:
            wait_needed = rate_state["next_allowed"] - time.time()
        if wait_needed > 0:
            time.sleep(wait_needed)

        resp = session.post(url, params=params)

        if resp.status_code == 429:
            wait = resp.headers.get("X-Wait-Until")
            if wait is None:
                try:
                    wait = resp.json().get("wait", 1.0)
                except ValueError:
                    wait = 1.0
            wait = float(wait)
            with rate_lock:
                rate_state["min_interval"] = wait
                rate_state["next_allowed"] = time.time() + wait
            print(f"429, waiting {wait:.3f}s and recalibrating")
            time.sleep(wait)
            continue

        if resp.status_code == 401:
            global shutdown
            print("401 - bad API key, stopping.")
            shutdown = True
            return None, 0, None, None

        if not resp.ok:
            print(f"order rejected ({resp.status_code}): {resp.text}")
            with rate_lock:
                rate_state["next_allowed"] = time.time() + rate_state["min_interval"]
            return None, 0, None, None

        with rate_lock:
            rate_state["next_allowed"] = time.time() + rate_state["min_interval"]

        order = resp.json()
        order_id, status = order.get("order_id"), order.get("status")
        filled, vwap = order.get("quantity_filled", 0), order.get("vwap")

        if order_id is not None:
            with order_status_lock:
                order_status_cache[order_id] = {
                    "status": status, "quantity_filled": filled, "vwap": vwap, "ts": time.time(),
                }
        return order_id, filled, vwap, status


def _quote_poller():
    # GETs don't get rate limited so just hammer it
    while not shutdown:
        quote = get_quote()
        if quote is not None:
            bid, bid_size, ask, ask_size = quote
            with quote_cache_lock:
                quote_cache.update({"bid": bid, "bid_size": bid_size,
                                     "ask": ask, "ask_size": ask_size, "ts": time.time()})


def _case_poller():
    while not shutdown:
        tick, status = get_case_status()
        if status is not None:
            with case_cache_lock:
                case_cache.update({"tick": tick, "status": status, "ts": time.time()})
        time.sleep(0.1)


def _order_status_poller():
    """refreshes order_status_cache for whichever order IDs are currently tracked."""
    while not shutdown:
        with tracked_lock:
            ids = [oid for oid in tracked_orders.values() if oid is not None]
        for oid in ids:
            detail = get_order_detail(oid)

            if detail == "NOT_FOUND":
                # guard a race with cancel_and_clear(): if the main loop already
                # cancelled and cleared this order since we listed `ids`, this
                # 404 is an artifact of that cancel, not a phantom fill
                with tracked_lock:
                    still_tracked = oid in tracked_orders.values()
                if not still_tracked:
                    continue
                with order_status_lock:
                    order_status_cache[oid] = {
                        "status": "ASSUMED_FILLED_NOT_FOUND",
                        "quantity_filled": None,   # resolved from tracked size/price - see _resolve_fill_data
                        "vwap": None,
                        "ts": time.time(),
                    }
            elif detail is not None:
                with order_status_lock:
                    order_status_cache[oid] = {
                        "status": detail.get("status"),
                        "quantity_filled": detail.get("quantity_filled", 0),
                        "vwap": detail.get("vwap"),
                        "ts": time.time(),
                    }
            # else: transient failure - leave the cache as-is, retry next poll


def get_cached_quote():
    with quote_cache_lock:
        entry = dict(quote_cache)
    if entry["bid"] is None or (time.time() - entry["ts"]) > MAX_CACHE_AGE:
        return None
    return entry["bid"], entry["bid_size"], entry["ask"], entry["ask_size"]


def get_cached_order_status(order_id):
    if order_id is None:
        return None
    with order_status_lock:
        return order_status_cache.get(order_id)


# pricing

def round_to_tick(price):
    return round(round(price / TICK_SIZE) * TICK_SIZE, 2)


def compute_quote_prices(mid_price, current_position):
    """long position shifts center down (cheaper ask -> encourages sells,
    less attractive bid -> discourages more buys), and vice versa."""
    skew = -SKEW_COEFFICIENT * current_position
    center = mid_price + skew
    return round_to_tick(center - BASE_SPREAD), round_to_tick(center + BASE_SPREAD)


def compute_quote_sizes(current_position):
    """scales down the side that would grow an already-large position,
    rather than cutting it off outright before SOFT_POSITION_LIMIT."""
    bid_size = ask_size = BASE_ORDER_SIZE
    if current_position > 0:
        scale = max(0.0, 1.0 - current_position / SOFT_POSITION_LIMIT)
        bid_size = max(0, int(BASE_ORDER_SIZE * scale))
    elif current_position < 0:
        scale = max(0.0, 1.0 - (-current_position) / SOFT_POSITION_LIMIT)
        ask_size = max(0, int(BASE_ORDER_SIZE * scale))
    return bid_size, ask_size


# trading

def record_fill(action, quantity, vwap, passive):
    """passive=True: resting LIMIT fill, earns rebate. passive=False:
    active/marketable fill (e.g. emergency flatten), pays commission."""
    global position, session_realized_pnl
    with pos_lock:
        position += quantity if action == "BUY" else -quantity
    fee = (REBATE_PER_SHARE if passive else -COMMISSION_PER_SHARE) * quantity
    with pnl_lock:
        session_realized_pnl += fee
    print(f"fill: {action} {quantity} @ {vwap} ({'passive' if passive else 'active'}) "
          f"fee={fee:+.2f} position={position} pnl={session_realized_pnl:.2f}")


def cancel_and_clear(side):
    """the sole path for tearing down a tracked order on 'bid'/'ask' - always
    checks the order's real status before cancelling, rather than firing a
    blind DELETE and guessing what a failure meant. (An earlier version did
    that and silently dropped a real fill whenever an order resolved in the
    gap between deciding to cancel and the DELETE landing.)"""
    with tracked_lock:
        order_id = tracked_orders[side]
        quoted_price = tracked_prices[side]
        quoted_size = tracked_sizes[side]
    if order_id is None:
        return

    detail = get_order_detail(order_id)
    action = "BUY" if side == "bid" else "SELL"

    if detail is None:
        return   # transient - don't clear tracking or guess, retry next tick

    if detail == "NOT_FOUND":
        # already gone by the time we checked - assume fully filled at our
        # quoted price (an approximation, not a confirmed fill)
        print(f"   ! order {order_id} ({side}) 404'd before cancel - assuming filled "
              f"{quoted_size} @ {quoted_price}")
        if quoted_size:
            record_fill(action, quoted_size, quoted_price, passive=True)
    elif detail.get("status") == "OPEN":
        # may have partially filled while resting - record that before
        # cancelling the remainder, or those shares vanish from tracking
        filled_so_far = detail.get("quantity_filled", 0)
        if filled_so_far > 0:
            record_fill(action, filled_so_far, detail.get("vwap"), passive=True)

        if not cancel_order(order_id):
            # cancel didn't confirm - order may still be live, keep tracking
            # it rather than lose sight of real exposure
            print(f"   ! cancel of {order_id} ({side}) didn't confirm - leaving it tracked")
            return
    else:
        filled = detail.get("quantity_filled", 0)
        if filled > 0:
            record_fill(action, filled, detail.get("vwap"), passive=True)

    with tracked_lock:
        tracked_orders[side] = tracked_prices[side] = tracked_sizes[side] = None


def place_pair(mid_price):
    """submits a fresh bid+ask pair concurrently, sized/priced off current inventory."""
    with pos_lock:
        current_position = position

    bid_price, ask_price = compute_quote_prices(mid_price, current_position)
    bid_size, ask_size = compute_quote_sizes(current_position)

    # don't add exposure past the soft limit (hard limit / flatten is handled in trading_tick)
    if current_position >= SOFT_POSITION_LIMIT:
        bid_size = 0
    if current_position <= -SOFT_POSITION_LIMIT:
        ask_size = 0

    def _place_bid():
        if bid_size <= 0:
            return None
        oid, *_ = submit_order("BUY", bid_size, bid_price, "LIMIT")
        return oid, bid_price, bid_size

    def _place_ask():
        if ask_size <= 0:
            return None
        oid, *_ = submit_order("SELL", ask_size, ask_price, "LIMIT")
        return oid, ask_price, ask_size

    future_bid = executor.submit(_place_bid)
    future_ask = executor.submit(_place_ask)
    bid_result, ask_result = future_bid.result(), future_ask.result()

    with tracked_lock:
        if bid_result is not None:
            tracked_orders["bid"], tracked_prices["bid"], tracked_sizes["bid"] = bid_result
        if ask_result is not None:
            tracked_orders["ask"], tracked_prices["ask"], tracked_sizes["ask"] = ask_result


def emergency_flatten(current_position):
    """crosses the spread on purpose (pays commission) - far cheaper than
    risking the 10c/share position-limit fine."""
    qty = min(EMERGENCY_FLATTEN_QTY, ORDER_SIZE_LIMIT, abs(current_position))
    if qty <= 0:
        return
    action = "SELL" if current_position > 0 else "BUY"
    print(f"   ! EMERGENCY FLATTEN: position={current_position}, {action} {qty} at market")
    oid, filled, vwap, status = submit_order(action, qty, order_type="MARKET")
    if filled:
        record_fill(action, filled, vwap, passive=False)


def _resolve_fill_data(side, order_id, status_entry):
    """ASSUMED_FILLED_NOT_FOUND: the order 404'd before we could confirm its
    real fill - assume fully filled at our quoted price (can't fill worse
    than a limit price, so this is at worst optimistic, not a random guess)."""
    if status_entry["status"] == "ASSUMED_FILLED_NOT_FOUND":
        with tracked_lock:
            filled = tracked_sizes.get(side) or 0
            vwap = tracked_prices.get(side)
        print(f"   ! order {order_id} ({side}) 404'd before fill confirmed - assuming filled "
              f"{filled} @ {vwap}")
        return filled, vwap
    return status_entry["quantity_filled"], status_entry["vwap"]


def trading_tick():
    quote = get_cached_quote()
    if quote is None:
        return
    bid, bidsz, ask, asksz = quote
    mid_price = (bid + ask) / 2.0

    with pos_lock:
        current_position = position

    if abs(current_position) >= HARD_POSITION_LIMIT:
        # pull the risk-increasing side before flattening, so we don't add
        # exposure while also trying to cut it
        cancel_and_clear("bid" if current_position > 0 else "ask")
        emergency_flatten(current_position)
        return

    with tracked_lock:
        bid_id, ask_id = tracked_orders["bid"], tracked_orders["ask"]

    bid_status = get_cached_order_status(bid_id) if bid_id else None
    ask_status = get_cached_order_status(ask_id) if ask_id else None
    bid_resolved = bid_id is not None and bid_status is not None and bid_status["status"] != "OPEN"
    ask_resolved = ask_id is not None and ask_status is not None and ask_status["status"] != "OPEN"

    if bid_resolved:
        filled, vwap = _resolve_fill_data("bid", bid_id, bid_status)
        if filled > 0:
            record_fill("BUY", filled, vwap, passive=True)
        with tracked_lock:
            tracked_orders["bid"] = tracked_prices["bid"] = tracked_sizes["bid"] = None

    if ask_resolved:
        filled, vwap = _resolve_fill_data("ask", ask_id, ask_status)
        if filled > 0:
            record_fill("SELL", filled, vwap, passive=True)
        with tracked_lock:
            tracked_orders["ask"] = tracked_prices["ask"] = tracked_sizes["ask"] = None

    with tracked_lock:
        bid_id, ask_id = tracked_orders["bid"], tracked_orders["ask"]

    if bid_id is None and ask_id is None:
        place_pair(mid_price)   # fresh start, or both sides just resolved - requote immediately
        return

    if (bid_id is None) != (ask_id is None):
        # one side filled, the other's still open - cancel it rather than wait
        # indefinitely for a trending market to come back to it
        cancel_and_clear("ask" if bid_id is None else "bid")
        return

    # both still open - re-peg if the market's drifted far enough from our quote center
    with tracked_lock:
        quoted_bid, quoted_ask = tracked_prices["bid"], tracked_prices["ask"]
    quoted_center = (quoted_bid + quoted_ask) / 2.0 if quoted_bid is not None else mid_price
    if abs(mid_price - quoted_center) > REPEG_THRESHOLD:
        cancel_and_clear("bid")
        cancel_and_clear("ask")
        place_pair(mid_price)


# closing

def signal_handler(signum, frame):
    global shutdown
    print("stopping...")
    shutdown = True


# main

def main():
    global position
    print("starting ALGO2 market-making loop...")

    if STARTING_POSITION != 0:
        with pos_lock:
            position = STARTING_POSITION
        print(f"*** seeded starting position = {STARTING_POSITION} - confirm this matches the RIT client ***")
        if abs(STARTING_POSITION) >= HARD_POSITION_LIMIT:
            print(f"*** already past HARD_POSITION_LIMIT ({HARD_POSITION_LIMIT}) - first tick will flatten ***")

    threading.Thread(target=_quote_poller, daemon=True).start()
    threading.Thread(target=_case_poller, daemon=True).start()
    threading.Thread(target=_order_status_poller, daemon=True).start()
    time.sleep(0.3)   # let pollers populate the cache before the first tick

    while not shutdown:
        with case_cache_lock:
            status = case_cache["status"]
            status_age = time.time() - case_cache["ts"]

        if status is None or status_age > MAX_CACHE_AGE:
            time.sleep(0.05)
            continue
        if status != "ACTIVE":
            print(f"case status '{status}', stopping")
            break

        trading_tick()

    cancel_and_clear("bid")
    cancel_and_clear("ask")

    print(f"final pnl (estimate): {session_realized_pnl:.2f}")
    print(f"final position: {position}")


if __name__ == "__main__":
    import signal
    signal.signal(signal.SIGINT, signal_handler)
    main()