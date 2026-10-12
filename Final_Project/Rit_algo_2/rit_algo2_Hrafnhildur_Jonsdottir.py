"""
RIT ALGO2 - market making
"""

import json
import time
import urllib.error
import urllib.request

API_KEY = "Hrafnhildur_sim2_2650"
HOST = "localhost"
PORT = 11222

TICKER = "ALGO"

MAX_ORDER = 5_000
ORDER_SIZE = 5_000
POSITION_LIMIT = 25_000  # fined 10c/share above this
POSITION_CAP = 20_000    # buffer below the limit
ONE_SIDED_AT = 5_000

SKEW_STEP = 2_000   # 1c skew per this many shares

STOP_LOSS = 0.06
COOLDOWN = 0

LOOP_PAUSE = 0.02
CASE_EVERY = 3

UNWIND_AT = 285
FLATTEN_AT = 297

BASE = f"http://{HOST}:{PORT}"
HEADERS = {"X-API-key": API_KEY, "Accept": "application/json"}
throttled = 0
requests_made = 0
SPEED_REPORT_EVERY = 10


# API

def request(method, path):
    """API call, retries once on 429."""
    global throttled, requests_made
    requests_made += 1
    for attempt in (1, 2):
        req = urllib.request.Request(BASE + path, headers=HEADERS, method=method)
        try:
            with urllib.request.urlopen(req, timeout=5) as reply:
                body = reply.read()
                return json.loads(body) if body else None
        except urllib.error.HTTPError as exc:
            if exc.code == 429 and attempt == 1:
                throttled += 1
                time.sleep(0.25)
                continue
            if method == "GET":
                raise
            if method != "DELETE":      # failed cancel = probably filled
                print(f"   ! {method} refused: HTTP {exc.code} {path}")
            return None
    return None


def get(path):
    return request("GET", path)


def post(path):
    return request("POST", path)


def send_limit(action, qty, price):
    return post(f"/v1/orders?ticker={TICKER}&type=LIMIT&action={action}"
                f"&quantity={int(qty)}&price={price:.2f}")


def send_market(action, qty):
    return post(f"/v1/orders?ticker={TICKER}&type=MARKET&action={action}"
                f"&quantity={int(qty)}")


def cancel_order(order_id):
    """False if the cancel failed (usually means it filled)."""
    return request("DELETE", f"/v1/orders/{order_id}") is not None


def cancel_all():
    post(f"/v1/commands/cancel?ticker={TICKER}")


def my_open_orders():
    orders = get("/v1/orders?status=OPEN") or []
    return [o for o in orders if o.get("ticker") == TICKER]


def remaining(order):
    return int(order.get("quantity", 0)) - int(order.get("quantity_filled", 0))


def others_best(my_ids, fallback_bid, fallback_ask):
    """Best bid/ask excluding our own orders."""
    book = get(f"/v1/securities/book?ticker={TICKER}&limit=20") or {}
    bids = [float(o["price"]) for o in book.get("bids", [])
            if o.get("order_id") not in my_ids and remaining(o) > 0]
    asks = [float(o["price"]) for o in book.get("asks", [])
            if o.get("order_id") not in my_ids and remaining(o) > 0]
    return (max(bids) if bids else fallback_bid,
            min(asks) if asks else fallback_ask)


# pricing

def cents(x):
    return round(x + 1e-9, 2)


def target_quotes(bid, ask, position, unwinding):
    """Returns {"BUY": (price, qty, join_price), "SELL": ...}.
    join_price = level tied with the best other order (keep our queue spot there)."""
    spread = cents(ask - bid)
    shift = -0.01 * int(position / SKEW_STEP)       # inventory skew

    # penny the book if there's room, otherwise join
    if spread >= 0.03:
        my_bid, my_ask = bid + 0.01, ask - 0.01
    else:
        my_bid, my_ask = bid, ask
    my_bid, my_ask = cents(my_bid + shift), cents(my_ask + shift)
    join_bid, join_ask = cents(bid + shift), cents(ask + shift)

    # don't cross (would pay commission instead of rebate)
    my_bid = min(my_bid, cents(ask - 0.01))
    my_ask = max(my_ask, cents(bid + 0.01))
    if my_ask <= my_bid:
        my_ask = cents(my_bid + 0.01)
    join_bid = min(join_bid, cents(ask - 0.01))
    join_ask = max(join_ask, cents(bid + 0.01))

    buy_qty = min(ORDER_SIZE, MAX_ORDER, POSITION_CAP - position)
    sell_qty = min(ORDER_SIZE, MAX_ORDER, POSITION_CAP + position)

    if position >= ONE_SIDED_AT:
        buy_qty = 0
    if position <= -ONE_SIDED_AT:
        sell_qty = 0

    # end of case: only reduce
    if unwinding:
        buy_qty = min(MAX_ORDER, -position) if position < 0 else 0
        sell_qty = min(MAX_ORDER, position) if position > 0 else 0

    want = {}
    if buy_qty >= 100:
        want["BUY"] = (my_bid, buy_qty, join_bid)
    if sell_qty >= 100:
        want["SELL"] = (my_ask, sell_qty, join_ask)
    return want


def manage_side(side, mine, target):
    """Update our order on one side. Returns True if anything changed."""
    if target is None:
        for o in mine:
            cancel_order(o["order_id"])
        return bool(mine)

    price, qty, join = target

    if len(mine) == 1:
        o = mine[0]
        px = cents(float(o["price"]))
        right_price = px in (price, join)
        right_size = remaining(o) <= qty
        if right_price and right_size:
            return False                        # keep queue position
        if not cancel_order(o["order_id"]):
            return False                        # filled
    elif len(mine) > 1:
        for o in mine:
            cancel_order(o["order_id"])
        return True

    send_limit(side, qty, price)
    return True


# closing

def flatten():
    cancel_all()
    position = int(get(f"/v1/securities?ticker={TICKER}")[0]["position"])
    if position == 0:
        print("flat.")
        return
    print(f"closing position {position:+d} at market ...")
    while position != 0:
        chunk = min(abs(position), MAX_ORDER)
        send_market("SELL" if position > 0 else "BUY", chunk)
        position += -chunk if position > 0 else chunk
    print("flat.")


# main

def main():
    print(f"connecting to {BASE} ...")
    try:
        case = get("/v1/case")
    except Exception as exc:
        print(f"\nCOULD NOT TALK TO {BASE}\n  {exc}")
        print("  A 401 means the API_KEY is wrong. Anything else usually means")
        print("  the PORT is wrong or the RIT API is switched off.")
        return
    print(f"connected. case {case['name']}, status {case['status']}")

    while case["status"] != "ACTIVE":
        print(f"waiting for the case to start (status {case['status']}) ...")
        time.sleep(1)
        case = get("/v1/case")

    print("clearing leftovers ...")
    try:
        flatten()
    except Exception as exc:
        print(f"  could not clear: {exc} - continuing")

    print("making markets\n")
    last_position = 0
    changes = 0
    kept = 0
    stops = 0
    cooldown_until, cooldown_side = -1, None
    loops = 0
    speed_t0, speed_loops, speed_reqs, speed_tick = time.time(), 0, requests_made, 0

    while True:
        try:
            if loops % CASE_EVERY == 0:
                case = get("/v1/case")
            tick = case["tick"]
            loops += 1

            if tick - speed_tick >= SPEED_REPORT_EVERY:
                secs = time.time() - speed_t0
                n = loops - speed_loops
                print(f"   [speed] {n / secs:.1f} checks/sec, "
                      f"{(requests_made - speed_reqs) / secs:.1f} requests/sec, "
                      f"orders kept in place {kept}/{kept + changes}")
                speed_t0, speed_loops, speed_reqs = time.time(), loops, requests_made
                speed_tick = tick

            if case["status"] == "STOPPED" or tick >= FLATTEN_AT:
                flatten()
                break
            if case["status"] != "ACTIVE":
                time.sleep(LOOP_PAUSE)
                continue

            sec = get(f"/v1/securities?ticker={TICKER}")[0]
            bid, ask = sec.get("bid"), sec.get("ask")
            position = int(sec.get("position") or 0)
            avg_cost = sec.get("vwap")

            if position != last_position:
                cost_txt = f"  avg cost {avg_cost:.3f}" if position and avg_cost else ""
                print(f"t={tick:3d}  position {position:+7d}  "
                      f"book {bid:.2f}/{ask:.2f}{cost_txt}")
                last_position = position

            # stop-loss vs avg cost
            if position and avg_cost and bid and ask:
                loss = (avg_cost - bid) if position > 0 else (ask - avg_cost)
                if loss >= STOP_LOSS:
                    cancel_all()
                    qty = min(abs(position), MAX_ORDER)
                    side = "SELL" if position > 0 else "BUY"
                    send_market(side, qty)
                    stops += 1
                    cooldown_until = tick + COOLDOWN
                    cooldown_side = "BUY" if position > 0 else "SELL"
                    print(f"   ! STOP: losing {loss:.2f}/share on {position:+d} "
                          f"(avg {avg_cost:.3f}), {side} {qty} at market")
                    time.sleep(LOOP_PAUSE)
                    continue

            # over cap -> cut at market
            if abs(position) > POSITION_CAP:
                cancel_all()
                excess = min(abs(position) - POSITION_CAP + ORDER_SIZE, MAX_ORDER)
                send_market("SELL" if position > 0 else "BUY", excess)
                print(f"   ! over cap, cut {excess} at market")
                time.sleep(LOOP_PAUSE)
                continue

            if not bid or not ask or ask <= bid:
                time.sleep(LOOP_PAUSE)
                continue

            open_orders = my_open_orders()
            my_ids = {o["order_id"] for o in open_orders}
            o_bid, o_ask = others_best(my_ids, bid, ask)
            if o_ask <= o_bid:
                time.sleep(LOOP_PAUSE)
                continue

            want = target_quotes(o_bid, o_ask, position, tick >= UNWIND_AT)
            if tick < cooldown_until:
                want.pop(cooldown_side, None)

            for side in ("BUY", "SELL"):
                mine = [o for o in open_orders if o["action"] == side]
                if manage_side(side, mine, want.get(side)):
                    changes += 1
                elif mine:
                    kept += 1

        except Exception as exc:
            print(f"   ! hiccup, skipping this cycle: {exc}")

        time.sleep(LOOP_PAUSE)

    print(f"\ndone. {changes} order changes, {stops} stop-loss cuts.")
    try:
        print(f"final NLV: {get('/v1/trader')['nlv']:,.2f}")
    except Exception:
        pass
    if throttled:
        print(f"rate-limited {throttled} times - raise LOOP_PAUSE")


if __name__ == "__main__":
    main()
