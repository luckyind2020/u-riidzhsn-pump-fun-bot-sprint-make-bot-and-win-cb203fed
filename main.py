#!/usr/bin/env python3
"""Pump.fun replay paper-trading bot -- agent-fun/replay/v2.

Reads newline-delimited JSON from stdin (hello, batch, done) and writes one
reply per batch on stdout. Fully deterministic: every decision is a pure
function of the event stream so far -- no randomness, no wall clock, no
network, no environment reads. Two runs over the same replay therefore
produce byte-identical action logs.

Strategy in one paragraph: probe young bonding curves that show real early
buy pressure, risk a small fixed slice per coin, and let winners run behind
a trailing stop while losers are cut fast. A coin qualifies only when buys
dominate sells since creation, several distinct buyers showed up, the price
already lifted off its creation level, the creator has not sold, and the
curve is nowhere near graduation. Exits are a hard take-profit, a hard
stop-loss, a trailing stop once the trade is comfortably green, a max hold
time, and an immediate exit when the curve completes.

Money facts that shape the numbers: the house starts the bot with 10 SOL,
caps open positions at 5, caps one order at 100 bps of a curve's real SOL
reserves, and charges roughly 95 bps protocol + 30 bps creator fee plus
105,000 lamports of tx fees per trade. A round trip therefore costs about
3% of the stake, so the probe is sized at 0.04 SOL, the stop sits at -15%
and the take-profit at +100%: a single winner pays for several losers.
"""

import json
import sys

# ---------------------------------------------------------------------------
# Tunables (lamports unless noted; 1 SOL = 1_000_000_000 lamports)
# ---------------------------------------------------------------------------
PROBE_LAMPORTS = 40_000_000          # 0.04 SOL per probe buy
MIN_PROBE_LAMPORTS = 15_000_000      # below this a probe is not worth the fees
MAX_ENTRY_AGE_SLOTS = 1_800          # coin must be this young to enter (~12 min)
MIN_BUY_SOL = 250_000_000           # cumulative buy volume since creation
MIN_BUY_SELL_RATIO = 2.0             # buys must dominate sells this much
MIN_DISTINCT_BUYERS = 4              # distinct wallets buying since creation
MAX_TOP_BUYER_SHARE = 0.60           # skip when one wallet drove >60% of buys
MIN_TRADES = 8                       # total trades since creation
MIN_LIFT = 1.10                      # price vs creation price to qualify
MAX_REAL_SOL_AT_ENTRY = 40_000_000_000  # skip curves near graduation (40 SOL)
TP_MULT = 2.00                       # take profit at +100%
SL_MULT = 0.85                       # stop loss at -15%
TRAIL_ARM_MULT = 1.25                # arm the trailing stop at +25%
TRAIL_MULT = 0.85                    # exit at 15% below the peak once armed
MAX_HOLD_SLOTS = 2_400               # max hold, about 16 minutes
STALE_MARK_BATCHES = 30              # exit if the curve stops printing marks
PENDING_AGE_BATCHES = 10             # forget an order with no fill after this
PRUNE_AFTER_SLOTS = 30_000           # drop dead coins from memory after this
BUY_SLIPPAGE_BPS = 500
SELL_SLIPPAGE_BPS = 500
MIN_CASH_RESERVE = 500_000_000       # keep 0.5 SOL of cash untouched


def lamports(value):
    """Parse a u64 decimal string (or int) into an int; missing -> 0."""
    if value is None:
        return 0
    if isinstance(value, int):
        return value
    return int(value)


def curve_price(virtual_sol, virtual_token):
    """SOL per token from virtual reserves; 0.0 when unusable."""
    if virtual_token <= 0 or virtual_sol <= 0:
        return 0.0
    return virtual_sol / virtual_token


class Coin:
    """Per-mint state, kept small: one object per curve seen."""

    __slots__ = (
        "mint", "creator", "created_slot", "create_price", "first_slot",
        "buy_sol", "sell_sol", "buy_trades", "sell_trades", "buyers",
        "buyer_vol", "creator_sold", "completed", "mayhem", "last_price", "last_slot",
        "real_sol", "last_mark_batch", "entry_slot", "entry_price",
        "peak_price", "batches_no_mark",
    )

    def __init__(self, mint, slot):
        self.mint = mint
        self.creator = None
        self.created_slot = slot
        self.create_price = 0.0
        self.first_slot = slot
        self.buy_sol = 0
        self.sell_sol = 0
        self.buy_trades = 0
        self.sell_trades = 0
        self.buyers = set()
        self.buyer_vol = {}
        self.creator_sold = False
        self.completed = False
        self.mayhem = False
        self.last_price = 0.0
        self.last_slot = slot
        self.real_sol = 0
        self.last_mark_batch = -1
        self.entry_slot = 0
        self.entry_price = 0.0
        self.peak_price = 0.0
        self.batches_no_mark = 0


class Bot:
    def __init__(self):
        self.capital = 10_000_000_000
        self.max_open_positions = 5
        self.max_order_bps = 100
        self.max_orders_per_batch = 16
        self.latency_slots = 2
        self.coins = {}
        self.batch_no = 0
        self.order_counter = 0
        self.pending_buys = {}   # client_id -> (mint, batch_no)
        self.pending_sells = {}  # client_id -> (mint, batch_no)
        self.pending_buy_mints = set()
        self.pending_sell_mints = set()
        self.exited = set()      # mints already traded once: never re-enter

    # -- protocol ---------------------------------------------------------
    def on_hello(self, msg):
        capital = msg.get("capital") or {}
        self.capital = lamports(capital.get("lamports", self.capital))
        caps = msg.get("caps") or {}
        self.max_open_positions = int(caps.get("max_open_positions", 5))
        self.max_order_bps = int(caps.get("max_order_bps_of_real_reserve", 100))
        self.max_orders_per_batch = int(caps.get("max_orders_per_batch", 16))
        self.latency_slots = int(msg.get("latency_slots", 2))

    def coin_for(self, mint, slot):
        coin = self.coins.get(mint)
        if coin is None:
            coin = Coin(mint, slot)
            self.coins[mint] = coin
        return coin

    def on_event(self, ev, slot):
        kind = ev.get("event")
        mint = ev.get("mint")
        if not mint:
            return
        if kind == "CreateEvent":
            coin = self.coin_for(mint, slot)
            coin.creator = ev.get("creator")
            coin.created_slot = slot
            coin.create_price = curve_price(
                lamports(ev.get("virtual_sol_reserves", "0")),
                lamports(ev.get("virtual_token_reserves", "0")),
            )
            coin.last_price = coin.create_price
            if ev.get("is_mayhem_mode"):
                coin.mayhem = True
        elif kind == "TradeEvent":
            coin = self.coin_for(mint, slot)
            sol = lamports(ev.get("sol_amount", "0"))
            if ev.get("mayhem_mode"):
                coin.mayhem = True
            if ev.get("is_buy"):
                coin.buy_sol += sol
                coin.buy_trades += 1
                user = ev.get("user")
                if len(coin.buyers) < 8:
                    coin.buyers.add(user)
                # Track per-wallet buy volume (capped) to spot one-wallet
                # wash flow: a single wallet driving most buys is not
                # organic demand.
                if user in coin.buyer_vol:
                    coin.buyer_vol[user] += sol
                elif len(coin.buyer_vol) < 32:
                    coin.buyer_vol[user] = sol
            else:
                coin.sell_sol += sol
                coin.sell_trades += 1
                if coin.creator is not None and ev.get("user") == coin.creator:
                    coin.creator_sold = True
            price = curve_price(
                lamports(ev.get("virtual_sol_reserves", "0")),
                lamports(ev.get("virtual_token_reserves", "0")),
            )
            if price > 0.0:
                coin.last_price = price
            coin.real_sol = lamports(ev.get("real_sol_reserves", "0"))
            coin.last_slot = slot
        elif kind == "CompleteEvent":
            coin = self.coin_for(mint, slot)
            coin.completed = True
            coin.last_slot = slot

    def on_mark(self, mark, slot):
        mint = mark.get("mint")
        if not mint:
            return
        coin = self.coins.get(mint)
        if coin is None:
            return
        price = curve_price(
            lamports(mark.get("virtual_quote_reserves", "0")),
            lamports(mark.get("virtual_token_reserves", "0")),
        )
        if price > 0.0:
            coin.last_price = price
        coin.real_sol = lamports(mark.get("real_quote_reserves", "0"))
        coin.last_mark_batch = self.batch_no
        coin.batches_no_mark = 0
        coin.last_slot = slot

    def on_fill(self, fill):
        client_id = fill.get("client_id")
        if client_id in self.pending_buys:
            del self.pending_buys[client_id]
            mint = fill.get("mint")
            self.pending_buy_mints.discard(mint)
        if client_id in self.pending_sells:
            del self.pending_sells[client_id]
            mint = fill.get("mint")
            self.pending_sell_mints.discard(mint)

    # -- decisions --------------------------------------------------------
    def next_client_id(self, prefix):
        self.order_counter += 1
        return "%s%d" % (prefix, self.order_counter)

    def entry_size(self, coin):
        """Probe size clamped to the per-order reserve cap, or 0 to skip."""
        cap = coin.real_sol * self.max_order_bps // 10_000
        # Stay a hair under the cap so a fill can never trip it.
        cap = cap * 90 // 100
        size = PROBE_LAMPORTS if PROBE_LAMPORTS < cap else cap
        if size < MIN_PROBE_LAMPORTS:
            return 0
        return size

    def qualifies(self, coin, slot, cash):
        if coin.mayhem or coin.completed or coin.creator_sold:
            return False
        if coin.creator is None and coin.buy_trades < MIN_TRADES:
            return False
        if slot - coin.created_slot > MAX_ENTRY_AGE_SLOTS:
            return False
        if coin.buy_trades + coin.sell_trades < MIN_TRADES:
            return False
        if len(coin.buyers) < MIN_DISTINCT_BUYERS:
            return False
        if coin.buy_sol < MIN_BUY_SOL:
            return False
        if coin.sell_sol > 0 and coin.buy_sol < MIN_BUY_SELL_RATIO * coin.sell_sol:
            return False
        if coin.sell_sol == 0 and coin.buy_sol < MIN_BUY_SOL:
            return False
        # One wallet must not dominate the buy flow.
        if coin.buyer_vol and coin.buy_sol > 0:
            top = max(coin.buyer_vol.values())
            if top > MAX_TOP_BUYER_SHARE * coin.buy_sol:
                return False
        if coin.create_price > 0.0 and coin.last_price < MIN_LIFT * coin.create_price:
            return False
        if coin.last_price <= 0.0:
            return False
        if coin.real_sol > MAX_REAL_SOL_AT_ENTRY:
            return False
        if cash < MIN_CASH_RESERVE + PROBE_LAMPORTS:
            return False
        return True

    def decide(self, msg):
        """Build the reply orders for one batch. Pure function of state."""
        seq = msg.get("seq", 0)
        slot = int(msg.get("slot", 0))
        account = msg.get("account") or {}
        cash = lamports(account.get("cash_lamports", "0"))
        held = {}
        for pos in account.get("positions") or []:
            mint = pos.get("mint")
            tokens = lamports(pos.get("token_amount", "0"))
            cost = lamports(pos.get("cost_lamports", "0"))
            if mint and tokens > 0:
                held[mint] = (tokens, cost)

        # Age out pending orders that never produced a fill.
        for cid in [c for c, (_, b) in self.pending_buys.items()
                    if self.batch_no - b > PENDING_AGE_BATCHES]:
            mint = self.pending_buys.pop(cid)[0]
            self.pending_buy_mints.discard(mint)
        for cid in [c for c, (_, b) in self.pending_sells.items()
                    if self.batch_no - b > PENDING_AGE_BATCHES]:
            mint = self.pending_sells.pop(cid)[0]
            self.pending_sell_mints.discard(mint)

        orders = []

        # --- exits first: free slots before considering new entries. ---
        for mint in sorted(held):
            tokens, cost = held[mint]
            coin = self.coins.get(mint)
            if coin is None:
                coin = self.coin_for(mint, slot)
            if mint in self.pending_sell_mints:
                continue
            entry_price = cost / tokens if tokens > 0 else 0.0
            if coin.entry_price <= 0.0:
                coin.entry_price = entry_price
                coin.entry_slot = slot
                coin.peak_price = entry_price
            price = coin.last_price
            if price <= 0.0:
                price = entry_price
            if price > coin.peak_price:
                coin.peak_price = price
            if coin.last_mark_batch != self.batch_no:
                coin.batches_no_mark += 1
            else:
                coin.batches_no_mark = 0
            should_sell = False
            if coin.completed:
                should_sell = True
            elif entry_price > 0.0 and price >= TP_MULT * entry_price:
                should_sell = True
            elif entry_price > 0.0 and price <= SL_MULT * entry_price:
                should_sell = True
            elif (entry_price > 0.0
                    and coin.peak_price >= TRAIL_ARM_MULT * entry_price
                    and price <= TRAIL_MULT * coin.peak_price):
                should_sell = True
            elif coin.entry_slot and slot - coin.entry_slot > MAX_HOLD_SLOTS:
                should_sell = True
            elif coin.batches_no_mark > STALE_MARK_BATCHES:
                should_sell = True
            if should_sell and len(orders) < self.max_orders_per_batch:
                cid = self.next_client_id("s")
                orders.append({
                    "side": "sell",
                    "client_id": cid,
                    "mint": mint,
                    "token_amount": str(tokens),
                    "max_slippage_bps": SELL_SLIPPAGE_BPS,
                })
                self.pending_sells[cid] = (mint, self.batch_no)
                self.pending_sell_mints.add(mint)
                self.exited.add(mint)

        # --- entries: young curves with real buy pressure. ---
        open_count = len(held) + len(self.pending_buy_mints)
        if open_count < self.max_open_positions and cash > MIN_CASH_RESERVE:
            candidates = []
            for mint, coin in self.coins.items():
                if mint in held or mint in self.pending_buy_mints:
                    continue
                if mint in self.pending_sell_mints or mint in self.exited:
                    continue
                if self.qualifies(coin, slot, cash):
                    candidates.append(coin)
            # Oldest first: the earliest momentum gets the probe.
            candidates.sort(key=lambda c: (c.created_slot, c.mint))
            for coin in candidates:
                if open_count >= self.max_open_positions:
                    break
                if len(orders) >= self.max_orders_per_batch:
                    break
                size = self.entry_size(coin)
                if size <= 0:
                    continue
                if cash < MIN_CASH_RESERVE + size:
                    continue
                cid = self.next_client_id("b")
                orders.append({
                    "side": "buy",
                    "client_id": cid,
                    "mint": coin.mint,
                    "quote_lamports": str(size),
                    "max_slippage_bps": BUY_SLIPPAGE_BPS,
                })
                self.pending_buys[cid] = (coin.mint, self.batch_no)
                self.pending_buy_mints.add(coin.mint)
                cash -= size
                open_count += 1

        # --- prune dead coins so memory stays bounded over 24h. ---
        if self.batch_no % 50 == 0:
            cutoff = slot - PRUNE_AFTER_SLOTS
            for mint in [m for m, c in self.coins.items()
                         if c.last_slot < cutoff
                         and m not in held
                         and m not in self.pending_buy_mints
                         and m not in self.pending_sell_mints]:
                del self.coins[mint]

        return {"type": "reply", "seq": seq, "orders": orders}

    # -- main loop ----------------------------------------------------------
    def run(self):
        out = sys.stdout
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except ValueError:
                continue
            mtype = msg.get("type")
            if mtype == "hello":
                self.on_hello(msg)
            elif mtype == "batch":
                # Count the batch first: marks processed below stamp
                # last_mark_batch with this number, and decide() reads it
                # in the same batch to tell fresh marks from stale ones.
                self.batch_no += 1
                slot = int(msg.get("slot", 0))
                try:
                    for ev in msg.get("events") or []:
                        self.on_event(ev, slot)
                    for mark in msg.get("marks") or []:
                        self.on_mark(mark, slot)
                    for fill in msg.get("fills") or []:
                        self.on_fill(fill)
                except Exception:
                    pass
                try:
                    reply = self.decide(msg)
                except Exception:
                    reply = {"type": "reply",
                             "seq": msg.get("seq", 0), "orders": []}
                out.write(json.dumps(reply, separators=(",", ":")) + "\n")
                out.flush()
            elif mtype == "done":
                break


def main():
    Bot().run()


if __name__ == "__main__":
    main()
