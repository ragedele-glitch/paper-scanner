"""Live-like fills for the PAPER-MODE scanner: price paper trades the way a real copy-bot trade would fill.

PAPER ONLY. Everything here is a read-only GET/POST of public data: Jupiter swap *quotes* (never a swap), Jupiter
prices, DexScreener liquidity and the public Solana RPC's recent priority fees. No wallet, no key, nothing is signed
or sent.

What a live account pays, and how each part is measured:
- Delay: the copy lands EXEC_DELAY_S after the scanner saw the wallet's buy. The fill uses the first real pool trade
  at or after that moment (GeckoTerminal trades), not the price the wallet got.
- Price impact + DEX/pool fees: a real Jupiter quote for the trade's own dollar size, both ways (buy, then sell the
  same tokens back). Half of the round-trip loss is charged on the way in, half on the way out; on exit the sell is
  quoted again at that moment for the exact token amount, so thin or drained pools cost what they really would.
  If Jupiter can't be reached, a constant-product pool estimate from DexScreener liquidity is used instead.
- Sell tax / honeypot: a token that Jupiter can buy but can't sell back is marked unsellable. If it still can't be
  sold at exit, the paper trade gets nothing back, as a live one would.
- Failed buys: if the price jumped more than COPY_SLIPPAGE_TOL between seeing the buy and the copy landing, a live
  bot's buy would fail on its slippage limit. The trade is not taken, and the failed transaction's fee is still paid.
- Copy-bot fee: BOT_FEE each way.
- Network: base fee + a priority fee read from the pool's recent transactions + a Jito tip, per transaction, in SOL
  converted at the live SOL price.
"""
import json, math, statistics, threading, time, urllib.request, urllib.error
import config as C

SOL = "So11111111111111111111111111111111111111112"
UA = "memecoin-paper-scanner/1.0 (read-only quotes, python-urllib)"
_lock = threading.Lock()
_state = {"jup_last": 0.0, "sol": (0.0, 0.0), "dec": {}, "prio": {}, "exitq": {}}


def _get(url, body=None, timeout=20):
    h = {"User-Agent": UA, "Accept": "application/json"}
    data = None
    if body is not None:
        h["Content-Type"] = "application/json"; data = json.dumps(body).encode()
    for i in range(2):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, data=data, headers=h), timeout=timeout) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504) and i == 0:
                time.sleep(3); continue
            if e.code in (400, 404):
                try:
                    return json.loads(e.read())
                except Exception:
                    return None
            raise
        except Exception:
            if i == 0:
                time.sleep(2); continue
            raise


def _jup(path):
    # keyless Jupiter allows about one request every 2 seconds
    with _lock:
        wait = _state["jup_last"] + 2.2 - time.time()
        if wait > 0:
            time.sleep(wait)
        _state["jup_last"] = time.time()
    return _get("https://lite-api.jup.ag" + path)


def sol_usd():
    t, px = _state["sol"]
    if time.time() - t < 60 and px > 0:
        return px
    try:
        d = _jup(f"/price/v3?ids={SOL}")
        px = float(d[SOL]["usdPrice"])
        _state["sol"] = (time.time(), px)
    except Exception:
        if not px:
            px = C.SOL_USD_FALLBACK
    return px


def decimals(mint):
    d = _state["dec"].get(mint)
    if d is not None:
        return d
    try:
        j = _jup(f"/price/v3?ids={mint}")
        d = int(j[mint]["decimals"])
    except Exception:
        d = 6   # pump.fun coins all use 6
    _state["dec"][mint] = d
    return d


def quote(inp, out, amount_raw):
    """A real Jupiter quote (read-only). Returns outAmount as int, or None if Jupiter found no route."""
    amount_raw = int(amount_raw)
    if amount_raw <= 0:
        return None
    q = _jup(f"/swap/v1/quote?inputMint={inp}&outputMint={out}&amount={amount_raw}"
             f"&slippageBps={int(C.COPY_SLIPPAGE_TOL * 10000)}&swapMode=ExactIn")
    if not q or not q.get("outAmount") or not q.get("routePlan"):
        return None
    return int(q["outAmount"]), "+".join(sorted({r["swapInfo"].get("label") or "?" for r in q["routePlan"]}))


def amm_ratios(liq_usd, size_usd):
    """Fallback: constant-product pool with half its liquidity on each side, plus the pool fee."""
    R = max(1.0, (liq_usd or 0) / 2)
    r_buy = (1 + C.POOL_FEE) * (1 + size_usd / R)
    v = size_usd / r_buy
    r_sell = (1 - C.POOL_FEE) * R / (R + v)
    return r_buy, r_sell


def priority_fee_sol(pool):
    """Median non-zero priority fee paid recently by transactions touching this pool, for one swap."""
    e = _state["prio"].get(pool)
    if e and time.time() - e[0] < 120:
        return e[1]
    micro = None
    try:
        j = _get(C.RPC_URL, {"jsonrpc": "2.0", "id": 1, "method": "getRecentPrioritizationFees", "params": [[pool]] if pool else []})
        v = [x["prioritizationFee"] for x in (j or {}).get("result") or [] if x.get("prioritizationFee")]
        micro = statistics.median(v) if v else 0
    except Exception:
        pass
    if micro is None:
        micro = C.PRIORITY_MICROLAMPORTS_FALLBACK
    sol = min(C.PRIORITY_MAX_SOL, micro * C.SWAP_COMPUTE_UNITS / 1e6 / 1e9)
    _state["prio"][pool] = (time.time(), sol)
    return sol


def tx_fee_usd(pool, landed=True):
    """Base fee + priority fee (+ Jito tip when the transaction lands) in dollars."""
    sol = C.BASE_FEE_LAMPORTS / 1e9 + priority_fee_sol(pool) + (C.JITO_TIP_SOL if landed else 0)
    return sol * sol_usd()


def entry_quote(mint, pool, size_usd, liq_usd):
    """Round trip at entry for this dollar size: buy with SOL, then sell the tokens back.
    Returns r_buy (fill price / mid), r_sell_est (sell proceeds / mid), round-trip loss %, sellable flag, source."""
    out = {"liq": liq_usd, "src": "jupiter", "sellable": True, "route": None}
    try:
        sp = sol_usd()
        lam = size_usd / sp * 1e9
        b = quote(SOL, mint, lam)
        if b:
            s = quote(mint, SOL, b[0])
            out["route"] = b[1]
            if s:
                rt = s[0] / lam
                out.update(rt_pct=(1 - rt) * 100, r_buy=1 / math.sqrt(max(rt, 1e-9)), r_sell=math.sqrt(max(rt, 1e-9)))
                return out
            out.update(sellable=False, rt_pct=100.0, src="jupiter (no sell route)")
            rb, _ = amm_ratios(liq_usd, size_usd)
            out.update(r_buy=rb, r_sell=0.0)
            return out
    except Exception as e:
        out["err"] = f"{type(e).__name__}"
    rb, rs = amm_ratios(liq_usd, size_usd)
    out.update(src="pool estimate", r_buy=rb, r_sell=rs, rt_pct=(1 - rs / rb) * 100)
    return out


def exit_quote(mint, qty, liq_usd, sellable_before=True):
    """At exit: quote selling exactly qty tokens now, and buying them back, for the exit-side cost.
    One quote per coin serves the $200 account and the flat-$20 test for 60 s."""
    key = mint
    e = _state["exitq"].get(key)
    if e and time.time() - e[0] < 60 and 0.7 < qty / max(e[1], 1e-18) < 1.4:
        return dict(e[2])
    out = {"liq": liq_usd, "src": "jupiter"}
    try:
        raw = qty * 10 ** decimals(mint)
        s = quote(mint, SOL, raw)
        if s:
            b = quote(SOL, mint, s[0])
            rt = (b[0] / raw) if b else None
            if rt:
                out["r_sell"] = math.sqrt(max(rt, 1e-9))
            else:   # sell route but no buy-back quote: use the sell against the pool estimate's mid
                out["r_sell"] = amm_ratios(liq_usd, 20)[1]; out["src"] = "jupiter sell + pool estimate"
        elif (liq_usd is not None and liq_usd < C.MIN_SELLABLE_LIQ_USD) or not sellable_before:
            out.update(r_sell=0.0, src="no sell route, no liquidity")
        else:
            out = None
    except Exception as ex:
        out = None
    if out is None:
        out = {"liq": liq_usd, "src": "pool estimate"}
        if liq_usd is not None and liq_usd < C.MIN_SELLABLE_LIQ_USD:
            out["r_sell"] = 0.0; out["src"] = "no liquidity left"
        else:
            out["r_sell"] = amm_ratios(liq_usd or C.LIQ_MIN_USD, 20)[1]
    _state["exitq"][key] = (time.time(), qty, dict(out))
    return out


def buy_qty(size_usd, mid, r_buy):
    return size_usd * (1 - C.BOT_FEE) / (mid * r_buy)


def sell_value(qty, px, r_sell, fee_usd):
    return max(0.0, qty * px * r_sell * (1 - C.BOT_FEE) - fee_usd)


def position_value(p, px):
    """What an open paper position would fetch if sold at px. Live-like positions use their measured sell cost."""
    keys = p.keys() if hasattr(p, "keys") else p
    if "fill_model" in keys and p["fill_model"] == "live":
        return sell_value(p["qty"] or 0, px or 0, p["r_sell_est"] or 0, p["fee_in"] or 0)
    return max(0.0, (p["qty"] or 0) * (px or 0) * (1 - C.SLIPPAGE) * (1 - C.BOT_FEE - C.POOL_FEE) - C.NETWORK_FEE_USD)
