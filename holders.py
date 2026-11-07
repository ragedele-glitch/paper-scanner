"""On-chain holder numbers for the PAPER-MODE scanner: top-10 holder filter, creator and early-buyer numbers.

Read-only public Solana RPC calls: no wallet, no keys, nothing signed.

- top10(): run when a coin passes the checks, before the paper buys. Reads the 20 biggest token accounts, finds
  their owners, and leaves out the pump.fun bonding curve and the copied wallet itself. The shadow 'holder filter'
  account skips pump.fun curve coins where the top 10 of the rest own HF_TOP10_MAX % or more of the supply.
  (Found in the rug backtest of Nov 4, 2026: rugs BASS 26.7% and it 32.4%; no runner, winner or clean stop above 21.2%.)
- chain_info(): logged on every new trade, in a background thread so it never delays a paper buy. Creator
  (from the bonding curve account), how much the creator still holds, and the coin's first-block buyers
  (bundle): how many, how much they bought and how much they still hold.
"""
import base64
import json
import threading
import time
import urllib.request
from collections import defaultdict

import config as C

# getTokenLargestAccounts is refused (429) by the free Solana Foundation nodes most of the time, so the holder
# list is read from other free public nodes first.
EPS_HOLDERS = ["https://public.rpc.solanavibestation.com", "https://solana-mainnet.gateway.tatum.io",
               C.RPC_URL, "https://api.mainnet.solana.com"]
EPS_ACCTS = ["https://solana-mainnet.gateway.tatum.io", C.RPC_URL, "https://api.mainnet.solana.com",
             "https://public.rpc.solanavibestation.com"]
# History reads need nodes that keep the full ledger. The vibestation node keeps only hours of history, which made
# old wallets look new and could hide a coin's first block (fixed Nov 7, 2026).
EPS_HIST = [C.RPC_URL, "https://api.mainnet.solana.com"]
PUMP_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

_lock = threading.Lock()
_next = defaultdict(float)     # endpoint -> earliest next call
_cool = defaultdict(float)     # endpoint -> rate-limited until


def b58(b):
    n = int.from_bytes(b, "big"); s = ""
    while n:
        n, r = divmod(n, 58); s = B58[r] + s
    return "1" * (len(b) - len(b.lstrip(b"\0"))) + s


def call(method, params, eps, tries=6, timeout=15):
    last = None
    for i in range(tries):
        with _lock:
            t = time.time()
            e = min(eps, key=lambda x: (_cool[x] > t, max(_next[x], _cool[x])))
            wait = max(_next[e], _cool[e]) - t
            _next[e] = max(t, _next[e]) + 0.4
        if wait > 0:
            time.sleep(min(wait, 10))
        try:
            req = urllib.request.Request(e, data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode(),
                                         headers={"Content-Type": "application/json", "User-Agent": "paper-scanner"})
            d = json.load(urllib.request.urlopen(req, timeout=timeout))
            if d.get("error"):
                last = f"{d['error'].get('code')} {str(d['error'].get('message'))[:60]}"
                with _lock:
                    _cool[e] = time.time() + 5
                continue
            return d["result"]
        except Exception as ex:
            last = f"{type(ex).__name__} {str(ex)[:60]}"
            with _lock:
                _cool[e] = time.time() + (8 if "429" in str(ex) else 4)
    raise RuntimeError(f"{method} failed: {last}")


def _owners(addrs):
    res = call("getMultipleAccounts", [addrs, {"encoding": "jsonParsed", "commitment": "confirmed"}], EPS_ACCTS, tries=3, timeout=8)
    out = {}
    for a, v in zip(addrs, res["value"]):
        try:
            out[a] = v["data"]["parsed"]["info"]["owner"]
        except Exception:
            out[a] = None
    return out


def top10(mint, curve, leader):
    """Top-10 holders' share of supply, leaving out the bonding curve and the copied wallet. Percent of supply."""
    t0 = time.time()
    # quick: this runs before the paper buy (prices come from the detection time, so a delay changes no price)
    big = call("getTokenLargestAccounts", [mint, {"commitment": "confirmed"}], EPS_HOLDERS, tries=4, timeout=8)["value"]
    sup = call("getTokenSupply", [mint, {"commitment": "confirmed"}], EPS_ACCTS, tries=3, timeout=8)["value"]
    supply = float(sup["amount"])
    if not big or supply <= 0:
        raise RuntimeError("no holder data")
    own = _owners([x["address"] for x in big])
    by = defaultdict(float)
    for x in big:
        by[own.get(x["address"]) or x["address"]] += float(x["amount"])
    curve_amt = by.pop(curve, 0.0) if curve else 0.0
    leader_amt = by.pop(leader, 0.0) if leader else 0.0
    top = sorted(((v, o) for o, v in by.items() if v > 0), reverse=True)
    pct = lambda v: round(v / supply * 100, 2)
    return {"top10": pct(sum(v for v, _ in top[:10])), "top1": pct(top[0][0]) if top else 0.0,
            "curve_pct": pct(curve_amt), "leader_pct": pct(leader_amt), "counted": min(10, len(top)),
            "top_owners": [o for _, o in top[:10]], "ts": round(t0, 1), "secs": round(time.time() - t0, 1)}


SYSTEM_PROGRAM = "11111111111111111111111111111111"
_P = 2 ** 255 - 19
_D = (-121665 * pow(121666, _P - 2, _P)) % _P


def _b58dec(s):
    n = 0
    for ch in s:
        n = n * 58 + B58.index(ch)
    b = n.to_bytes((n.bit_length() + 7) // 8, "big") if n else b""
    return (b"\0" * (len(s) - len(s.lstrip("1"))) + b).rjust(32, b"\0")


def on_curve(addr):
    """True for a normal wallet address (an ed25519 point). Program-derived addresses (pool authorities, vaults,
    lockers) are off the curve, so they are not people holding the coin."""
    try:
        b = _b58dec(addr)
        y = int.from_bytes(b, "little") & ((1 << 255) - 1)
        if y >= _P:
            return False
        u = (y * y - 1) % _P; v = (_D * y * y + 1) % _P
        x2 = u * pow(v, _P - 2, _P) % _P
        return x2 == 0 or pow(x2, (_P - 1) // 2, _P) == 1
    except Exception:
        return True


def _funder(tx, wallet):
    """The account that sent the wallet its SOL in this transaction (biggest SOL drop), or None."""
    keys = [k["pubkey"] if isinstance(k, dict) else k for k in tx["transaction"]["message"]["accountKeys"]]
    m = tx["meta"]; pre, post = m.get("preBalances") or [], m.get("postBalances") or []
    if wallet not in keys or len(pre) != len(keys) or len(post) != len(keys):
        return None
    i = keys.index(wallet)
    if post[i] <= pre[i]:
        return None
    drops = [(pre[j] - post[j], keys[j]) for j in range(len(keys)) if j != i and pre[j] - post[j] > 0]
    return max(drops)[1] if drops else None


def holder_extra(mint, curve, leader, pool, det_ts, max_wallets=10):
    """Stronger holder checks, LOGGED ONLY (Nov 7, 2026 test from Grok's rug-escape ideas): nothing here blocks or
    sells anything on any account. Biggest single real wallet (leaving out the bonding curve, the pool, the copied
    wallet and any program-owned account such as an AMM vault), how many real wallets hold over 8% / 10%, how many of
    the top-10 real wallets are fresh (first transaction under 24 h / 48 h before detection), and how many share the
    same funder (the wallet that sent them their first SOL) = a likely bundle. Read in the background thread."""
    t0 = time.time(); out = {"v": 1, "notes": []}
    big = call("getTokenLargestAccounts", [mint, {"commitment": "confirmed"}], EPS_HOLDERS, tries=4, timeout=10)["value"]
    supply = float(call("getTokenSupply", [mint, {"commitment": "confirmed"}], EPS_ACCTS, tries=3, timeout=10)["value"]["amount"])
    if not big or supply <= 0:
        raise RuntimeError("no holder data")
    own = _owners([x["address"] for x in big])
    by = defaultdict(float)
    for x in big:
        by[own.get(x["address"]) or x["address"]] += float(x["amount"])
    skip = {a for a in (curve, leader, pool) if a}
    owners = [o for o in by if o not in skip]
    # program-owned owners (AMM / pool vault authorities, lockers) are not people: leave them out
    prog = {o for o in owners if not on_curve(o)}
    try:
        res = call("getMultipleAccounts", [owners, {"encoding": "base64", "commitment": "confirmed"}], EPS_ACCTS, tries=3, timeout=10)
        for o, v in zip(owners, res["value"]):
            if v and v.get("owner") != SYSTEM_PROGRAM:
                prog.add(o)
    except Exception as e:
        out["notes"].append(f"owner types not read: {str(e)[:60]}")
    pct = lambda v: round(v / supply * 100, 2)
    top = sorted(((v, o) for o, v in by.items() if o not in skip and o not in prog and v > 0), reverse=True)[:max_wallets]
    out["excluded_program_pct"] = pct(sum(by[o] for o in prog))
    out["top_wallets"] = [[o, pct(v)] for v, o in top]
    out["top1_wallet_pct"] = pct(top[0][0]) if top else 0.0
    out["n_over_8"] = sum(1 for v, _ in top if pct(v) > 8)
    out["n_over_10"] = sum(1 for v, _ in top if pct(v) > 10)
    # wallet age and first funder
    ages = {}; funders = {}
    for v, o in top:
        try:
            sigs = call("getSignaturesForAddress", [o, {"limit": 1000}], EPS_HIST, tries=4, timeout=15)
        except Exception:
            continue
        bts = [s["blockTime"] for s in sigs if s.get("blockTime")]
        if not bts:
            continue
        oldest = min(bts)
        if len(sigs) < 1000:
            ages[o] = round((det_ts - oldest) / 3600, 1)          # full history: real age in hours
            for s in [x for x in reversed(sigs) if not x.get("err")][:3]:   # first SOL in, from the oldest txs
                try:
                    tx = call("getTransaction", [s["signature"], {"encoding": "jsonParsed",
                              "maxSupportedTransactionVersion": 1, "commitment": "confirmed"}], EPS_HIST, tries=3, timeout=20)
                    f = _funder(tx, o) if tx else None
                except Exception:
                    f = None
                if f:
                    funders[o] = f
                    break
        elif det_ts - oldest > 48 * 3600:
            ages[o] = round((det_ts - oldest) / 3600, 1)          # at least this old (very busy wallet)
        else:
            ages[o] = None                                        # 1000+ txs inside 48 h: age not known
    out["ages_h"] = ages
    out["age_read"] = sum(1 for a in ages.values() if a is not None)
    out["fresh24_n"] = sum(1 for a in ages.values() if a is not None and a < 24)
    out["fresh48_n"] = sum(1 for a in ages.values() if a is not None and a < 48)
    pv = {o: pct(v) for v, o in top}
    out["fresh48_pct"] = round(sum(pv[o] for o, a in ages.items() if a is not None and a < 48), 2)
    grp = defaultdict(list)
    for o, f in funders.items():
        grp[f].append(o)
    best = max(grp.values(), key=len) if grp else []
    out["funders"] = funders
    out["same_funder_n"] = len(best) if len(best) >= 2 else 0
    out["same_funder_pct"] = round(sum(pv[o] for o in best), 2) if len(best) >= 2 else 0.0
    out["funder_read"] = len(funders)
    out["flags"] = [k for k, on in (
        ("wallet_over_8", out["top1_wallet_pct"] > 8), ("wallet_over_10", out["top1_wallet_pct"] > 10),
        ("fresh24_2plus", out["fresh24_n"] >= 2), ("fresh48_2plus", out["fresh48_n"] >= 2),
        ("bundle_2plus", out["same_funder_n"] >= 2)) if on]
    out["secs"] = round(time.time() - t0, 1)
    if not out["notes"]:
        out.pop("notes")
    return out


def chain_and_holder_info(mint, curve, leader, det_ts, is_curve, pool=None):
    """chain_info() plus the logged-only stronger holder checks under 'hx'. Either part failing leaves a note."""
    try:
        out = chain_info(mint, curve, leader, det_ts, is_curve)
    except Exception as e:
        out = {"error": str(e)[:200]}
    try:
        out["hx"] = holder_extra(mint, curve if is_curve else None, leader, pool, det_ts)
    except Exception as e:
        out["hx"] = {"error": str(e)[:200]}
    try:   # creator (dev) track record, logged only (Rage, Nov 7 shadow test); never blocks or sells
        import creators
        out["cr"] = creators.creator_info(mint, det_ts)
    except Exception as e:
        out["cr"] = {"flag": "unknown", "why": str(e)[:200]}
    return out


def _balance(owner, mint):
    res = call("getTokenAccountsByOwner", [owner, {"mint": mint}, {"encoding": "jsonParsed", "commitment": "confirmed"}], EPS_ACCTS)
    return sum(float(a["account"]["data"]["parsed"]["info"]["tokenAmount"]["amount"]) for a in res["value"])


def _creator_from_curve(curve):
    v = call("getAccountInfo", [curve, {"encoding": "base64", "commitment": "confirmed"}], EPS_ACCTS)["value"]
    if not v or v.get("owner") != PUMP_PROGRAM:
        return None, None
    raw = base64.b64decode(v["data"][0])
    if len(raw) < 81:
        return None, None
    real_tokens = int.from_bytes(raw[24:32], "little")
    progress = round((1 - real_tokens / 793_100_000_000_000) * 100, 1)   # % of the curve sold
    return b58(raw[49:81]), progress


def _deltas(tx, mint):
    m = tx["meta"]; d = defaultdict(float)
    for b in m.get("postTokenBalances") or []:
        if b.get("mint") == mint and b.get("owner"):
            d[b["owner"]] += float(b["uiTokenAmount"]["amount"])
    for b in m.get("preTokenBalances") or []:
        if b.get("mint") == mint and b.get("owner"):
            d[b["owner"]] -= float(b["uiTokenAmount"]["amount"])
    return d


def chain_info(mint, curve, leader, det_ts, is_curve, max_pages=10, max_txs=25, max_holders=12):
    """Creator and early-buyer numbers (percent of supply). Best effort: missing parts are left out with a note."""
    out = {"notes": []}
    sup = float(call("getTokenSupply", [mint], EPS_ACCTS)["value"]["amount"])
    pct = lambda v: round(v / sup * 100, 2) if sup else None
    creator = None
    if is_curve and curve:
        try:
            creator, prog = _creator_from_curve(curve)
            out["curve_sold_pct"] = prog
        except Exception as e:
            out["notes"].append(f"curve read failed: {e}")
    # oldest transactions of the coin: creation slot and the buyers in it
    sigs = []; before = None; complete = False
    for _ in range(max_pages):
        p = {"limit": 1000}
        if before:
            p["before"] = before
        page = call("getSignaturesForAddress", [mint, p], EPS_HIST)
        sigs += page
        if len(page) < 1000:
            complete = True; break
        before = page[-1]["signature"]
    if complete and sigs:
        cslot = sigs[-1]["slot"]; cbt = sigs[-1].get("blockTime")
        out["age_s"] = round(det_ts - cbt) if cbt else None
        early = [s for s in reversed(sigs) if s["slot"] <= cslot + 2 and not s.get("err")][:max_txs]
        b0 = defaultdict(float); b3 = defaultdict(float); payer0 = None
        for s in early:
            tx = call("getTransaction", [s["signature"], {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 1,
                                                         "commitment": "confirmed"}], EPS_HIST, timeout=25)
            if not tx:
                continue
            if payer0 is None:
                k = tx["transaction"]["message"]["accountKeys"][0]
                payer0 = k["pubkey"] if isinstance(k, dict) else k
            for o, v in _deltas(tx, mint).items():
                if v > 0 and o != curve:
                    b3[o] += v
                    if s["slot"] == cslot:
                        b0[o] += v
        creator = creator or payer0
        if creator:
            out["creator_first_buy_pct"] = pct(b0.get(creator, 0.0))
        b0x = {o: v for o, v in b0.items() if o != creator}
        out.update(early_n=len(b0x), early_pct=pct(sum(b0x.values())), early3_n=len(b3), early3_pct=pct(sum(b3.values())))
        held = 0.0; checked = 0
        for o, _ in sorted(b0x.items(), key=lambda x: -x[1])[:max_holders]:
            try:
                held += _balance(o, mint); checked += 1
            except Exception:
                pass
        out["early_hold_pct"] = pct(held)
        if checked < min(len(b0x), max_holders):
            out["notes"].append(f"early-buyer balances read for {checked} of {min(len(b0x), max_holders)}")
        if len(b0x) > max_holders:
            out["notes"].append(f"still-held counts the {max_holders} biggest of {len(b0x)} first-block buyers")
    else:
        out["notes"].append(f"coin has over {max_pages * 1000} transactions, early buyers not read")
    if creator:
        out["creator"] = creator
        try:
            out["creator_hold_pct"] = pct(_balance(creator, mint))
        except Exception as e:
            out["notes"].append(f"creator balance failed: {e}")
    if not out["notes"]:
        out.pop("notes")
    return out
