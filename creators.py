"""Creator (dev) track record for the PAPER-MODE scanner. LOGGED ONLY: it never blocks, buys or sells anything.

Rage, Nov 7, 2026: shadow-test the coin creator's reputation. For every new signal the bot records who launched the
coin (the creator stored in the coin's pump.fun bonding-curve account), every other pump.fun coin that wallet
launched, how many of them graduated off the curve, how many look rugged, and a flag:
  rugged_before  at least one earlier coin graduated and is now worth under 15% of the graduation market cap
                 (fell 85%+ after graduating)
  clean          earlier coins found, at least one graduated, none look rugged
  no_grads       earlier coins found, none graduated (a launcher whose coins never left the curve)
  first_coin     no earlier pump.fun launch by this wallet
  launcher       the wallet launched 100+ coins (a launch service), so it says nothing about one dev
  unknown        not a pump.fun coin, or the lookup failed

Sources, all free and read-only: the public Solana node api.mainnet-beta.solana.com (the curve account, and a
search of all pump.fun curves by creator), and DexScreener for each earlier coin's launch time and market cap now.
Helius is not used, so its free credits stay for the live feed. Each creator is looked up once per run and cached
for 12 hours.
"""
import base64
import hashlib
import json
import time
import urllib.request
from collections import Counter

import holders as H

PUMP = H.PUMP_PROGRAM
EPS = ["https://api.mainnet-beta.solana.com"]      # the only free node that answers the creator search
T22 = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
TOK = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
DEX = "https://api.dexscreener.com/tokens/v1/solana/"
SKIP_MINTS = {"So11111111111111111111111111111111111111112", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
              "pumpCmXqMfrsAkQ5r49WcJnRayYRqmXz6ae8H7H9Dfn"}
GRAD_MCAP_SOL = 410.0        # about the market cap of a pump.fun coin when it graduates, in SOL
RUG_FRACTION = 0.15          # graduated and now under 15% of that = fell 85%+
MAX_RESOLVE = 25             # earlier coins looked up in detail per creator (graduated ones first)
CACHE_S = 12 * 3600


def _on_curve(b):
    y = int.from_bytes(b, "little") & ((1 << 255) - 1)
    if y >= H._P:
        return False
    u = (y * y - 1) % H._P
    v = (H._D * y * y + 1) % H._P
    x2 = u * pow(v, H._P - 2, H._P) % H._P
    return x2 == 0 or pow(x2, (H._P - 1) // 2, H._P) == 1


def curve_of(mint):
    """pump.fun bonding-curve address of a mint (program-derived, seeds 'bonding-curve' + mint)."""
    try:
        seeds = b"bonding-curve" + H._b58dec(mint)
    except Exception:
        return None
    pid = H._b58dec(PUMP)
    for bump in range(255, -1, -1):
        h = hashlib.sha256(seeds + bytes([bump]) + pid + b"ProgramDerivedAddress").digest()
        if not _on_curve(h):
            return H.b58(h)
    return None


def _curve_fields(raw):
    return {"creator": H.b58(raw[49:81]), "complete": bool(raw[48]),
            "sold_pct": round((1 - int.from_bytes(raw[24:32], "little") / 793_100_000_000_000) * 100, 1)}


def creator_of(mint):
    c = curve_of(mint)
    if not c:
        return None, None
    v = H.call("getAccountInfo", [c, {"encoding": "base64", "commitment": "confirmed"}], EPS, tries=4)["value"]
    if not v or v.get("owner") != PUMP:
        return None, c
    raw = base64.b64decode(v["data"][0])
    return (_curve_fields(raw) if len(raw) >= 81 else None), c


def curves_by(creator):
    """Every pump.fun curve whose creator field is this wallet (curve accounts stay after graduation)."""
    res = H.call("getProgramAccounts", [PUMP, {"encoding": "base64", "dataSlice": {"offset": 0, "length": 81},
                                              "filters": [{"memcmp": {"offset": 49, "bytes": creator}}]}],
                 EPS, tries=4, timeout=40)
    out = {}
    for a in res:
        raw = base64.b64decode(a["account"]["data"][0])
        if len(raw) >= 81:
            out[a["pubkey"]] = _curve_fields(raw)
    return out


def mint_of_curve(curve):
    """The coin a curve belongs to: the curve's own token account, else a recent curve transaction; always
    checked by deriving the curve address back from the mint."""
    for prog in (T22, TOK):
        try:
            r = H.call("getTokenAccountsByOwner", [curve, {"programId": prog}, {"encoding": "jsonParsed"}], EPS, tries=3)
            for a in r["value"]:
                m = a["account"]["data"]["parsed"]["info"]["mint"]
                if curve_of(m) == curve:
                    return m
        except Exception:
            pass
    try:
        sigs = H.call("getSignaturesForAddress", [curve, {"limit": 2}], EPS, tries=3)
        seen = Counter()
        for s in sigs:
            tx = H.call("getTransaction", [s["signature"], {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 1,
                                                         "commitment": "confirmed"}], EPS, tries=3, timeout=25)
            for b in (tx or {}).get("meta", {}).get("postTokenBalances") or []:
                if b.get("mint") and b["mint"] not in SKIP_MINTS:
                    seen[b["mint"]] += 1
            for m, _ in seen.most_common():
                if curve_of(m) == curve:
                    return m
    except Exception:
        pass
    return None


def dex_state(mints):
    """mint -> launch time (first DexScreener pair), market cap now, liquidity now, dexes. Plus SOL price."""
    out = {}
    sol = None
    mints = list(dict.fromkeys(mints))
    for i in range(0, len(mints), 30):
        url = DEX + ",".join(mints[i:i + 30])
        pairs = None
        for k in range(3):
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
                pairs = json.load(urllib.request.urlopen(req, timeout=20)) or []
                break
            except Exception:
                time.sleep(2 * (k + 1))
        for p in pairs or []:
            m = (p.get("baseToken") or {}).get("address")
            if m not in mints:
                continue
            d = out.setdefault(m, {"mcap": 0.0, "liq": 0.0, "dexes": [], "t0": None})
            d["mcap"] = max(d["mcap"], float(p.get("marketCap") or p.get("fdv") or 0))
            d["liq"] += float((p.get("liquidity") or {}).get("usd") or 0)
            if p.get("dexId") and p["dexId"] not in d["dexes"]:
                d["dexes"].append(p["dexId"])
            if p.get("pairCreatedAt"):
                t = p["pairCreatedAt"] / 1000
                d["t0"] = t if d["t0"] is None else min(d["t0"], t)
            try:
                if (p.get("quoteToken") or {}).get("symbol") == "SOL" and float(p["priceNative"]) > 0:
                    sol = float(p["priceUsd"]) / float(p["priceNative"])
            except Exception:
                pass
    return out, sol


def history(creator, exclude_curve=None):
    """All earlier pump.fun coins of a creator, with graduation, launch time and market cap now (cacheable)."""
    t0 = time.time()
    curves = curves_by(creator)
    curves.pop(exclude_curve, None)
    order = sorted(curves, key=lambda c: not curves[c]["complete"])      # graduated first
    coins = []
    cap = 5 if len(curves) >= 100 else MAX_RESOLVE      # launch-service wallets: sample only, keep the queue quick
    for c in order[:cap]:
        m = mint_of_curve(c)
        coins.append({"curve": c, "mint": m, "grad": curves[c]["complete"], "sold_pct": curves[c]["sold_pct"]})
    dex, sol = dex_state([x["mint"] for x in coins if x["mint"]])
    for x in coins:
        d = dex.get(x["mint"]) or {}
        x["t0"] = round(d["t0"]) if d.get("t0") else None
        x["mcap_now"] = round(d.get("mcap", 0)) if d else None
        x["liq_now"] = round(d.get("liq", 0)) if d else None
        if d and any(k != "pumpfun" for k in d.get("dexes", [])):
            x["grad"] = True
    return {"creator": creator, "n_all": len(curves), "n_grad_all": sum(v["complete"] for v in curves.values()),
            "resolved": len(coins), "coins": coins, "sol": sol, "fetched": round(time.time()), "secs": round(time.time() - t0, 1)}


def judge(hist, as_of, sol=None):
    """Flag a creator from its history, counting only coins launched before as_of (no look-ahead on launches).
    Market caps are today's, so 'rugged' means 'fell 85%+ after graduating, as seen now'."""
    sol = sol or hist.get("sol") or 160.0
    floor = RUG_FRACTION * GRAD_MCAP_SOL * sol
    prev = [x for x in hist["coins"] if x.get("t0") and x["t0"] < as_of - 60]
    undated = sum(1 for x in hist["coins"] if not x.get("t0"))
    rugs = [x for x in prev if x["grad"] and x.get("mcap_now") is not None and x["mcap_now"] < floor]
    grads = [x for x in prev if x["grad"]]
    unresolved = hist["n_all"] - hist["resolved"]
    out = {"creator": hist["creator"], "n_prev": len(prev), "n_grad": len(grads), "n_rug": len(rugs),
           "n_7d": sum(1 for x in prev if as_of - x["t0"] <= 7 * 86400),
           "best_mcap_now": max([x.get("mcap_now") or 0 for x in prev] or [0]),
           "n_all_coins": hist["n_all"], "undated": undated, "unresolved": unresolved,
           "rug_mints": [x["mint"] for x in rugs][:5], "grad_mints": [x["mint"] for x in grads][:5]}
    out["flag"] = "rugged_before" if rugs else ("clean" if grads else ("no_grads" if prev else "first_coin"))
    if out["flag"] == "first_coin" and (undated or unresolved):
        out["flag"] = "unknown"          # earlier coins exist but could not be dated or found
    return out


_MEM = {}


RANK = {"launcher": 0, "rugged_before": 4, "clean": 3, "no_grads": 2, "first_coin": 1, "unknown": 0}


def _hist(creator, curve):
    hit = _MEM.get(creator)
    if hit and time.time() - hit["fetched"] < CACHE_S:
        return hit, True
    h = history(creator, exclude_curve=curve)
    _MEM[creator] = h
    return h, False


def creator_info(mint, det_ts=None, deployer=None):
    """Creator record for a new signal. Best effort; never raises; never touches the database.
    deployer: the wallet that paid for the coin's first transaction, when the bot already found it. pump.fun's
    curve 'creator' (who gets creator fees) is usually the same wallet; when it isn't, both are checked and the
    worse record wins (rugged before > clean > no graduations > first coin)."""
    det_ts = det_ts or time.time()
    t0 = time.time()
    try:
        cv, curve = creator_of(mint)
        if not cv:
            return {"flag": "unknown", "why": "not a pump.fun coin (no bonding curve)"}
        hist, cached = _hist(cv["creator"], curve)
        out = judge(hist, det_ts)
        if deployer and deployer != cv["creator"]:
            try:
                d = judge(_hist(deployer, curve)[0], det_ts)
                out["deployer"] = {k: d[k] for k in ("creator", "flag", "n_prev", "n_grad", "n_rug", "n_all_coins")}
                if RANK[d["flag"]] > RANK[out["flag"]]:
                    out["flag"] = d["flag"]; out["flag_from"] = "deployer"
            except Exception as e:
                out["deployer"] = {"creator": deployer, "error": str(e)[:120]}
        out["launcher"] = out["n_all_coins"] >= 100        # launch-service wallet, not one dev's track record
        if out["launcher"]:
            out["flag_raw"] = out["flag"]; out["flag"] = "launcher"
        out.update(src="pump.fun curve", cached=cached, secs=round(time.time() - t0, 1))
        return out
    except Exception as e:
        return {"flag": "unknown", "why": f"lookup failed: {str(e)[:150]}", "secs": round(time.time() - t0, 1)}


# ---------------------------------------------------------------- dashboard / report (read-only)
FLAGS = [("rugged_before", "Creator rugged before"), ("clean", "Clean creator (graduated coins, no rugs)"),
         ("no_grads", "Creator's earlier coins never graduated"), ("first_coin", "Creator's first coin"),
         ("launcher", "Launch-service wallet (100+ coins, not one dev)"), ("unknown", "Unknown / not pump.fun")]


def view(con):
    """Closed paper trades grouped by creator flag (only trades whose signal has creator data), per account."""
    import config as C
    rows = con.execute("SELECT p.account, p.symbol, p.mint, p.status, p.pnl_usd, p.entry_ts, s.chain_info FROM positions p "
                       "JOIN signals s ON s.id=p.signal_id WHERE p.account IN (?,?) AND s.chain_info LIKE '%\"cr\"%'",
                       (C.ACCT_REAL, C.ACCT_REAL_SHADOW)).fetchall()
    acc = {}
    recent = []
    for a, sym, mint, st, pnl, ets, ci in rows:
        try:
            cr = json.loads(ci).get("cr") or {}
        except Exception:
            continue
        f = cr.get("flag") or "unknown"
        g = acc.setdefault(a, {}).setdefault(f, {"closed": 0, "wins": 0, "pnl": 0.0, "open": 0})
        if st == "closed":
            g["closed"] += 1; g["wins"] += 1 if (pnl or 0) > 0 else 0; g["pnl"] += pnl or 0
        else:
            g["open"] += 1
        if a == C.ACCT_REAL_SHADOW:
            recent.append({"coin": sym or mint[:6], "flag": f, "status": st, "pnl": pnl, "ts": ets,
                           "prev": cr.get("n_prev"), "grad": cr.get("n_grad"), "rug": cr.get("n_rug")})
    recent.sort(key=lambda r: -(r["ts"] or 0))
    labels = dict(FLAGS)
    out = {"flags": [{"flag": f, "label": labels[f]} for f, _ in FLAGS], "recent": recent[:15], "accounts": []}
    for a, lab in ((C.ACCT_REAL, "Main $200 account"), (C.ACCT_REAL_SHADOW, "Flat-$20 test (every survivor)")):
        out["accounts"].append({"label": lab, "groups": {f: acc.get(a, {}).get(f, {"closed": 0, "wins": 0, "pnl": 0.0, "open": 0})
                                                         for f, _ in FLAGS}})
    out["logged"] = con.execute("SELECT COUNT(*) FROM signals WHERE chain_info LIKE '%\"cr\"%'").fetchone()[0]
    return out


def report_lines(con):
    v = view(con)
    L = [f"CREATOR CHECK (logged only, never blocks; {v['logged']} signals checked so far). Closed trades by creator flag:"]
    for a in v["accounts"]:
        parts = []
        for f, lab in FLAGS:
            g = a["groups"][f]
            if g["closed"] or g["open"]:
                parts.append(f"{f} {g['closed']} closed, {g['wins']} won, ${g['pnl']:+.2f}" + (f", {g['open']} open" if g["open"] else ""))
        L.append(f"  {a['label']}: " + ("; ".join(parts) if parts else "no trades with creator data yet"))
    return L
