#!/usr/bin/env python3
"""Read-only phone dashboard for the PAPER-MODE memecoin copy-trade scanner.

Serves one page and one JSON feed under a secret path. It only READS scanner.db and the scanner log.
There is no wallet, no keys, no trading and no control that can touch money: it is a window, nothing more.
"""
import json, os, re, sqlite3, sys, time
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import config as C
import fills

PORT = int(os.environ.get("DASH_PORT", "8790"))
LAGOS = timezone(timedelta(hours=1))

FILTERS = {
    "liquidity_under_25k": "Too little money in the pool (under $25k)",
    "up_100pct_1h": "Already pumped over 100% in the last hour",
    "top10_over_35pct": "Top 10 holders own over 35%",
    "dev_over_5pct": "Creator holds over 5%",
    "bundled_buys": "Bundled / insider buys",
    "smart_wallet_sold_early": "Smart wallet sold before we could copy",
    "analyst_mint_authority_live": "Creator can still print more coins",
    "analyst_freeze_authority_live": "Creator can still freeze wallets",
    "analyst_flagged_rugged": "Flagged as a rug",
    "safety_check_unavailable": "Safety check site didn't answer",
}
EXITS = {
    "stop": "Stop loss hit", "trailing stop": "Trailing stop hit (locked profit)",
    "stop (gapped through)": "Stop loss hit (price jumped past it)",
    "trailing stop (gapped)": "Trailing stop hit (price jumped past it)",
    "smart wallet sold": "Copied wallet sold, so we sold", "24h time stop": "24-hour time limit",
    "max-loss stop": "$5 max-loss stop hit", "max-loss stop (gapped)": "$5 max-loss stop hit (price jumped past it)",
    "breakeven stop": "Breakeven stop hit (winner closed at no loss)",
    "breakeven stop (gapped)": "Breakeven stop hit (price jumped past it)",
}
ALT_NAMES = {"old_exits": "Old exits (no half sell, old tightening trail)",
             "no_stop_be": "New exits without the $5 stop and breakeven"}


def exit_compare(con, acct):
    """New exits vs the comparison copies on the same finished trades (v3 trades where every copy has finished)."""
    rows = con.execute("SELECT pnl_usd, alts, alts_open, part_ts FROM positions WHERE account=? AND status='closed' AND rules='v3'",
                       (acct,)).fetchall()
    done = [r for r in rows if not r[2] and r[1]]
    out = {"n": len(done), "waiting": len(rows) - len(done), "new": {"pnl": sum(r[0] for r in done),
           "wins": sum(1 for r in done if r[0] > 0), "losses": sum(1 for r in done if r[0] <= 0)},
           "half_sold": sum(1 for r in done if r[3]), "alts": {}}
    for k in ALT_NAMES:
        pairs = [(r[0], json.loads(r[1]).get(k, {}).get("pnl")) for r in done]
        pairs = [(a, b) for a, b in pairs if b is not None]
        differ = [(a, b) for a, b in pairs if abs(a - b) > 0.005]
        new_pnl, alt_pnl = sum(a for a, _ in pairs), sum(b for _, b in pairs)
        new_l, alt_l = sum(1 for a, _ in pairs if a <= 0), sum(1 for _, b in pairs if b <= 0)
        meaningful = len(pairs) >= C.CMP_MIN_TRADES and len(differ) >= C.CMP_MIN_DIFFER
        worse = meaningful and (new_pnl < alt_pnl or new_l > alt_l)
        out["alts"][k] = {"label": ALT_NAMES[k], "n": len(pairs), "differ": len(differ), "new_pnl": new_pnl, "alt_pnl": alt_pnl,
                          "new_wins": len(pairs) - new_l, "alt_wins": len(pairs) - alt_l, "new_losses": new_l,
                          "alt_losses": alt_l, "meaningful": meaningful, "worse": worse}
    return out


def compare_lines(cmp, label):
    """Plain-text lines for the report and the scanner updates."""
    L = []
    m = lambda v: ("-" if v < 0 else "+") + f"${abs(v):.2f}"
    for k, a in cmp["alts"].items():
        if not a["n"]:
            L.append(f"  {label}: new exits vs {a['label'].lower()}: no finished trades yet ({cmp['waiting']} still being followed).")
            continue
        verdict = ("WARNING: NEW EXITS DOING WORSE" if a["worse"] else "new exits ahead or level" if a["meaningful"]
                   else f"too early to judge (needs {C.CMP_MIN_TRADES} trades with {C.CMP_MIN_DIFFER} that differ)")
        L.append(f"  {label}: new exits {m(a['new_pnl'])} ({a['new_wins']} won, {a['new_losses']} lost) vs "
                 f"{a['label'].lower()} {m(a['alt_pnl'])} ({a['alt_wins']} won, {a['alt_losses']} lost) on the same "
                 f"{a['n']} trades, {a['differ']} differed. {verdict}.")
    return L


def sell_proceeds(qty, px):
    return max(0.0, qty * px * (1 - C.SLIPPAGE) * (1 - C.BOT_FEE - C.POOL_FEE) - C.NETWORK_FEE_USD)


def mtm_terms(p):
    """Terms the page needs to re-price an open paper position at a fresh market price, with the same costs as the bot:
    value = max(0, qa * price - sub), P&L = value + base. Display only; the bot's own books are not touched."""
    try:
        keys = p.keys()
        qty = p["qty"] or 0
        if "fill_model" in keys and p["fill_model"] == "live":
            qa = qty * (p["r_sell_est"] or 0) * (1 - C.BOT_FEE)
            sub = p["fee_in"] or 0
            entry_fee = p["fee_in"] or 0
        else:
            qa = qty * (1 - C.SLIPPAGE) * (1 - C.BOT_FEE - C.POOL_FEE)
            sub = C.NETWORK_FEE_USD
            entry_fee = C.NETWORK_FEE_USD
        return {"qa": qa, "sub": sub, "base": (p["part_proceeds"] or 0) - p["size_usd"] - entry_fee}
    except Exception:
        return None


def short(a):
    return (a[:4] + "…" + a[-4:]) if a and len(a) > 10 else (a or "")


def read_log_tail(nbytes=400_000):
    try:
        with open(C.LOG_PATH, "rb") as f:
            f.seek(0, 2); size = f.tell(); f.seek(max(0, size - nbytes))
            lines = f.read().decode("utf-8", "ignore").splitlines()
            return lines[1:] if size > nbytes else lines
    except Exception:
        return []


LOGRE = re.compile(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d),\d+ (\w+) (.*)$")


def log_ts(s):
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp()


def error_kind(msg):
    if msg.startswith("snapshot failed"):
        src = "Solana wallet data"
    elif msg.startswith("board refresh failed"):
        src = "Kolscan wallet leaderboard"
    elif msg.startswith("guard error"):
        src = "Coin price feed (GeckoTerminal)"
    elif msg.startswith("candidate error"):
        src = "Coin checks"
    elif msg.startswith("loop error"):
        src = "Bot loop"
    else:
        src = "Other"
    m = re.search(r"HTTP Error (\d+)", msg)
    why = {"403": "blocked (403)", "429": "rate-limited (429)", "404": "not found (404)"}.get(m.group(1), "HTTP " + m.group(1)) if m else (
        "timed out" if "timed out" in msg.lower() else "error")
    return src, why


def parse_log(now):
    errs = {}; activity = []; burst = None
    for line in read_log_tail():
        m = LOGRE.match(line)
        if not m:
            continue
        ts = log_ts(m.group(1)); lvl, msg = m.group(2), m.group(3)
        if lvl in ("WARNING", "ERROR"):
            src, why = error_kind(msg)
            if now - ts < 15 * 60:
                k = src + ": " + why
                e = errs.setdefault(k, {"what": k, "count": 0, "last": ts}); e["count"] += 1; e["last"] = max(e["last"], ts)
            key = src + ": " + why
            if burst and burst["key"] == key and ts - burst["ts"] < 600:
                burst["n"] += 1; burst["ts"] = ts
                burst["text"] = f"{src} failed {burst['n']} times ({why})"
            else:
                burst = {"key": key, "ts": ts, "n": 1, "kind": "bad", "text": f"{src} failed ({why})"}
                activity.append(burst)
            continue
        burst = None
        if msg.startswith("scanner start"):
            activity.append({"ts": ts, "kind": "info", "text": "Bot started (paper mode)"})
        elif msg.startswith("board refreshed"):
            mm = re.search(r"(\d+) wallets followed", msg)
            activity.append({"ts": ts, "kind": "info", "text": f"Re-read Kolscan leaderboard, following {mm.group(1) if mm else '?'} wallets"})
        elif msg.startswith("PAPER BUY"):
            mm = re.match(r"PAPER BUY (\S+) \S+ \$([\d.]+) .* copying (.*)$", msg)
            activity.append({"ts": ts, "kind": "buy", "text": f"Paper BUY {mm.group(1)} for ${mm.group(2)}, copying {mm.group(3)}" if mm else msg})
        elif msg.startswith("PAPER SELL"):
            mm = re.match(r"PAPER SELL (\S+) (.*) pnl \$(-?[\d.]+) \((-?\d+)%\)", msg)
            activity.append({"ts": ts, "kind": "sell", "text": f"Paper SELL {mm.group(1)}: {EXITS.get(mm.group(2), mm.group(2))}, {'+' if float(mm.group(3)) >= 0 else '-'}${abs(float(mm.group(3))):.2f} ({mm.group(4)}%)" if mm else msg})
    for a in activity:
        a.pop("key", None); a.pop("n", None)
    return sorted(errs.values(), key=lambda e: -e["count"]), activity[-60:][::-1]


def uptime(con, now):
    beats = [r[0] for r in con.execute("SELECT ts FROM heartbeat WHERE ts>?", (now - 48 * 3600,))]
    mins = {int(t // 60) for t in beats}
    cov = lambda m: m in mins or (m - 1) in mins
    cells = []
    end_h = int(now // 3600)
    for h in range(end_h - 47, end_h + 1):
        ms = range(h * 60, min((h + 1) * 60, int(now // 60) + 1))
        cells.append({"h": h * 3600, "pct": round(100 * sum(1 for m in ms if cov(m)) / max(1, len(ms)))})
    def share(sec):
        ms = range(int((now - sec) // 60), int(now // 60) + 1)
        return round(100 * sum(1 for m in ms if cov(m)) / len(ms), 1)
    return {"h24": share(86400), "h48": share(48 * 3600), "cells": cells}


def live_status(con):
    """Live mode summary for the status tab. Never lets a live-mode problem break the paper dashboard."""
    try:
        import live
        return live.dashboard_status(con)
    except Exception:
        return {"mode": "unknown"}


def realism_view(con, real_start):
    """Ideal vs live-like P&L for the realistic accounts' trades priced with live-like fills (fills.py).
    Ideal = bought at the copied wallet's own price and sold at the exit price, with no costs at all."""
    con.row_factory = sqlite3.Row
    q = lambda s, a=(): con.execute(s, a).fetchall()
    out = {"exec_delay_s": C.EXEC_DELAY_S, "slippage_tol_pct": C.COPY_SLIPPAGE_TOL * 100, "jito_tip_sol": C.JITO_TIP_SOL,
           "bot_fee_pct": C.BOT_FEE * 100}
    r = q("SELECT MIN(entry_ts) FROM positions WHERE fill_model='live'")
    since = r[0][0] if r else None
    out["since"] = since
    for key, acct in (("shadow", C.ACCT_REAL_SHADOW), ("main", C.ACCT_REAL)):
        rows = q("SELECT * FROM positions WHERE account=? AND fill_model='live' AND status='closed'", (acct,))
        n = len(rows)
        tot = lambda k: sum((x[k] or 0) for x in rows)
        d = {"n": n, "open": q("SELECT COUNT(*) FROM positions WHERE account=? AND fill_model='live' AND status='open'", (acct,))[0][0]}
        if n:
            d.update(ideal=tot("ideal_pnl"), actual=tot("pnl_usd"), delay=tot("gap_delay"), impact=tot("gap_impact"),
                     botfee=tot("gap_botfee"), netfee=tot("gap_netfee"), size=tot("size_usd"),
                     no_sell=sum(1 for x in rows if x["r_sell"] == 0))
            d["gap"] = d["ideal"] - d["actual"]
        out[key] = d
    allp = q("SELECT * FROM positions WHERE account=? AND fill_model='live'", (C.ACCT_REAL_SHADOW,))
    if allp:
        avg = lambda v: (sum(v) / len(v)) if v else None
        out["avg_rt_pct"] = avg([x["rt_pct"] for x in allp if x["rt_pct"] is not None and x["sellable"]])
        out["avg_fee_tx"] = avg([x["fee_in"] for x in allp if x["fee_in"] is not None])
        out["avg_delay_move_pct"] = avg([(x["entry_price"] / x["leader_px"] - 1) * 100 for x in allp if x["leader_px"]])
        out["avg_landing_move_pct"] = avg([(x["entry_price"] / x["seen_px"] - 1) * 100 for x in allp if x["seen_px"]])
        out["avg_liq_in"] = avg([x["liq_in"] for x in allp if x["liq_in"]])
        out["avg_liq_out"] = avg([x["liq_out"] for x in allp if x["liq_out"] is not None and x["status"] == "closed"])
        out["unsellable_at_entry"] = sum(1 for x in allp if x["sellable"] == 0)
        out["jupiter_share"] = round(100 * sum(1 for x in allp if (x["q_src_in"] or "").startswith("jupiter")) / len(allp))
    out["failed_buys"] = q("SELECT COUNT(*) FROM signals WHERE fill_note LIKE '%buy failed%'")[0][0]
    out["failed_examples"] = [dict(coin=r[0], note=r[1]) for r in
                              q("SELECT symbol, fill_note FROM signals WHERE fill_note LIKE '%buy failed%' ORDER BY detected_ts DESC LIMIT 5")]
    return out


def build_state():
    now = time.time()
    con = sqlite3.connect(f"file:{C.DB_PATH}?mode=ro", uri=True, timeout=10)
    con.row_factory = sqlite3.Row
    q = lambda s, a=(): con.execute(s, a).fetchall()
    meta = {r["key"]: json.loads(r["value"]) for r in q("SELECT * FROM meta")}
    hb = q("SELECT ts,polled FROM heartbeat ORDER BY ts DESC LIMIT 1")
    last_beat = hb[0]["ts"] if hb else None
    polled_10m = con.execute("SELECT COALESCE(SUM(polled),0) FROM heartbeat WHERE ts>?", (now - 600,)).fetchone()[0]
    wallets = q("SELECT name,address,last_rank,pnl_usd,wins,losses,n_accounts,poll_s,last_poll FROM wallets WHERE active=1 ORDER BY last_rank")
    names = {r["address"]: r["name"] for r in q("SELECT address,name FROM wallets")}

    # ---- $200 paper accounts: realistic entry (headline) and the old 5-seconds-after-the-wallet test
    real_start = meta.get("real_start_ts")
    start_ts = meta.get("start_ts", now)

    sessions = meta.get("session_ts") or {}

    def account_view(acct, cash, eq_table, t0):
        sess = sessions.get(str(acct)) or 0   # paper balance reset: count from the reset on (old trades stay in the db)
        if sess:
            t0 = sess
        opens = q("SELECT * FROM positions WHERE account=? AND status='open' ORDER BY entry_ts DESC", (acct,))
        open_rows = []; open_val = 0.0
        for p in opens:
            val = fills.position_value(p, p["last_price"] or 0); open_val += val
            pnl = val + (p["part_proceeds"] or 0) - p["size_usd"] - (p["fee_in"] if p["fill_model"] == "live" else C.NETWORK_FEE_USD)
            open_rows.append({
                "coin": p["symbol"] or short(p["mint"]), "mint": short(p["mint"]), "entry_price": p["entry_price"],
                "now_price": p["last_price"], "size": p["size_usd"], "pnl": pnl, "pnl_pct": pnl / p["size_usd"] * 100,
                "stop": p["stop"], "stop_pct": (p["stop"] / p["entry_price"] - 1) * 100 if p["entry_price"] else None,
                "stop_kind": ("breakeven" if p["be_px"] and abs(p["stop"] - p["be_px"]) <= 1e-9 * p["stop"] else
                              "trailing" if p["stop"] > p["entry_price"] else
                              ("breakeven" if abs(p["stop"] / p["entry_price"] - 1) < .2 and p["stop"] >= p["entry_price"] else "initial")),
                "half": ({"px": p["part_px"], "x": p["part_px"] / p["entry_price"], "got": p["part_proceeds"]} if p["part_ts"] else None),
                "peak_pct": (p["peak"] / p["entry_price"] - 1) * 100 if p["entry_price"] else None,
                "wallet": names.get(p["leader"]) or short(p["leader"]), "entry_ts": p["entry_ts"],
                # real age of the price the bot is using (candle end or live-quote time), not of its last check-in
                "price_age": now - ((p["price_ts"] if "price_ts" in p.keys() else None) or ((p["last_candle_ts"] or p["entry_ts"]) + 60)),
                "price_src": (p["price_src"] if "price_src" in p.keys() else None) or "candle",
                # for the page's live market prices (display only): full mint, the bot's pool, cost terms
                "mint_full": p["mint"], "pool": p["pool"], "mtm": mtm_terms(p),
                "last_candle_ts": p["last_candle_ts"],
            })
        equity = cash + open_val
        curve = [(t0, C.START_EQUITY)] + [(r["ts"], r["equity"]) for r in q(f"SELECT ts,equity FROM {eq_table} WHERE ts>=? ORDER BY ts", (t0 or 0,))]
        curve.append((now, equity)); curve.sort()
        peak = -1e18; mdd = 0.0; mdd_usd = 0.0
        for _, v in curve:
            peak = max(peak, v)
            if peak > 0 and (peak - v) / peak > mdd:
                mdd = (peak - v) / peak; mdd_usd = peak - v
        hist = []
        for c in q("SELECT * FROM positions WHERE account=? AND status='closed' AND COALESCE(exit_ts,0)>=? ORDER BY exit_ts DESC", (acct, sess)):
            hist.append({"coin": c["symbol"] or short(c["mint"]), "mint": short(c["mint"]), "wallet": names.get(c["leader"]) or short(c["leader"]),
                         "entry_ts": c["entry_ts"], "exit_ts": c["exit_ts"], "entry_price": c["entry_price"], "exit_price": c["exit_price"],
                         "size": c["size_usd"], "pnl": c["pnl_usd"], "pnl_pct": c["pnl_pct"],
                         "reason": EXITS.get(c["exit_reason"], c["exit_reason"]),
                         "half": ({"x": c["part_px"] / c["entry_price"], "got": c["part_proceeds"]} if c["part_ts"] else None),
                         "stop_fill": ({"stop": c["exit_trigger_px"], "fill": c["exit_price"], "src": c["exit_fill_src"]}
                                       if c["exit_trigger_px"] else None),
                         "alts": ({k: {"label": ALT_NAMES[k], "pnl": a.get("pnl"), "reason": EXITS.get(a.get("reason"), a.get("reason")),
                                       "open": a.get("st") == "open"} for k, a in json.loads(c["alts"]).items() if k in ALT_NAMES}
                                  if c["alts"] else None),
                         "fill": ({"ideal": c["ideal_pnl"], "delay": c["gap_delay"], "impact": c["gap_impact"],
                                   "botfee": c["gap_botfee"], "netfee": c["gap_netfee"], "rt_pct": c["rt_pct"],
                                   "liq_in": c["liq_in"], "liq_out": c["liq_out"], "src_in": c["q_src_in"],
                                   "src_out": c["q_src_out"], "sellable": c["sellable"]}
                                  if c["fill_model"] == "live" else None)})
        wins = [h for h in hist if h["pnl"] > 0]
        acc = {"start": C.START_EQUITY, "equity": equity, "cash": cash, "open_value": open_val, "since": t0,
               "change_pct": (equity / C.START_EQUITY - 1) * 100, "change_usd": equity - C.START_EQUITY,
               "mdd_pct": mdd * 100, "mdd_usd": mdd_usd, "curve": curve[-600:], "n_open": len(open_rows),
               "max_open": C.MAX_OPEN, "closed": len(hist), "wins": len(wins),
               "win_rate": (len(wins) / len(hist) * 100) if hist else None, "total_pnl": sum(h["pnl"] for h in hist),
               "reset_ts": sess or None}
        return acc, open_rows, hist

    if real_start:
        account, open_rows, hist = account_view(C.ACCT_REAL, meta.get("cash_real", C.START_EQUITY), "equity_real", real_start)
        account["kind"] = "realistic"
    account_5s, _, hist_5s = account_view(C.ACCT_5S, meta.get("cash", C.START_EQUITY), "equity", start_ts)
    account_5s["kind"] = "5s"
    # ---- rug filter shadow test: the realistic account's trades minus the ones the rug filter would skip
    rug = None
    try:
        hf_start = meta.get("hf_start_ts")
        if hf_start:
            rug, rug_open, _ = account_view(C.ACCT_HF, meta.get("cash_hf", C.START_EQUITY), "equity_hf", hf_start)
            rug.pop("curve", None)
            real_same = q("SELECT COUNT(*) n, COALESCE(SUM(pnl_usd),0) p, COALESCE(SUM(pnl_usd>0),0) w FROM positions "
                          "WHERE account=? AND status='closed' AND entry_ts>=?", (C.ACCT_REAL, hf_start))[0]
            skipped = []
            for r in q("SELECT s.id, s.symbol, s.mint, s.wallet, s.notes, s.detected_ts, p.status, p.pnl_usd FROM signals s "
                       "LEFT JOIN positions p ON p.signal_id=s.id AND p.account=? WHERE s.hf_block=1 AND s.detected_ts>=? "
                       "ORDER BY s.detected_ts DESC LIMIT 50", (C.ACCT_REAL, hf_start)):
                why = (r["notes"] or "").split("rug-filter test: skipped, ")[-1].split(";")[0] if "rug-filter" in (r["notes"] or "") else ""
                skipped.append({"coin": r["symbol"] or short(r["mint"]), "wallet": names.get(r["wallet"]) or short(r["wallet"]),
                                "ts": r["detected_ts"], "why": why, "real_status": r["status"], "real_pnl": r["pnl_usd"]})
            checked = q("SELECT COUNT(*) n, COALESCE(SUM(hf_top10 IS NOT NULL),0) ok FROM signals WHERE hf_block IS NOT NULL AND detected_ts>=?",
                        (hf_start,))[0]
            rug.update(kind="rug", since=hf_start, top10_max=C.HF_TOP10_MAX, rt_max=C.HF_RT_MAX, skipped=skipped,
                       checked=checked["n"], checked_ok=checked["ok"],
                       real_same={"closed": real_same["n"], "wins": real_same["w"], "pnl": real_same["p"]})
    except Exception as e:
        rug = {"error": f"rug filter test could not be read: {e}"}
    # ---- wallet scorecard (Rage's checklist)
    card = None
    try:
        rows = q("SELECT name,address,active,card_status,card_total,card_json FROM wallets WHERE card_status IS NOT NULL")
        order = {"pinned": 0, "keep": 1, "new": 2, "bench": 3, "removed": 4}
        card = sorted(({"name": r["name"] or short(r["address"]), "addr": short(r["address"]), "status": r["card_status"],
                        "followed": bool(r["active"]), "total": r["card_total"], **json.loads(r["card_json"] or "{}")}
                       for r in rows), key=lambda x: (order.get(x["status"], 9), -(x["total"] or 0)))
        card = [c for c in card if c["status"] != "bench" or c.get("facts", {}).get("copies") or c.get("items", 0) >= 4][:150]
    except Exception as e:
        card = {"error": f"wallet scorecard could not be read: {e}"}
    if not real_start:
        account, open_rows, hist = account_5s, _, hist_5s
    wins = [h for h in hist if h["pnl"] > 0]
    best = max(hist, key=lambda h: h["pnl_pct"]) if hist else None
    worst = min(hist, key=lambda h: h["pnl_pct"]) if hist else None

    # ---- detection lag: live feed vs holdings re-read, last 24 h and since the realistic accounts started
    def lag_stats(where, args):
        v = sorted(r[0] for r in q(f"SELECT detect_lag_s FROM signals WHERE detect_lag_s IS NOT NULL AND status!='ignored' AND {where}", args))
        if not v:
            return {"n": 0}
        return {"n": len(v), "median": v[len(v) // 2], "p90": v[min(len(v) - 1, int(len(v) * 0.9))],
                "under10": round(100 * sum(1 for x in v if x <= 10) / len(v))}
    lag = {"feed_24h": lag_stats("detect_src='stream' AND detected_ts>?", (now - 86400,)),
           "reread_24h": lag_stats("COALESCE(detect_src,'poll')='poll' AND detected_ts>?", (now - 86400,)),
           "all_24h": lag_stats("detected_ts>?", (now - 86400,)),
           "before_feed": lag_stats("detected_ts<?", (real_start or now,))}
    feed = meta.get("feed") or {}
    ws_beats = q("SELECT ws FROM heartbeat WHERE ts>? AND ws IS NOT NULL", (now - 86400,))
    feed_share = round(100 * sum(1 for r in ws_beats if r[0]) / len(ws_beats), 1) if ws_beats else None
    # realistic vs 5 s entry on the same coins (both versions bought it)
    pair = q(f"""SELECT r.entry_price re, f.entry_price fe, r.pnl_pct rp, f.pnl_pct fp FROM positions r JOIN positions f
                 ON f.signal_id=r.signal_id AND f.account={C.ACCT_5S_SHADOW} WHERE r.account={C.ACCT_REAL_SHADOW}""")
    entry_gap = sorted((x["re"] / x["fe"] - 1) * 100 for x in pair if x["re"] and x["fe"])
    sh_real = q("SELECT pnl_usd FROM positions WHERE account=? AND status='closed'", (C.ACCT_REAL_SHADOW,))

    # ---- signals feed
    sigs = []
    for s in q("SELECT * FROM signals ORDER BY detected_ts DESC LIMIT 400"):
        st = s["status"]; notes = s["notes"] or ""
        own = "; ".join(x for x in notes.split("; ") if not x.startswith("5 s test"))
        if st == "survivor":
            if "account full" in own:
                verdict, label = "passed", "Passed checks, not copied: 5 trades already open"
            elif "realistic buy failed" in own:
                m = re.search(r"price jumped (\d+)%", s["fill_note"] or "")
                verdict, label = "passed", ("Passed checks, but the copy would have failed: price jumped "
                                            + (m.group(1) + "%" if m else "too far") + " before it landed")
            elif "not enough paper cash" in own:
                verdict, label = "passed", "Passed checks, not copied: not enough paper cash"
            else:
                verdict, label = "copied", "Copied (paper buy)"
        elif st == "killed":
            verdict = "killed"
            label = "Blocked: " + "; ".join(FILTERS.get(k, k) for k in (s["killed_by"] or "").split(",") if k)
            if "5 s test still copied it" in notes:
                label += " (it sold before the scanner saw its buy)"
        elif st == "repeat":
            verdict, label = "skip", "Skipped: same coin already seen today"
        elif st == "missed":
            verdict, label = "skip", "Skipped: spotted too late to copy (" + (re.search(r"found (\d+) min late", notes).group(0) if re.search(r"found (\d+) min late", notes) else "late") + ")"
        else:
            verdict = "ignored"
            label = "Not a real buy: " + ({"no DEX market (airdrop/spam/unlisted)": "airdrop or spam coin with no market",
                                          "received by transfer, not bought": "coins were sent to the wallet, not bought"}.get(notes)
                                         or ("buy too small to matter" if notes.startswith("leader bag only") else notes or "unknown"))
        blind = "403" in notes
        sigs.append({"ts": s["detected_ts"], "wallet": s["wallet_name"] or short(s["wallet"]), "coin": s["symbol"] or short(s["mint"]),
                     "mint": short(s["mint"]), "verdict": verdict, "label": label, "liq": s["liq_usd"], "h1": s["h1_pct"],
                     "mcap": s["mcap"], "lag": s["detect_lag_s"], "p1h": s["p1h"], "p24h": s["p24h"], "partial": blind,
                     "holders": s["holders"], "rt": s["rt_pct"], "fill_note": s["fill_note"]})

    # ---- did the filters help? killed vs survivors after 1h / 24h
    def grp(status, col):
        v = [r[0] for r in q(f"SELECT {col} FROM signals WHERE status=? AND {col} IS NOT NULL", (status,))]
        if not v:
            return {"n": 0}
        v.sort()
        return {"n": len(v), "avg": sum(v) / len(v), "median": v[len(v) // 2], "up": sum(1 for x in v if x > 0),
                "dead": sum(1 for x in v if x <= -90)}
    compare = {k: {"h1": grp(k, "p1h"), "h24": grp(k, "p24h")} for k in ("killed", "survivor")}
    nkilled = con.execute("SELECT COUNT(*) FROM signals WHERE status='killed'").fetchone()[0]
    nsurv = con.execute("SELECT COUNT(*) FROM signals WHERE status='survivor'").fetchone()[0]
    filt = {}
    for r in q("SELECT killed_by FROM signals WHERE status='killed'"):
        for k in (r[0] or "").split(","):
            if k:
                filt[FILTERS.get(k, k)] = filt.get(FILTERS.get(k, k), 0) + 1

    # shadow test account: every survivor at a flat $20, no slot limit
    sh = q("SELECT pnl_usd FROM positions WHERE account=? AND status='closed'", (C.ACCT_REAL_SHADOW if real_start else C.ACCT_5S_SHADOW,))

    errs, activity = parse_log(now)
    awake = last_beat is not None and now - last_beat < 120
    live_st = live_status(con)
    exits = {"min_trades": C.CMP_MIN_TRADES, "min_differ": C.CMP_MIN_DIFFER, "groups": [
        {"label": "Realistic $200 account", **exit_compare(con, C.ACCT_REAL)},
        {"label": "Realistic, every survivor ($20 each)", **exit_compare(con, C.ACCT_REAL_SHADOW)},
        {"label": "Old 5 s test, $200 account", **exit_compare(con, C.ACCT_5S)},
        {"label": "Old 5 s test, every survivor", **exit_compare(con, C.ACCT_5S_SHADOW)}]}
    con.close()
    try:
        import report
        report_text = report.make_report(con2())
    except Exception as e:
        report_text = f"report failed: {e}"
    try:
        realism = realism_view(con2(), real_start)
    except Exception as e:
        realism = {"error": f"{type(e).__name__}: {e}"}
    try:
        import wallet_stats
        wallet_board = wallet_stats.boards(con2(), real_start, start_ts)
    except Exception as e:
        wallet_board = {"error": f"{type(e).__name__}: {e}"}
    return {
        "report": report_text,
        "now": now, "paper_only": live_st.get("mode") == "off", "live": live_st,
        "status": {"awake": awake, "last_beat": last_beat, "since_start_h": (now - start_ts) / 3600,
                   "wallets": len(wallets), "polled_10m": polled_10m, "errors": errs, "uptime": uptime(con2(), now),
                   "feed": {"connected": bool(feed.get("connected")) and now - (feed.get("ts") or 0) < 120,
                            "share_24h": feed_share, "reconnects": feed.get("reconnects"), "url": feed.get("url"),
                            "noisy": len(feed.get("noisy") or []), "side": feed.get("side")},
                   "lag": lag},
        "account": account, "account_5s": account_5s, "rug_filter": rug, "wallet_card": card,
        "exits": exits,
        "entry_compare": {"n": len(entry_gap), "median_gap_pct": entry_gap[len(entry_gap) // 2] if entry_gap else None,
                          "real_shadow": {"n": len(sh_real), "pnl": sum(r[0] for r in sh_real)}},
        "positions": open_rows, "history": hist[:200],
        "stats": {"closed": len(hist), "wins": len(wins), "win_rate": (len(wins) / len(hist) * 100) if hist else None,
                  "best": best, "worst": worst, "total_pnl": sum(h["pnl"] for h in hist),
                  "signals": len(sigs), "killed": nkilled, "survivors": nsurv, "compare": compare,
                  "filters": sorted(filt.items(), key=lambda x: -x[1]),
                  "shadow": {"n": len(sh), "wins": sum(1 for r in sh if r[0] > 0), "pnl": sum(r[0] for r in sh)}},
        "signals": sigs,
        "wallets": [{"name": w["name"] or short(w["address"]), "addr": short(w["address"]), "rank": w["last_rank"],
                     "pnl_usd": w["pnl_usd"], "wins": w["wins"], "losses": w["losses"], "every_s": w["poll_s"],
                     "last_check": w["last_poll"]} for w in wallets],
        "wallet_board": wallet_board,
        "realism": realism,
        "activity": activity,
        "rules": {"size": f"{int(C.POS_FRACTION*100)}% of the account per trade, max {C.MAX_OPEN} open",
                  "stop": (f"$5 max-loss stop: {int(C.MAX_LOSS_PCT*100)}% under entry (a dump can fill worse; the real fill is recorded). "
                           f"From +{int(C.BE2_TRIGGER*100)}% the stop never sits below breakeven after all costs"),
                  "runner": (f"Runner plan: at {C.HALF_AT:g}x sell half to take back the stake; the rest rides under a trail that widens: "
                             + ", ".join(f"{int(d*100)}% under the peak from {'+' + str(int(g*100)) + '%' if g < 1 else str(g + 1).rstrip('0').rstrip('.') + 'x'}"
                                         for g, d in C.TRAIL_V3)),
                  "compare": ("Every trade is also followed under the old exits and under the new exits without the $5 stop and "
                              "breakeven, so both P&Ls can be compared on the same trades"),
                  "costs": f"{int(C.SLIPPAGE*100)}% slippage + {int((C.BOT_FEE+C.POOL_FEE)*100)}% fees each way + ${C.NETWORK_FEE_USD:.2f} network fee",
                  "costs_live": (f"lands {C.EXEC_DELAY_S} s after seen; real Jupiter quotes for the trade size (price impact + DEX fees, "
                                 f"sell re-quoted at exit); {int(C.BOT_FEE*100)}% bot fee each way; base + live priority fee + "
                                 f"{C.JITO_TIP_SOL} SOL Jito tip per transaction; buys fail if price runs over {int(C.COPY_SLIPPAGE_TOL*100)}% before landing"),
                  "time": "Anything still open after 24 hours is closed"},
    }


def con2():
    c = sqlite3.connect(f"file:{C.DB_PATH}?mode=ro", uri=True, timeout=10)
    return c


class H(BaseHTTPRequestHandler):
    server_version = "dash"; sys_version = ""

    def log_message(self, *a):
        pass

    def send(self, code, body, ctype):
        b = body.encode() if isinstance(body, str) else body
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(b)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Robots-Tag", "noindex, nofollow")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(b)

    def do_HEAD(self):
        self.do_GET()

    def do_GET(self):
        path = self.path.split("?")[0]
        if path in (f"/{TOKEN}", f"/{TOKEN}/"):
            return self.send(200, PAGE, "text/html; charset=utf-8")
        if path == f"/{TOKEN}/api/state":
            try:
                return self.send(200, json.dumps(build_state(), default=str), "application/json")
            except Exception:
                return self.send(500, json.dumps({"error": "could not read the bot's data"}), "application/json")
        if path == "/robots.txt":
            return self.send(200, "User-agent: *\nDisallow: /\n", "text/plain")
        return self.send(404, "Not found", "text/plain")


if __name__ == "__main__":
    TOKEN = open(os.path.join(HERE, ".dash_token")).read().strip()
    PAGE = open(os.path.join(HERE, "dashboard.html"), encoding="utf-8").read()
    ThreadingHTTPServer(("127.0.0.1", PORT), H).serve_forever()
