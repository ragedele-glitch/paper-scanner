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
}


def sell_proceeds(qty, px):
    return max(0.0, qty * px * (1 - C.SLIPPAGE) * (1 - C.BOT_FEE - C.POOL_FEE) - C.NETWORK_FEE_USD)


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

    # ---- $200 paper account
    cash = meta.get("cash", C.START_EQUITY)
    opens = q("SELECT * FROM positions WHERE account=1 AND status='open' ORDER BY entry_ts DESC")
    open_rows = []; open_val = 0.0
    for p in opens:
        val = sell_proceeds(p["qty"], p["last_price"] or 0); open_val += val
        pnl = val - p["size_usd"] - C.NETWORK_FEE_USD
        open_rows.append({
            "coin": p["symbol"] or short(p["mint"]), "mint": short(p["mint"]), "entry_price": p["entry_price"],
            "now_price": p["last_price"], "size": p["size_usd"], "pnl": pnl, "pnl_pct": pnl / p["size_usd"] * 100,
            "stop": p["stop"], "stop_pct": (p["stop"] / p["entry_price"] - 1) * 100 if p["entry_price"] else None,
            "stop_kind": "trailing" if p["stop"] > p["entry_price"] else ("breakeven" if abs(p["stop"] / p["entry_price"] - 1) < .2 and p["stop"] >= p["entry_price"] else "initial"),
            "peak_pct": (p["peak"] / p["entry_price"] - 1) * 100 if p["entry_price"] else None,
            "wallet": names.get(p["leader"]) or short(p["leader"]), "entry_ts": p["entry_ts"],
            "price_age": now - (p["last_update"] or p["entry_ts"]),
        })
    equity = cash + open_val
    eq = [(r["ts"], r["equity"]) for r in q("SELECT ts,equity FROM equity ORDER BY ts")]
    start_ts = meta.get("start_ts", eq[0][0] if eq else now)
    curve = [(start_ts, C.START_EQUITY)] + eq
    closed_all = q("SELECT * FROM positions WHERE account=1 AND status='closed' ORDER BY exit_ts")
    # equity curve also steps at every closed trade, so it is exact even with gaps in the 5-minute samples
    run = C.START_EQUITY
    for c in closed_all:
        run += c["pnl_usd"]
    curve.append((now, equity))
    curve.sort()
    peak = -1e18; mdd = 0.0; mdd_usd = 0.0
    for _, v in curve:
        peak = max(peak, v)
        if peak > 0 and (peak - v) / peak > mdd:
            mdd = (peak - v) / peak; mdd_usd = peak - v

    hist = []
    for c in closed_all[::-1]:
        hist.append({"coin": c["symbol"] or short(c["mint"]), "mint": short(c["mint"]), "wallet": names.get(c["leader"]) or short(c["leader"]),
                     "entry_ts": c["entry_ts"], "exit_ts": c["exit_ts"], "entry_price": c["entry_price"], "exit_price": c["exit_price"],
                     "size": c["size_usd"], "pnl": c["pnl_usd"], "pnl_pct": c["pnl_pct"],
                     "reason": EXITS.get(c["exit_reason"], c["exit_reason"])})
    wins = [h for h in hist if h["pnl"] > 0]
    best = max(hist, key=lambda h: h["pnl_pct"]) if hist else None
    worst = min(hist, key=lambda h: h["pnl_pct"]) if hist else None

    # ---- signals feed
    sigs = []
    for s in q("SELECT * FROM signals ORDER BY detected_ts DESC LIMIT 400"):
        st = s["status"]; notes = s["notes"] or ""
        if st == "survivor":
            if "account full" in notes:
                verdict, label = "passed", "Passed checks, not copied: 5 trades already open"
            elif "not enough paper cash" in notes:
                verdict, label = "passed", "Passed checks, not copied: not enough paper cash"
            else:
                verdict, label = "copied", "Copied (paper buy)"
        elif st == "killed":
            verdict = "killed"
            label = "Blocked: " + "; ".join(FILTERS.get(k, k) for k in (s["killed_by"] or "").split(",") if k)
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
                     "mcap": s["mcap"], "lag": s["detect_lag_s"], "p1h": s["p1h"], "p24h": s["p24h"], "partial": blind})

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
    sh = q("SELECT pnl_usd FROM positions WHERE account=0 AND status='closed'")

    errs, activity = parse_log(now)
    awake = last_beat is not None and now - last_beat < 120
    live_st = live_status(con)
    con.close()
    return {
        "now": now, "paper_only": live_st.get("mode") == "off", "live": live_st,
        "status": {"awake": awake, "last_beat": last_beat, "since_start_h": (now - start_ts) / 3600,
                   "wallets": len(wallets), "polled_10m": polled_10m, "errors": errs, "uptime": uptime(con2(), now)},
        "account": {"start": C.START_EQUITY, "equity": equity, "cash": cash, "open_value": open_val,
                    "change_pct": (equity / C.START_EQUITY - 1) * 100, "change_usd": equity - C.START_EQUITY,
                    "mdd_pct": mdd * 100, "mdd_usd": mdd_usd, "curve": curve[-600:], "n_open": len(open_rows),
                    "max_open": C.MAX_OPEN},
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
        "activity": activity,
        "rules": {"size": f"{int(C.POS_FRACTION*100)}% of the account per trade, max {C.MAX_OPEN} open",
                  "stop": f"Stop {int(C.INITIAL_STOP*100)}% under entry; moves to breakeven at +{int(C.BE_TRIGGER*100)}%, then trails",
                  "costs": f"{int(C.SLIPPAGE*100)}% slippage + {int((C.BOT_FEE+C.POOL_FEE)*100)}% fees each way + ${C.NETWORK_FEE_USD:.2f} network fee",
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
