"""Per-wallet results for the PAPER-MODE memecoin copy-trade scanner: which copied wallets win and which lose.

Read-only: it only reads scanner.db. It never removes a wallet from the copy list; it only flags wallets that keep
losing or keep dumping fast, so Rage can decide.

The numbers come from the flat-$20 test accounts (every coin that passed the checks, $20 each, no slot limit), so
every wallet gets a fair, same-size comparison. All P&L is after slippage, fees and network costs.
"""
import sqlite3
import config as C

MIN_TRADES = 5          # closed copies needed before a wallet gets a verdict
QUICK_DUMP_S = 5 * 60   # the wallet sold half or more of its bag within 5 minutes of buying
DUMP_FLAG_PCT = 50.0    # flag a wallet that dumps that fast on half or more of its buys


def _short(a):
    return (a[:4] + "…" + a[-4:]) if a and len(a) > 10 else (a or "")


def wallet_board(con, account, since=None):
    """One row per copied wallet for one test account, best total P&L first."""
    q = lambda s, a=(): con.execute(s, a).fetchall()
    since = since or 0
    info = {r[0]: (r[1], r[2]) for r in q("SELECT address,name,active FROM wallets")}
    rows = {}

    def row(w):
        if w not in rows:
            name, active = info.get(w, (None, 0))
            rows[w] = {"name": name or _short(w), "addr": _short(w), "following": bool(active),
                       "trades": 0, "wins": 0, "total": 0.0, "hold_s": 0.0, "open": 0,
                       "judged": 0, "quick": 0, "sold_before_copy": 0, "best": None, "worst": None}
        return rows[w]

    pos = q("""SELECT p.leader, p.status, p.entry_ts, p.exit_ts, p.pnl_usd, p.symbol, p.mint,
                      s.leader_buy_ts, ls.ts, ls.sold_frac
               FROM positions p LEFT JOIN signals s ON s.id = p.signal_id
               LEFT JOIN leader_sells ls ON ls.wallet = p.leader AND ls.mint = p.mint
               WHERE p.account = ? AND p.entry_ts >= ?""", (account, since))
    for leader, status, ets, xts, pnl, sym, mint, buy_ts, sell_ts, frac in pos:
        if not leader:
            continue
        r = row(leader)
        r["judged"] += 1
        if buy_ts and sell_ts and (frac or 0) >= C.MIRROR_SELL_FRAC and sell_ts - buy_ts <= QUICK_DUMP_S:
            r["quick"] += 1
        if status == "open":
            r["open"] += 1
            continue
        p = pnl or 0.0
        r["trades"] += 1; r["total"] += p; r["wins"] += p > 0
        r["hold_s"] += max(0.0, (xts or ets) - ets)
        coin = sym or _short(mint)
        if r["best"] is None or p > r["best"]["pnl"]:
            r["best"] = {"coin": coin, "pnl": p}
        if r["worst"] is None or p < r["worst"]["pnl"]:
            r["worst"] = {"coin": coin, "pnl": p}
    # buys the wallet sold before the scanner could even copy them: the fastest dumps of all
    for w, n in q("""SELECT wallet, COUNT(*) FROM signals WHERE status='killed' AND detected_ts >= ?
                     AND killed_by LIKE '%smart_wallet_sold_early%' GROUP BY wallet""", (since,)):
        r = row(w)
        r["sold_before_copy"] += n; r["judged"] += n; r["quick"] += n
    out = []
    for r in rows.values():
        if not r["trades"] and not r["open"]:
            continue   # never copied in this account (only blocked buys): not on the board
        n = r["trades"]
        r["win_rate"] = 100 * r["wins"] / n if n else None
        r["avg"] = r["total"] / n if n else None
        r["avg_hold_s"] = r["hold_s"] / n if n else None
        r["quick_pct"] = 100 * r["quick"] / r["judged"] if r["judged"] else None
        flags = []
        if n >= MIN_TRADES:
            verdict = "winning" if r["total"] > 0 else "losing"
            if r["total"] <= 0:
                flags.append("keeps losing")
        else:
            verdict = "too early"
        if r["judged"] >= MIN_TRADES and (r["quick_pct"] or 0) >= DUMP_FLAG_PCT:
            flags.append("dumps fast")
        r["verdict"] = verdict; r["flags"] = flags
        del r["hold_s"]
        out.append(r)
    out.sort(key=lambda r: (-r["total"], -r["trades"]))
    return out


def boards(con, real_start, start_ts):
    """Both views for the dashboard: realistic entry (headline) and the old 5-seconds-after test."""
    return {"min_trades": MIN_TRADES, "quick_dump_min": QUICK_DUMP_S // 60, "dump_flag_pct": DUMP_FLAG_PCT,
            "real": wallet_board(con, C.ACCT_REAL_SHADOW, real_start) if real_start else None,
            "real_since": real_start,
            "old": wallet_board(con, C.ACCT_5S_SHADOW, start_ts), "old_since": start_ts}


def _hold(s):
    if s is None:
        return "-"
    return f"{s / 60:.0f} min" if s < 3600 else f"{s / 3600:.1f} h"


def _line(r):
    wr = f"{r['win_rate']:.0f}% won" if r["win_rate"] is not None else "no closed trades"
    q = f", dumped within {QUICK_DUMP_S // 60} min on {r['quick']} of {r['judged']} buys" if r["judged"] else ""
    extra = (f", {r['open']} open" if r["open"] else "") + ("" if r["following"] else ", no longer on the Kolscan list")
    fl = f" [FLAG: {', '.join(r['flags'])}]" if r["flags"] else ""
    return (f"{r['name']}: {r['trades']} closed, {wr}, total ${r['total']:+.2f}, avg ${(r['avg'] or 0):+.2f}, "
            f"avg hold {_hold(r['avg_hold_s'])}{q}{extra}{fl}")


def report_lines(con, real_start, start_ts, top=3):
    """Short wallet leaderboard for the plain-text report."""
    L = [f"WALLET LEADERBOARD ($20 per copy, after costs; a wallet needs {MIN_TRADES}+ closed copies before a verdict; "
         f"flags only, nothing is removed automatically):"]
    for title, acct, t0 in (("Realistic entry", C.ACCT_REAL_SHADOW, real_start), ("Old 5 s test", C.ACCT_5S_SHADOW, start_ts)):
        if t0 is None:
            continue
        b = wallet_board(con, acct, t0)
        ready = [r for r in b if r["verdict"] != "too early"]
        L.append(f"  {title}: {len(b)} wallets copied, {len(ready)} with {MIN_TRADES}+ closed copies.")
        if not b:
            continue
        best = [r for r in b if r["trades"] and r["total"] > 0][:top]
        worst = [r for r in reversed(b) if r["trades"] and r["total"] <= 0][:top]
        for r in best:
            L.append("    best  " + _line(r))
        for r in worst:
            L.append("    worst " + _line(r))
        flagged = [r for r in b if r["flags"] and r not in best and r not in worst]
        for r in flagged:
            L.append("    flag  " + _line(r))
    return L


if __name__ == "__main__":
    import json, scanner
    con = scanner.db()
    meta = {k: json.loads(v) for k, v in con.execute("SELECT key,value FROM meta")}
    print("\n".join(report_lines(con, meta.get("real_start_ts"), meta.get("start_ts"))))
