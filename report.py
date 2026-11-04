"""Plain-text health and results report for the PAPER-MODE memecoin copy-trade scanner.

Runs wherever the database is. On GitHub Actions it is built into the encrypted dashboard data every few minutes;
on the Sesame VM, /home/sprite/memecoin-scanner/report.py fetches and prints that copy.
  python report.py            # report from the local scanner.db (SCANNER_DB env var to point elsewhere)
"""
import sqlite3, time, json
import config as C


def _q(con, sql, args=()):
    return con.execute(sql, args).fetchall()


def _awake(con, t0, t1):
    """Share of minutes between t0 and t1 with at least one bot check-in."""
    # same rule as the dashboard: a minute counts as awake if the bot checked in during it or the minute before
    # (one loop can take a little over a minute)
    beats = {int(r[0] // 60) for r in _q(con, "SELECT ts FROM heartbeat WHERE ts>=? AND ts<=?", (t0 - 60, t1))}
    ms = range(int(t0 // 60), int(t1 // 60) + 1)
    up = sum(1 for m in ms if m in beats or (m - 1) in beats)
    return min(100.0, 100 * up / max(1, len(ms))), up / 60


def _lag(con, where, args):
    v = sorted(r[0] for r in _q(con, f"SELECT detect_lag_s FROM signals WHERE detect_lag_s IS NOT NULL "
                                     f"AND status!='ignored' AND {where}", args))
    if not v:
        return "no buys yet"
    med = v[len(v) // 2]; p90 = v[min(len(v) - 1, int(len(v) * .9))]
    u10 = 100 * sum(1 for x in v if x <= 10) / len(v)
    return f"median {med:.1f} s, 90% within {p90:.0f} s, {u10:.0f}% within 10 s ({len(v)} buys)"


def _acct(con, acct, cash_key, t0):
    meta = {r[0]: json.loads(r[1]) for r in _q(con, "SELECT key,value FROM meta")}
    cash = meta.get(cash_key, C.START_EQUITY)
    opens = _q(con, "SELECT qty,last_price,size_usd FROM positions WHERE account=? AND status='open'", (acct,))
    ov = sum(max(0.0, (o[0] or 0) * (o[1] or 0) * (1 - C.SLIPPAGE) * (1 - C.BOT_FEE - C.POOL_FEE) - C.NETWORK_FEE_USD) for o in opens)
    closed = _q(con, "SELECT pnl_usd FROM positions WHERE account=? AND status='closed'", (acct,))
    wins = sum(1 for c in closed if c[0] > 0)
    eq = cash + ov
    return (f"${eq:.2f} ({(eq / C.START_EQUITY - 1) * 100:+.2f}% from $200 since {time.strftime('%a %b %d %H:%M', time.gmtime(t0 + 3600))} Lagos), "
            f"{len(closed)} closed ({wins} won, realised ${sum(c[0] for c in closed):+.2f}), {len(opens)} open")


def _shadow(con, acct):
    r = _q(con, "SELECT COUNT(*), COALESCE(SUM(pnl_usd),0), SUM(pnl_usd>0) FROM positions WHERE account=? AND status='closed'", (acct,))[0]
    o = _q(con, "SELECT COUNT(*) FROM positions WHERE account=? AND status='open'", (acct,))[0][0]
    return f"{r[0]} closed, {r[2] or 0} won, ${r[1]:+.2f} total at $20 each, {o} open"


def make_report(con, host="GitHub Actions (public repo ragedele-glitch/paper-scanner)"):
    now = time.time()
    meta = {r[0]: json.loads(r[1]) for r in _q(con, "SELECT key,value FROM meta")}
    start = meta.get("start_ts", now); real_start = meta.get("real_start_ts")
    last = (_q(con, "SELECT MAX(ts) FROM heartbeat")[0][0]) or 0
    L = []
    L.append("PAPER MODE ONLY: no wallet, no keys, nothing signed. Runs on " + host + ".")
    L.append(f"Last check-in {(now - last) / 60:.1f} min ago.")
    a24, _ = _awake(con, now - 86400, now)
    a_all, h_all = _awake(con, start, now)
    L.append(f"Coverage: awake {a24:.1f}% of the last 24 h; {a_all:.1f}% since the start ({h_all:.0f} of {(now - start) / 3600:.0f} h).")
    if real_start:
        a_r, h_r = _awake(con, real_start, now)
        L.append(f"  Since the realistic version started: awake {a_r:.1f}% ({h_r:.1f} of {(now - real_start) / 3600:.1f} h).")
    f = meta.get("feed") or {}
    beats = _q(con, "SELECT ws FROM heartbeat WHERE ts>? AND ws IS NOT NULL", (now - 86400,))
    if f:
        up = (f"connected to {f.get('url')}" if f.get("connected") and now - (f.get("ts") or 0) < 120 else "DOWN")
        share = f"{100 * sum(1 for b in beats if b[0]) / len(beats):.1f}%" if beats else "n/a"
        side = f.get("side") or [0, 0]
        L.append(f"Live wallet feed: {up}; up at {share} of check-ins in the last 24 h; {f.get('reconnects', 0)} reconnects this run; "
                 f"{len(f.get('noisy') or [])} very noisy wallets on {side[1]} side connections ({side[0]} up).")
    else:
        L.append("Live wallet feed: not running (holdings re-reads only).")
    L.append("Detection delay (smart wallet's buy -> scanner saw it):")
    L.append("  live feed, last 24 h: " + _lag(con, "detect_src='stream' AND detected_ts>?", (now - 86400,)))
    L.append("  backup re-read, last 24 h: " + _lag(con, "COALESCE(detect_src,'poll')='poll' AND detected_ts>?", (now - 86400,)))
    L.append("  before the live feed: " + _lag(con, "detected_ts<?", (real_start or now,)))
    if real_start:
        L.append("HEADLINE, realistic entry (buys at the moment the scanner saw the wallet's buy, plus slippage and fees):")
        L.append("  $200 account: " + _acct(con, C.ACCT_REAL, "cash_real", real_start))
        L.append("  Every survivor: " + _shadow(con, C.ACCT_REAL_SHADOW))
    L.append("Old test, entry 5 s after the smart wallet (optimistic, kept for comparison):")
    L.append("  $200 account: " + _acct(con, C.ACCT_5S, "cash", start))
    L.append("  Every survivor: " + _shadow(con, C.ACCT_5S_SHADOW))
    pair = _q(con, f"""SELECT r.entry_price, f.entry_price FROM positions r JOIN positions f ON f.signal_id=r.signal_id
                       AND f.account={C.ACCT_5S_SHADOW} WHERE r.account={C.ACCT_REAL_SHADOW}""")
    gaps = sorted((r[0] / r[1] - 1) * 100 for r in pair if r[0] and r[1])
    if gaps:
        L.append(f"  Same coins, realistic entry price vs 5 s entry: median {gaps[len(gaps) // 2]:+.1f}% ({len(gaps)} coins).")
    s24 = _q(con, "SELECT status, COUNT(*) FROM signals WHERE detected_ts>? GROUP BY status", (now - 86400,))
    L.append("Signals, last 24 h: " + (", ".join(f"{k} {n}" for k, n in s24) or "none"))
    for p in _q(con, f"SELECT account,symbol,entry_price,last_price,size_usd FROM positions WHERE status='open' AND account IN ({C.ACCT_REAL},{C.ACCT_5S})"):
        tag = "realistic" if p[0] == C.ACCT_REAL else "5 s test"
        ch = (p[3] / p[2] - 1) * 100 if p[2] and p[3] else 0
        L.append(f"  open [{tag}] {p[1]} ${p[4]:.2f}, price {ch:+.0f}% since entry")
    return "\n".join(L)


if __name__ == "__main__":
    import scanner
    print(make_report(scanner.db()))
