"""Pool-band shadow test (Rage, Nov 7 evening): dashboard card and report lines. Read-only.

The scanner logs, for every new coin, whether each version would have skipped it (signals.pb_band / pb_u46), the pool
figure it saw (pb_liq) and where that figure came from (pb_src, pb_curve). LOGGED ONLY: nothing is ever skipped.
The effect shown is the closed P&L of the trades a version would have skipped, with the sign flipped (skipping a
-$5 loser = +$5). On the $200 account a skipped trade would also have freed a slot and changed later sizes; that
knock-on is not modelled.
"""
import json

VERSIONS = [("pb_band", "Skip pools $30k-46k"), ("pb_u46", "Skip pools under $46k")]


def _start(con):
    r = con.execute("SELECT value FROM meta WHERE key='pb_start_ts'").fetchone()
    return json.loads(r[0]) if r else None


def view(con):
    import config as C
    since = _start(con)
    out = {"since": since, "band": [C.PB_BAND_LO, C.PB_BAND_HI], "under": C.PB_UNDER_MAX, "judge_n": C.PB_JUDGE_N,
           "versions": [{"key": k, "label": lab} for k, lab in VERSIONS], "accounts": [], "recent": []}
    if since is None:
        out["pending"] = True
        return out
    for acct, lab in ((C.ACCT_REAL, "Main $200 account"), (C.ACCT_REAL_SHADOW, "Flat-$20 test (every survivor)")):
        rows = con.execute("SELECT p.symbol, p.mint, p.status, p.pnl_usd, p.entry_ts, s.pb_liq, s.pb_src, s.pb_curve, "
                           "s.pb_band, s.pb_u46 FROM positions p JOIN signals s ON s.id=p.signal_id "
                           "WHERE p.account=? AND p.entry_ts>=?", (acct, since)).fetchall()
        closed = [r for r in rows if r[2] == "closed"]
        a = {"label": lab, "trades": len(rows), "closed": len(closed), "open": len(rows) - len(closed),
             "wins": sum(1 for r in closed if (r[3] or 0) > 0), "pnl": sum(r[3] or 0 for r in closed),
             "no_pool": sum(1 for r in rows if r[8] is None), "versions": {}}
        for i, (k, _) in enumerate(VERSIONS):
            col = 8 + i
            sk = [r for r in rows if r[col] == 1]
            skc = [r for r in sk if r[2] == "closed"]
            spnl = sum(r[3] or 0 for r in skc)
            a["versions"][k] = {"skipped": len(sk), "closed": len(skc), "open": len(sk) - len(skc),
                                "wins": sum(1 for r in skc if (r[3] or 0) > 0),
                                "losses": sum(1 for r in skc if (r[3] or 0) <= 0),
                                "skipped_pnl": spnl, "effect": -spnl, "pnl_with": a["pnl"] - spnl,
                                "from_rugcheck": sum(1 for r in sk if r[6] == "rugcheck"),
                                "on_curve": sum(1 for r in sk if r[7] == 1)}
        out["accounts"].append(a)
        if acct == C.ACCT_REAL:
            for r in sorted(rows, key=lambda r: -(r[4] or 0)):
                if r[8] == 1 or r[9] == 1:
                    out["recent"].append({"coin": r[0] or r[1][:6], "status": r[2], "pnl": r[3], "ts": r[4], "pool": r[5],
                                          "src": r[6], "curve": r[7], "band": r[8], "u46": r[9]})
    out["recent"] = out["recent"][:12]
    out["logged"] = con.execute("SELECT COUNT(*) FROM signals WHERE pb_liq IS NOT NULL AND detected_ts>=?",
                                (since,)).fetchone()[0]
    return out


def report_lines(con):
    v = view(con)
    if v.get("pending"):
        return ["POOL-BAND SHADOW TEST (logged only, never blocks): starts at the next scanner start."]
    m = v["accounts"][0]
    L = [f"POOL-BAND SHADOW TEST (logged only, never blocks; new trades only, {m['closed']} of ~{v['judge_n']} closed "
         f"main-account trades needed before judging). Effect = P&L of the skipped trades, sign flipped:"]
    for a in v["accounts"]:
        parts = []
        for k, lab in VERSIONS:
            g = a["versions"][k]
            parts.append(f"{lab}: would skip {g['skipped']} ({g['wins']} won, {g['losses']} lost"
                         + (f", {g['open']} open" if g["open"] else "") + f"), effect ${g['effect']:+.2f}"
                         + (f", {g['from_rugcheck']} on a RugCheck pool figure" if g["from_rugcheck"] else ""))
        L.append(f"  {a['label']} ({a['closed']} closed, ${a['pnl']:+.2f}): " + "; ".join(parts))
    return L
