#!/usr/bin/env python3
"""Checks for live mode. Nothing here has a key, signs anything or sends anything.

Run: python test_live.py          (offline checks only)
     python test_live.py --quotes (also a dry run against real Jupiter quotes, read-only GET requests)
"""
import importlib, json, os, sqlite3, sys, time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

GOOD = dict(LIVE_TRADE_USD="5", LIVE_TOTAL_BUDGET_USD="25", LIVE_MAX_OPEN="3", LIVE_DAILY_LOSS_USD="5",
            LIVE_SLIPPAGE_BPS="300", LIVE_MAX_PRICE_IMPACT_PCT="3", LIVE_MAX_SIGNAL_AGE_S="90",
            LIVE_MAX_WALLET_USD="60", LIVE_KILL="0")
ALL = list(GOOD) + ["LIVE_MODE", "LIVE_WALLET_ADDRESS", "LIVE_WALLET_KEY", "LIVE_RPC_URL", "JUP_API_KEY",
                    "GITHUB_REPOSITORY", "GITHUB_TOKEN"]
BONK = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"
WIF = "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm"
results = []


def fresh(env):
    for k in ALL:
        os.environ.pop(k, None)
    os.environ.update(env)
    import live
    live = importlib.reload(live)
    calls = {"get": 0, "post": 0}
    real = live._http

    def spy(url, body=None, headers=None, timeout=30):
        calls["post" if body is not None else "get"] += 1
        if not ONLINE:
            raise AssertionError("network call in an offline check: " + url)
        if body is not None:
            raise AssertionError("a POST was attempted: " + url)
        return real(url, body, headers, timeout)
    live._http = spy

    def no_sign(*a, **k):
        raise AssertionError("tried to sign/send")
    live._swap_and_send = no_sign
    con = sqlite3.connect(":memory:"); con.row_factory = sqlite3.Row
    return live, con, calls


def check(name, ok, detail=""):
    ok = bool(ok); results.append(ok)
    print(("PASS " if ok else "FAIL ") + name + (f"  [{detail}]" if detail else ""))


def tables(con):
    return {r[0] for r in con.execute("SELECT name FROM sqlite_master")}


ONLINE = False

# 1. default: off, no network, no tables
live, con, calls = fresh({})
live.on_paper_buy(con, 1, BONK, "BONK", time.time()); live.on_paper_close(con, 1, BONK, "stop"); live.tick(con)
check("off by default: does nothing", live.mode() == "off" and calls == {"get": 0, "post": 0} and not tables(con),
      f"mode={live.mode()} calls={calls}")
check("off: dashboard says off", live.dashboard_status(con) == {"mode": "off"})

# 2. a missing setting -> refuses
for missing in GOOD:
    env = dict(GOOD, LIVE_MODE="dry-run"); env.pop(missing)
    live, con, calls = fresh(env)
    live.on_paper_buy(con, 1, BONK, "BONK", time.time())
    ok = live._state["settings"] is None and any(missing in p for p in live._state["problems"]) and calls["get"] == 0
    if not ok:
        check(f"refuses without {missing}", False, str(live._state["problems"]))
        break
else:
    check("refuses to run if any of the 9 settings is missing", True)
row = con.execute("SELECT kind,msg FROM live_events").fetchone()
check("refusal is written to the log", row and row[0] == "refused", row and row[1])

# 3. hard caps
for k, v in [("LIVE_TRADE_USD", "51"), ("LIVE_TOTAL_BUDGET_USD", "500"), ("LIVE_MAX_OPEN", "6"),
             ("LIVE_SLIPPAGE_BPS", "2000"), ("LIVE_KILL", "2"), ("LIVE_TRADE_USD", "30")]:
    live, con, calls = fresh(dict(GOOD, LIVE_MODE="dry-run", **{k: v}))
    s, problems = live.load_settings()
    check(f"refuses {k}={v}", s is None and problems, "; ".join(problems))

# 4. live mode without a key, with a bad key, with a typo in LIVE_MODE
live, con, calls = fresh(dict(GOOD, LIVE_MODE="live", LIVE_WALLET_ADDRESS="11111111111111111111111111111111"))
live.on_paper_buy(con, 1, BONK, "BONK", time.time())
check("live refuses with no key secret", live._state["settings"] is None and calls["get"] == 0, "; ".join(live._state["problems"]))
live, con, calls = fresh(dict(GOOD, LIVE_MODE="live", LIVE_WALLET_ADDRESS="11111111111111111111111111111111",
                              LIVE_WALLET_KEY="not-a-real-key"))
live.on_paper_buy(con, 1, BONK, "BONK", time.time())
msg = "; ".join(live._state["problems"])
check("live refuses a bad key, without echoing it", live._state["settings"] is None and "not-a-real-key" not in msg
      and "LIVE_WALLET_KEY" not in os.environ, msg)
live, con, calls = fresh(dict(GOOD, LIVE_MODE="on"))
check("LIVE_MODE typo refuses", live.load_settings()[0] is None)

# 5. the signer itself refuses outside live mode
live, con, calls = fresh(dict(GOOD, LIVE_MODE="dry-run"))
importlib.reload(live)
try:
    live._swap_and_send({"mode": "dry-run"}, {}); check("signer refuses in dry-run", False)
except RuntimeError as e:
    check("signer refuses in dry-run", "not in live mode" in str(e))

if "--quotes" in sys.argv:
    ONLINE = True
    ctl = os.path.join(os.path.dirname(os.path.abspath(__file__)), "live_control.json")
    original = open(ctl).read()
    try:
        live, con, calls = fresh(dict(GOOD, LIVE_MODE="dry-run"))
        live.on_paper_buy(con, 1, BONK, "BONK", time.time() - 10)
        live.on_paper_buy(con, 2, WIF, "WIF", time.time() - 10)
        t = [dict(r) for r in con.execute("SELECT * FROM live_trades")]
        ev = [r["msg"] for r in con.execute("SELECT msg FROM live_events")]
        check("dry run: 2 buys quoted and recorded", len(t) == 2 and all(int(x["token_raw"]) > 0 for x in t),
              " | ".join(ev[1:]))
        print("     SOL price from Jupiter: $%.2f" % live._state["sol_px"][1])
        for x in t:
            print(f"     {x['symbol']}: {x['sol_in']:.5f} SOL in, {int(x['token_raw']):,} raw tokens, impact {x['price_impact_pct']:.3f}%")
        live.on_paper_buy(con, 3, BONK, "BONK", time.time() - 500)
        check("dry run: old signal skipped", "signal too old" in con.execute(
            "SELECT msg FROM live_events ORDER BY rowid DESC").fetchone()[0])
        live.on_paper_buy(con, 4, WIF, "WIF", time.time() - 5)
        live.on_paper_buy(con, 5, BONK, "BONK", time.time() - 5)
        last = con.execute("SELECT msg FROM live_events ORDER BY rowid DESC").fetchone()[0]
        check("dry run: max 3 open", "already open" in last, last)
        live.on_paper_close(con, 1, BONK, "trailing stop")
        live.on_paper_close(con, 2, WIF, "leader sold")
        c = [dict(r) for r in con.execute("SELECT * FROM live_trades WHERE status='closed'")]
        check("dry run: 2 sells quoted, P&L recorded", len(c) == 2 and all(x["usd_out"] > 0 for x in c),
              " | ".join(f"{x['symbol']} ${x['usd_in']:.2f} -> ${x['usd_out']:.2f} ({x['pnl_usd']:+.3f})" for x in c))
        # daily loss limit and budget
        con.execute("UPDATE live_trades SET pnl_usd=-5 WHERE id=1"); con.commit()
        live.on_paper_buy(con, 6, BONK, "BONK", time.time() - 5)
        last = con.execute("SELECT msg FROM live_events ORDER BY rowid DESC").fetchone()[0]
        check("dry run: daily loss limit blocks new buys", "daily loss limit" in last, last)
        con.execute("UPDATE live_trades SET pnl_usd=-4.5, closed_ts=closed_ts-200000 WHERE id=1")
        con.execute("UPDATE live_trades SET pnl_usd=-11, closed_ts=closed_ts-200000 WHERE id=2"); con.commit()
        live.on_paper_buy(con, 7, BONK, "BONK", time.time() - 5)
        last = con.execute("SELECT msg FROM live_events ORDER BY rowid DESC").fetchone()[0]
        check("dry run: total budget blocks new buys", "budget used up" in last, last)
        # kill switch from live_control.json, re-read while running
        con.execute("DELETE FROM live_trades WHERE status='closed'"); con.commit()
        with open(ctl, "w") as f:
            json.dump({"kill": True}, f)
        live._state["control_tried"] = 0; live._last_tick["t"] = 0
        live.tick(con)
        left = con.execute("SELECT COUNT(*) FROM live_trades WHERE status='open'").fetchone()[0]
        live.on_paper_buy(con, 8, WIF, "WIF", time.time() - 5)
        last = con.execute("SELECT msg FROM live_events ORDER BY rowid DESC").fetchone()[0]
        check("kill switch file: sells everything open, blocks buys", left == 0 and "kill switch is on" in last,
              f"open after kill={left}; {last}")
        st = live.dashboard_status(con)
        check("dashboard summary", st["mode"] == "dry-run" and "book" in st, json.dumps(st["book"]))
        check("dry run never POSTed, signed or sent", calls["post"] == 0, f"GET requests: {calls['get']}, POST: {calls['post']}")
    finally:
        with open(ctl, "w") as f:
            f.write(original)

print(f"\n{sum(results)}/{len(results)} passed")
sys.exit(0 if all(results) else 1)
