"""Live feed of the followed wallets' transactions over a read-only Solana websocket (logsSubscribe).

PAPER MODE: this only LISTENS. It subscribes to public transaction logs that mention each wallet; nothing
is ever signed or sent. Every notification is a transaction signature; the scanner then reads that one
transaction to see what the wallet bought or sold. This replaces minute-by-minute holdings polling, so a
smart wallet's buy is seen within seconds instead of about a minute.

A handful of the followed wallets are bots that fire thousands of failed transactions a minute. Those "noisy"
wallets are moved to their own connections (3 per connection) so that, when the public node drops a flooded
connection, the main connection carrying every other wallet keeps running. Failed transactions and ones that
never touch a token program are dropped here.
"""
import asyncio, json, logging, queue, threading, time
from collections import deque

log = logging.getLogger("scanner")
NEW_ACCOUNT_MARKS = ("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL invoke", "Instruction: InitializeAccount",
                     "Instruction: InitializeImmutableOwner", "Log truncated")
TOKEN_MARKS = ("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb", "Log truncated")
NOISY_PER_MIN = 200          # a wallet mentioned in more than this many transactions a minute gets its own connection
NOISY_GROUP = 3


class _Group:
    def __init__(self, wallets, main, url_i):
        self.wallets = wallets; self.main = main; self.url_i = url_i
        self.connected = False; self.subscribed = 0; self.url = None; self.err = None
        self.up_since = None; self.last_msg = 0.0; self.last_note = 0.0


class WalletStream:
    def __init__(self, urls, user_agent="memecoin-paper-scanner/1.1 (read-only)", noisy=(), keepalive=False):
        self.urls = [u for u in urls if u]
        # keepalive (Radium only): a quiet connection is proven alive with a websocket ping instead of being torn down
        # after 3 quiet minutes and counted as 'down' after 90 s, so the backup re-read is needed far less
        self.keepalive = keepalive
        self.ua = user_agent
        self.q = queue.Queue(maxsize=50000)
        self._want = ()
        self.noisy = set(noisy)
        self.groups = {}            # key -> _Group
        self.reconnects = 0
        self.n_all = 0              # every notification, failed ones included
        self.n_kept = 0             # successful token transactions handed to the scanner
        self.up_s = 0.0             # seconds the main connection has been up
        self.watch_all = set()      # wallets whose every token transaction is read (leaders of open paper positions)
        self.n_skipped = 0          # token transactions that opened no new token account and weren't from a watched wallet
        self._thread = None
        self._recent = deque(maxlen=20000); self._recent_set = set()   # same transaction from two endpoints: keep the first

    # --- read from the scanner's main thread
    def set_wallets(self, wallets):
        self._want = tuple(sorted(set(wallets)))

    def _main_group(self):
        """The connected main connection (there is one per endpoint, carrying the same wallets), else any."""
        mains = [g for g in list(self.groups.values()) if g.main]
        for g in mains:
            if g.connected:
                return g
        return mains[0] if mains else None

    def mains_up(self):
        mains = [g for g in list(self.groups.values()) if g.main]
        return sum(1 for g in mains if g.connected), len(mains)

    @property
    def connected(self):
        return any(g.main and g.connected for g in list(self.groups.values()))

    @property
    def url(self):
        g = self._main_group(); return g.url if g else None

    @property
    def err(self):
        g = self._main_group(); return g.err if g else None

    @property
    def last_msg(self):
        g = self._main_group(); return g.last_msg if g else 0.0

    @property
    def subscribed(self):
        g = self._main_group()
        return (g.subscribed if g else 0) + sum(x.subscribed for x in list(self.groups.values()) if not x.main)

    def side_status(self):
        side = [g for g in list(self.groups.values()) if not g.main]
        return sum(1 for g in side if g.connected), len(side)

    def healthy(self):
        return any(g.main and g.connected and time.time() - g.last_msg < 90 for g in list(self.groups.values()))

    def uptime_s(self):
        return self.up_s

    def start(self):
        if not self.urls:
            return False
        try:
            import websockets  # noqa: F401
        except ImportError:
            log.warning("websockets package missing: live feed off, falling back to holdings re-reads only")
            return False
        self._thread = threading.Thread(target=lambda: asyncio.run(self._supervise()), daemon=True, name="wallet-stream")
        self._thread.start()
        return True

    # --- background thread
    def _layout(self):
        want = set(self._want)
        noisy = sorted(self.noisy & want)
        quiet = tuple(sorted(want - set(noisy)))
        # the quiet wallets ride one connection per endpoint at the same time (first copy of a transaction wins),
        # so a drop on one endpoint loses nothing; noisy wallets get one connection each group of 3
        out = [((quiet, i), True, i) for i in range(min(2, len(self.urls)))] if quiet else []
        for i in range(0, len(noisy), NOISY_GROUP):
            out.append(((tuple(noisy[i:i + NOISY_GROUP]), -1), False, 0))
        return out

    async def _supervise(self):
        tasks = {}; last = time.time()
        while True:
            t = time.time()
            if self.connected:
                self.up_s += t - last       # feed time = time at least one main connection was up
            last = t
            layout = self._layout()
            keys = {k for k, _, _ in layout}
            for k in list(tasks):
                if k not in keys:
                    tasks.pop(k).cancel()
                    self.groups.pop(k, None)
            for k, main, ui in layout:
                if k not in tasks:
                    self.groups[k] = _Group(k[0], main, ui)
                    tasks[k] = asyncio.ensure_future(self._runner(self.groups[k]))
            await asyncio.sleep(1)

    def _down(self, g):
        g.up_since = None; g.connected = False; g.subscribed = 0

    async def _runner(self, g):
        i = g.url_i; backoff = 1; quick = 0
        try:
            while True:
                url = self.urls[i % len(self.urls)]
                t0 = time.time()
                try:
                    await self._session(g, url)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    g.err = f"{type(e).__name__}: {str(e)[:120]}"
                    if g.main and time.time() - t0 < 60:
                        log.warning("live feed connection failed (%s): %s", url.split("?")[0], g.err)
                self._down(g)
                self.reconnects += 1
                if time.time() - t0 > 60:
                    backoff = 1; quick = 0  # it ran fine for a while: reconnect at once, same endpoint
                else:
                    quick += 1
                    if not g.main or (self.keepalive and quick >= 2):
                        i += 1             # failed quickly (Radium: main feed too, after 2 quick failures): next endpoint
                    # Radium waits at most 10 s between tries, so the feed is back fast and the backup is rarely needed
                    backoff = min(10 if self.keepalive else 60, backoff * 2)
                await asyncio.sleep(backoff)
        finally:
            self._down(g)

    async def _session(self, g, url):
        import websockets
        want = g.wallets
        kw = dict(max_size=2 ** 24, ping_interval=20, ping_timeout=20 if self.keepalive else 60, open_timeout=20,
                  close_timeout=5)
        try:
            conn = websockets.connect(url, user_agent_header=self.ua, **kw)
        except TypeError:
            conn = websockets.connect(url, **kw)
        async with conn as ws:
            g.url = url.split("?")[0]
            for k, w in enumerate(want):
                await asyncio.sleep(0.05)
                await ws.send(json.dumps({"jsonrpc": "2.0", "id": k + 1, "method": "logsSubscribe",
                                          "params": [{"mentions": [w]}, {"commitment": "confirmed"}]}))
            subs = {}
            g.last_msg = time.time(); g.last_note = time.time()
            win = deque(); counts = {}       # per-wallet mentions in the last minute (main connection only)
            while True:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=20 if self.keepalive else 30)
                except asyncio.TimeoutError:
                    if self.keepalive:
                        # the websocket library pings every 20 s and drops the link if no answer comes within 20 s,
                        # so an open, quiet connection is a live one: it stays 'up' instead of being torn down
                        g.last_msg = time.time()
                        if time.time() - g.last_note > 1200:   # 20 quiet minutes even with pings: refresh the subscriptions
                            raise RuntimeError("no wallet activity for 20 minutes: refreshing the subscriptions")
                        continue
                    if time.time() - g.last_msg > 180:
                        raise RuntimeError("no messages for 3 minutes")
                    continue
                t = time.time()
                g.last_msg = t
                if '"logsNotification"' in raw:
                    self.n_all += 1; g.last_note = t
                    if '"err":null' not in raw and not g.main:
                        continue                 # failed transaction on a noisy connection: skip without parsing
                m = json.loads(raw)
                if "id" in m:
                    if "result" in m and isinstance(m["id"], int) and 0 < m["id"] <= len(want):
                        subs[m["result"]] = want[m["id"] - 1]
                        g.subscribed = len(subs)
                        if not g.connected and len(subs) >= max(1, int(len(want) * 0.8)):
                            g.connected = True; g.up_since = t; g.err = None
                            if g.main and self.reconnects < 10:
                                log.info("live feed connected to %s: %d wallets (+%d noisy on side connections)",
                                         g.url, len(want), len(self.noisy & set(self._want)))
                    elif "error" in m:
                        log.warning("live feed subscribe error: %s", str(m["error"])[:150])
                    continue
                if m.get("method") != "logsNotification":
                    continue
                res = m["params"]["result"]; v = res["value"]
                w = subs.get(m["params"]["subscription"])
                if not w:
                    continue
                if g.main:
                    win.append((t, w)); counts[w] = counts.get(w, 0) + 1
                    while win and t - win[0][0] > 60:
                        _, ow = win.popleft(); counts[ow] -= 1
                    if counts[w] > NOISY_PER_MIN / 2 and g.up_since and t - g.up_since > 30:
                        frac = min(1.0, (t - g.up_since) / 60)
                        new = {x for x, n in counts.items() if n > NOISY_PER_MIN * frac * 0.5}
                        self.noisy |= new
                        log.info("live feed: %d very noisy wallets moved to side connections (%s)", len(new),
                                 ",".join(sorted(x[:6] for x in new)))
                        return
                if v.get("err") is not None:
                    continue
                logs = v.get("logs") or []
                if not any(mk in l for l in logs for mk in TOKEN_MARKS):
                    continue
                # A first buy of a coin opens a token account for it. Only those transactions are read, plus every
                # token transaction of wallets we are copying right now (to see their sells). This keeps the reads
                # inside the free node's limits.
                if w not in self.watch_all and not any(mk in l for l in logs for mk in NEW_ACCOUNT_MARKS):
                    self.n_skipped += 1
                    continue
                sg = v["signature"]
                if sg in self._recent_set:
                    continue
                if len(self._recent) == self._recent.maxlen:
                    self._recent_set.discard(self._recent[0])
                self._recent.append(sg); self._recent_set.add(sg)
                self.n_kept += 1
                try:
                    self.q.put_nowait((t, w, v["signature"], res.get("context", {}).get("slot")))
                except queue.Full:
                    pass
