import os
# Paper-mode Solana memecoin copy-trade scanner: settings.
# PAPER MODE ONLY. No wallet, no keys, nothing is ever signed or sent on-chain.

RPC_URL = "https://api.mainnet-beta.solana.com"   # free public Solana node (read-only calls)
# Live feed of the followed wallets' transactions (read-only websocket, logsSubscribe). Tried in order; on a drop
# it reconnects, moving to the next one if a connection fails quickly. A free Helius key, if ever added as the
# HELIUS_API_KEY secret, is used first, for both the feed and the RPC reads.
HELIUS_API_KEY = os.environ.get("HELIUS_API_KEY") or ""
WS_URLS = ([f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"] if HELIUS_API_KEY else []) + [
    "wss://api.mainnet-beta.solana.com",
    "wss://solana-rpc.publicnode.com",
]
if HELIUS_API_KEY:
    RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
POLL_BACKUP_S = 600         # while the live feed is up, holdings are still re-read every 10 min as a safety net
DEX_RETRY_S = 120           # a brand-new coin may take a minute to appear on DexScreener: keep retrying for 2 min
KOLSCAN_URL = "https://kolscan.io/leaderboard"     # public leaderboard of profitable Solana wallets
DEX_TOKENS = "https://api.dexscreener.com/tokens/v1/solana/"
GECKO = "https://api.geckoterminal.com/api/v2/networks/solana"
RUGCHECK = "https://api.rugcheck.xyz/v1/tokens/{}/report"

TOKEN_PROGRAMS = [
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",   # SPL Token
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",   # Token-2022 (newer pump.fun coins)
]

# --- Scout ---
MAX_WALLETS = 50            # follow up to 50 wallets
# Rage's proven wallets (big wins: Stigman HeeHaw, Fox Lealana): always followed, even after they drop off the board.
PINNED_WALLETS = {"8fsKLLtvKNanL4ginCaiRS6UfeemY11rSf8U8fN1dJw4": "Stigman",
                  "DAEdBmTPEKM6xkwfzC3d411QUe6coKpkND6UURa4CvHC": "Fox"}
# Wallets taken off the copy list because they kept losing (removed_wallets.json, with their stats at removal).
# They are never followed again, even when they are back on the leaderboard. To add one back, delete its entry.
try:
    import json as _json
    REMOVED_WALLETS = {w["address"] for w in _json.load(open(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                                                         "removed_wallets.json")))["removed"]}
    REMOVED_ERROR = None
except Exception as _e:   # a broken file must not stop the scanner; the error is logged at start
    REMOVED_WALLETS = set(); REMOVED_ERROR = f"{type(_e).__name__}: {_e}"
BOARD_KEEP_DAYS = 3         # keep following a wallet for 3 days after it last appeared on the board
BOARD_REFRESH_S = 6 * 3600  # re-read the leaderboard every 6 hours
LOOP_S = 20
GAP_REBASELINE_S = 30 * 60  # if a wallet wasn't polled for 30+ min (VM asleep), re-baseline silently
MISSED_LAG_S = 10 * 60      # buys found 10+ min late are logged as missed, not traded
MIN_LEADER_BUY_USD = 20     # ignore dust/airdrops worth under $20

# --- Filter (rules from the post) ---
DEV_MAX_PCT = 5.0
LIQ_MIN_USD = 25_000
TOP10_MAX_PCT = 35.0
BUNDLE_INSIDER_PCT = 5.0    # insider/bundle-linked holders owning 5%+ of supply = bundled
PUMP_1H_MAX_PCT = 100.0

# --- Sniper / paper account ---
START_EQUITY = 200.0
POS_FRACTION = 0.10         # 10% of current equity per trade ($20 to start)
MIN_POS_USD = 5.0
MAX_OPEN = 5
COPY_DELAY_S = 5            # old, optimistic test: copier buys 5 seconds after the smart wallet
# Headline (realistic) accounts buy at the moment the scanner actually saw the smart wallet's buy.
ACCT_REAL, ACCT_REAL_SHADOW = 11, 10   # $200 account and flat-$20 test, realistic entry
ACCT_5S, ACCT_5S_SHADOW = 1, 0         # the same two, entering 5 s after the smart wallet (kept for comparison)
# Rug filter shadow test (from Nov 7, 2026). A second realistic $200 account that takes the same trades as ACCT_REAL,
# except the ones the rug filter would skip. The current rules are unchanged; this only shows what the filter would do.
#  1. pump.fun curve coin where the top 10 holders (leaving out the bonding curve and the copied wallet) own
#     HF_TOP10_MAX % or more of the supply when the scanner sees the buy (rug backtest Nov 4: caught BASS and it)
#  2. round trip at entry (buy then sell back, real quote at the trade's size) of HF_RT_MAX % or more
#     (backtest Nov 7: only PVE 30.2% and PAMPIT 20.4% were that high, both losers; no winner above 15.7%)
ACCT_HF = 21
# One-time paper balance reset (Rage asked Nov 7, 2026). The scanner applies it once, at its next start, on its own
# saved database, so a running copy can't overwrite it. Each listed $200 paper account goes back to $200 equity
# (still-open paper trades keep running and count inside the $200). Nothing is deleted: the old session's totals are
# archived in the paper_resets table, every closed trade stays in the database (wallet leaderboard, exit comparison
# and wallet scorecard still use all of them), and the account cards and report count trades closed from the reset on.
PAPER_RESET = {"id": "2026-11-07-200", "accounts": [ACCT_REAL, ACCT_5S]}   # realistic $200 account and the old 5 s test
HF_TOP10_MAX = 22.0
HF_RT_MAX = 20.0
SLIPPAGE = 0.03             # 3% worse price on every buy and every sell
BOT_FEE = 0.01              # 1% copy-bot fee per side (typical Telegram copy bots)
POOL_FEE = 0.01             # ~1% DEX / pump.fun fee per side
NETWORK_FEE_USD = 0.10      # priority fee + tip per transaction
# Live-like fills (fills.py), used by the realistic accounts from the switch-over onward. Read-only quotes, paper only.
EXEC_DELAY_S = 2            # seconds from the scanner seeing the buy to the copy landing on chain
COPY_SLIPPAGE_TOL = 0.15    # copy bot's slippage limit: a buy whose price ran up more than 15% before landing fails
BASE_FEE_LAMPORTS = 5000    # Solana base fee per signature
SWAP_COMPUTE_UNITS = 250_000  # compute units a typical swap transaction pays priority on
PRIORITY_MICROLAMPORTS_FALLBACK = 1_000_000   # if the RPC can't be read
PRIORITY_MAX_SOL = 0.01     # cap on the priority fee per transaction
JITO_TIP_SOL = 0.001        # tip per landed transaction, as copy bots commonly set it
SOL_USD_FALLBACK = 150.0    # only if no SOL price could ever be read
MIN_SELLABLE_LIQ_USD = 500  # below this pool liquidity, with no Jupiter sell route, a sell gets nothing

# --- Guard ---
INITIAL_STOP = 0.30         # stop 30% under entry, set before entry
BE_TRIGGER = 0.40           # once up 40%, stop moves to breakeven after costs
BE_LEVEL = 1.12             # 12% over entry price ~= breakeven after ~10% round-trip costs
TRAIL_STEPS = [             # (peak gain reached, stop distance under the peak)
    (0.40, 0.25),
    (1.00, 0.20),
    (3.00, 0.15),
]
# Exit rules v3 (from Nov 4, 2026 afternoon; positions opened earlier keep the rules above)
MAX_LOSS_PCT = 0.25         # $5 max-loss stop: 25% under entry = $5 on a $20 trade (real fill can be worse)
BE2_TRIGGER = 0.50          # once up 50%, stop moves to the real breakeven price after all costs
HALF_AT = 2.0               # runner plan: at 2x the entry price sell half (takes back about the stake)
HALF_FRAC = 0.5
TRAIL_V3 = [                # runner trail: widens as the trade grows, so the second half can run
    (0.40, 0.25),           # from +40%: 25% under the peak (same as the old trail)
    (1.00, 0.30),           # from 2x: 30%
    (2.00, 0.40),           # past 3x: 40%
]
# exit comparison: flag the new exits once this many finished trades are compared, with at least this many differing
CMP_MIN_TRADES = 20
CMP_MIN_DIFFER = 5
MIRROR_SELL_FRAC = 0.50     # exit if the smart wallet sells 50%+ of its bag
TIME_STOP_S = 24 * 3600     # close anything still open after 24 hours
# Stale-candle guard (Nov 7): GeckoTerminal has no candles for some pools (e.g. a PumpSwap pool paired with another
# memecoin, like SMOOVE/NIO), so the bot's price froze at entry and the stop was never checked. When the newest candle
# is older than this, the bot checks stops, the 2x half-sell and the trail on a live market price instead
# (Jupiter first, DexScreener's biggest pool as backup). Price source only: stop levels and rules are unchanged.
QUOTE_STALE_S = 180
QUOTE_MAX_AGE_S = 60        # a live price older than this is not used

EXCLUDE_MINTS = {
    "So11111111111111111111111111111111111111112",  # wSOL
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
    "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",  # JUP
    "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So",  # mSOL
    "J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn",  # JitoSOL
    "7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj",  # stSOL
    "bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piy1",  # bSOL
    "2b1kV6DkPAnxd5ixfnxCpjxmKwqjjaYmCZfHsFu24GXo",  # PYUSD
}

DB_PATH = os.environ.get("SCANNER_DB", "/home/sprite/memecoin-scanner/scanner.db")
LOG_PATH = os.environ.get("SCANNER_LOG", "/home/sprite/memecoin-scanner/scanner.log")
