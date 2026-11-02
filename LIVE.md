# Live mode (switched off)

Live mode lets the scanner copy its paper trades with real money, through Jupiter swaps on Solana.
It is **off**. There is no wallet, no private key and no real trade anywhere in this project.
Paper mode runs exactly as before.

## How it stays off

- Live mode only wakes up when the repository variable `LIVE_MODE` is set. It isn't set.
- `LIVE_MODE=live` also needs the `LIVE_WALLET_KEY` secret. That secret doesn't exist.
- If any setting is missing or out of range, live mode refuses to start, writes why in the log, and paper mode carries on.
- The code that signs a transaction refuses to run unless the mode is `live` and the key matched the wallet address.
- Any error in live mode is caught, so it can't stop paper mode.

## What it does when it's on

It follows the paper account. When the paper account opens a trade, live buys the same coin for a small fixed amount.
When the paper account closes that trade (stop, trailing stop, smart wallet sold, or the 24-hour limit),
live sells every token of that coin back to SOL.

## Modes

| LIVE_MODE | What happens |
|---|---|
| (empty) or `off` | Nothing. Paper only. |
| `dry-run` | Every check runs, and Jupiter gives a real price quote for each buy and sell. Nothing is signed or sent. No key needed. Good for comparing real quotes with paper results. |
| `live` | Real money. |

## Settings (repository variables)

All of these are required. Suggested starting values for a $25 test:

| Variable | Start with | What it does |
|---|---|---|
| `LIVE_MODE` | `dry-run`, later `live` | Master switch |
| `LIVE_WALLET_ADDRESS` | your new wallet's address | The key must belong to this address |
| `LIVE_TRADE_USD` | `5` | Dollars per trade (code won't allow more than 50) |
| `LIVE_TOTAL_BUDGET_USD` | `25` | Open trades plus all losses can never go past this (max 100) |
| `LIVE_MAX_OPEN` | `3` | Most trades open at once (max 5) |
| `LIVE_DAILY_LOSS_USD` | `5` | No new trades for the rest of the day (Lagos time) after losing this |
| `LIVE_SLIPPAGE_BPS` | `300` | Slippage limit, 300 = 3% (max 1500) |
| `LIVE_MAX_PRICE_IMPACT_PCT` | `3` | Skip a buy that would move the price more than this |
| `LIVE_MAX_SIGNAL_AGE_S` | `90` | Skip a buy if the smart wallet bought more than this many seconds ago |
| `LIVE_MAX_WALLET_USD` | `60` | Refuse to trade if the wallet holds more than this, so it can only ever be a small hot wallet |
| `LIVE_KILL` | `0` | `1` = kill switch at start |

Secrets (Settings > Secrets and variables > Actions > Secrets):

- `LIVE_WALLET_KEY`: the new wallet's private key. Live mode only.
- `LIVE_RPC_URL` (optional, recommended): a private Solana RPC link. The free public node is shared and rate-limited.
- `JUP_API_KEY` (optional): a free Jupiter API key, for a higher rate limit.

The code also always leaves 0.02 SOL in the wallet for fees.

## Kill switch

- **Fast:** open `live_control.json` in this repository (the GitHub app can edit it) and change `false` to `true`.
  Within about a minute the bot stops opening trades and sells every coin in the wallet back to SOL.
  If the bot can't read that file for 10 minutes, it stops opening trades by itself.
- **Hard stop:** Actions > Paper scanner > cancel the running run, then the ... menu > Disable workflow.
  That stops everything at once but doesn't sell. Sell anything left from your wallet app.

## Turning it on later

1. Watch the paper results for about two weeks first.
2. Optional: set the variables above with `LIVE_MODE=dry-run` for a few days to see real quotes. No key needed.
3. Make a brand-new wallet account used only for this bot. Never use your main wallet.
4. Send it about $30 of SOL. That covers the $25 budget and fees.
5. Add its private key as the `LIVE_WALLET_KEY` secret, and its address as `LIVE_WALLET_ADDRESS`.
6. Change `LIVE_MODE` to `live`. It takes effect at the next run, or cancel the current run and start a new one.

## Risks

- **Real results will be worse than paper.** Paper assumes it bought 5 seconds after the smart wallet.
  The scanner usually spots a buy later (median 53 seconds in its own data so far), and by then early buyers and
  bots have often pushed the price up. Memecoin swaps can also be front-run or "sandwiched", so you often get close
  to your full slippage limit. Some coins can't be sold at all.
- **This repository is public.** The key would sit in a GitHub secret, which is masked in logs, but the code and
  run history are public, and GitHub's terms make a nonstop bot on free runners a gray area. GitHub could stop the
  runs while trades are open.
- **Better home for real money:** a small always-on server you control, such as an Oracle Cloud free VM, with the
  key in a file only that server can read and a private RPC. The paper scanner can stay on GitHub.

## Tests

`python test_live.py` runs the offline checks. `python test_live.py --quotes` adds a dry run against real
Jupiter quotes. Neither has a key, and neither signs or sends anything.
