# Rage Radium Bot

A standalone real-money copy-trader with its own job (`radium.yml`), its own signal engine and its own page:
https://ragedele-glitch.github.io/rage-radium-bot/ (the full link carries the page key after the #).

Since Nov 8 it no longer copies the paper main account. Its engine (`scanner.py` run with `RADIUM_ENGINE=1` and its
own database, saved in the `radium-engine-` cache) follows the same wallets as the paper main account, reads their
buys and sells itself and runs the same filters ($25k pool minimum, 32.5% top-10 holder block, safety checks) and
exits (−25% stop, +15% breakeven, +$5 floor, live-price checks every ~4 s, trail from +40%, half at 2x, breakeven
floor from +50%, copied-wallet sell) on a $20 reference position. `radium.py` buys the moment the engine opens and
sells when the engine sells, and guards open trades itself (live prices, its own $5 line, copied-wallet check) while
the engine restarts. `radium_runner.py` runs both. Between runs `radium-bridge.yml` holds the lock and the trades.
The paper accounts (`scan.yml`) run unchanged, for comparison. The paper job publishes the main account's wallet
list, sealed, as `follow.enc` on the data branch; the engine reads it every 5 minutes.

Page data: `radium.enc` on the `radium-data` branch, sealed with the `RADIUM_DASH_KEY` secret. The page code lives in
the `ragedele-glitch/rage-radium-bot` repo. The wallet address is never written to it.

## Modes (repository variable `RADIUM_MODE`)
- `dry-run`: real Jupiter quotes and real swap transactions built for the bot's wallet, then thrown away.
  Nothing is signed or sent.
- `live` (now, since Nov 14 at Rage's request): signs and sends real trades. Set while the phone switch is on Pause,
  so nothing buys until Rage resumes it himself.
- `off`: Radium doesn't run.

## Wallet
- Address: kept in the `RADIUM_WALLET_ADDRESS` secret (not shown on the dashboard).
- Key: the `RADIUM_WALLET_KEY` secret, pasted in by Rage from Phantom (base58 private key). At startup the bot
  checks the key belongs to the address and refuses live trading if it doesn't.

## Add the key (phone)
1. Phantom: tap the account icon (top left) > Add / Connect Wallet > Create New Account. Name it Radium.
2. Phantom: account icon > the pencil next to the Radium account > Show Private Key > enter your password >
   tap to copy it. Never paste it anywhere else.
3. Browser (not the GitHub app): github.com/ragedele-glitch/paper-scanner > Settings > Secrets and variables >
   Actions > New repository secret.
4. Name: `RADIUM_WALLET_KEY`. Secret: paste. Tap Add secret.

## Settings (repository variables, Settings > Secrets and variables > Actions > Variables)
| Variable | Default | Meaning |
|---|---|---|
| RADIUM_MAX_LOSS_USD | 5 | Radium's own max-loss line: sells everything at this loss; a coin whose costs would make the −25% stop lose more is skipped (now $2) |
| RADIUM_MAX_STAKE_USD | 20 | buy size per trade, never more than this (now $4) |
| RADIUM_MIN_STAKE_USD | 5 | no buy below this; equal to the max stake for a fixed buy size (now $4) |
| RADIUM_MAX_OPEN | 3 | max open trades |
| RADIUM_DAILY_LOSS_USD | 15 | no new buys after this much lost today (Lagos day) |
| RADIUM_TOTAL_LOSS_USD | 30 | no new buys after this much lost in total |
| RADIUM_BUY_SLIPPAGE_BPS | 1000 | 10% buy slippage cap |
| RADIUM_SELL_SLIPPAGE_BPS | 1500 | 15%, widened on retries up to a 30% hard cap |
| RADIUM_MAX_IMPACT_PCT | 5 | skip if Jupiter's price impact is higher |
| RADIUM_MAX_LAG_S | 30 | skip if Radium sees the paper buy later than this |
| RADIUM_PRIORITY_MAX_LAMPORTS | 1000000 | priority fee cap, 0.001 SOL |
| RADIUM_FEE_RESERVE_SOL | 0.03 | SOL always kept for fees |

## Phone switch
Actions > "Rage Radium Bot switch" > Run workflow > `pause`, `sell everything + stop`, or `resume`.
