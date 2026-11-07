# Rage Radium Bot

The real-money copy of the main $200 paper account. Same signals, same 13 wallets, same filters, same exits
(−25% stop, +15% breakeven, +$5 floor, live-price checks, trail from +40%, half at 2x, copied-wallet sell).
Its code is `radium.py`; it starts with each scan run and hands over to `radium-bridge.yml` between runs.

## Modes (repository variable `RADIUM_MODE`)
- `dry-run` (now): real Jupiter quotes and real swap transactions built for the bot's wallet, then thrown away.
  Nothing is signed or sent.
- `live`: signs and sends. Only after the wallet is funded, the key secret is added, the limits are confirmed and
  Rage says go.
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
| RADIUM_MAX_LOSS_USD | 5 | stake sized so the −25% stop loses about this, costs included |
| RADIUM_MAX_STAKE_USD | 20 | never stake more than this |
| RADIUM_MIN_STAKE_USD | 8 | skip if costs would push the stake below this |
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
