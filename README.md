# Paper scanner

A **paper-mode** Solana memecoin copy-trade scanner. It watches public smart wallets from the Kolscan leaderboard, runs safety filters on the coins they buy, and simulates trades on a $200 paper account.

- No wallet, no keys, no approvals. It never signs or sends anything on-chain.
- Runs on GitHub Actions: each run lasts about 4 hours and queues the next one; a 15-minute schedule restarts it if the chain breaks.
- The dashboard data is encrypted before it is published, so only someone with the private dashboard link can read it.
