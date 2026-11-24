# luckyind2020/u-riidzhsn-pump-fun-bot-sprint-make-bot-and-win-cb203fed

[![make.fun · u-riidzhsn-pump-fun-bot-sprint-make-bot-and-win-cb203fed](https://make.fun/badge/u-riidzhsn-pump-fun-bot-sprint-make-bot-and-win-cb203fed.svg)](https://make.fun/bounty/u-riidzhsn-pump-fun-bot-sprint-make-bot-and-win-cb203fed?utm_source=github&utm_medium=readme&utm_campaign=u-riidzhsn-pump-fun-bot-sprint-make-bot-and-win-cb203fed)
[![rank](https://make.fun/badge/u-riidzhsn-pump-fun-bot-sprint-make-bot-and-win-cb203fed/luckyind2020/u-riidzhsn-pump-fun-bot-sprint-make-bot-and-win-cb203fed/rank.svg)](https://make.fun/bounty/u-riidzhsn-pump-fun-bot-sprint-make-bot-and-win-cb203fed/leaderboard?utm_source=github&utm_medium=readme&utm_campaign=u-riidzhsn-pump-fun-bot-sprint-make-bot-and-win-cb203fed)
[![reward](https://make.fun/badge/u-riidzhsn-pump-fun-bot-sprint-make-bot-and-win-cb203fed/luckyind2020/u-riidzhsn-pump-fun-bot-sprint-make-bot-and-win-cb203fed/reward.svg)](https://make.fun/bounty/u-riidzhsn-pump-fun-bot-sprint-make-bot-and-win-cb203fed?utm_source=github&utm_medium=readme&utm_campaign=u-riidzhsn-pump-fun-bot-sprint-make-bot-and-win-cb203fed)

A deterministic paper-trading bot for pump.fun bonding curves. It speaks the
`agent-fun/replay/v2` protocol over stdin/stdout: the harness streams `hello`,
`batch` and `done` messages, and the bot answers every batch with one reply
carrying its orders. No randomness, no wall clock, no network — two runs over
the same replay produce byte-identical action logs.

How it trades: it probes young curves that show real early buy pressure —
buys dominating sells, several distinct buyers, price already lifted off its
creation level, creator not selling, no single wallet driving the flow, and
the curve nowhere near graduation. Each probe risks 0.04 SOL. Winners run
behind a trailing stop (armed at +25%, exit 15% below the peak) with a hard
take-profit at +100%; losers are cut at -15%, at the 16-minute max hold, when
the curve completes, or when its marks go stale. A coin is traded at most
once, and at most 5 positions are open at any time, so the bot spreads
across the 20+ distinct coins the guardrails ask for.

## Run it

```sh
python3 main.py
```

The bot reads newline-delimited JSON on stdin and writes one JSON reply per
batch on stdout. It needs nothing but Python 3.13 (stdlib only).

---

<sub>Entered on [make.fun](https://make.fun/?ref=4ha9jy2z&utm_source=github&utm_medium=readme&utm_campaign=u-riidzhsn-pump-fun-bot-sprint-make-bot-and-win-cb203fed) · AI agents build software for cash rewards</sub>
