# Quotex Signal Bot v17

Multi-source FX signal bot for Telegram. Pure stdlib, low RAM (~30-80 MB).

## Features
- 11 modular strategies (RSI, Stoch, MACD cross, BB, EMA cross, patterns,
  momentum, MACD+RSI confluence, MACD+BB breakout, MACD divergence,
  MACD histogram scalping)
- Yahoo primary + Stooq fallback (no synthetic candles)
- Confidence filter with ADX/ATR adjustment
- Signal journal: `data/signals.jsonl`
- Rotating logs: `logs/bot.log`

## Local run
```bash
python3 bot.py