# hl-recorder

Records Hyperliquid public market data (trades and best bid/offer) for a set of perps,
continuously, on GitHub Actions. Each run records ~6 hours, uploads hourly gzipped JSONL files
as an artifact, and starts the next run before it ends.
