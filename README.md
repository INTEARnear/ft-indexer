# Ft Indexer

This indexer watches for FT events (mint, transfer, burn) and sends them to Redis streams `ft_mint`, `ft_transfer`, and `ft_burn` respectively.

To run it, set `REDIS_URL` environment variable and `cargo run --release`
