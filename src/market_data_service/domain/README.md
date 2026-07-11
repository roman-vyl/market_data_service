# Domain layer

Owns transport- and storage-independent market concepts and rules:

- market-stream identity;
- canonical candle identity;
- timeframe specifications and grid math;
- candle validation;
- duplicate/correction classifications;
- gap interval semantics.

This layer must not import application, adapters, entrypoints, SQLite, Bybit clients, HTTP frameworks, or process lifecycle code.
