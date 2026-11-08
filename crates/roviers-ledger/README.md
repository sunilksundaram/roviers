# roviers-ledger

Append-only memory-mapped commit log, radix topic trie, and deduplication engine for **Roviers**.

## Features
- Low-latency disk-backed memory-mapped (mmap) commit log segments
- Radix topic trie supporting wildcard pattern matching (`#`, `+`)
- In-memory circular sliding-window deduplication ring buffer
- CRC32 verification and optional LZ4 compression

