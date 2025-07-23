Açıklaması şöyle  
https://medium.com/@ThreadSafeDiaries/kafka-was-overkill-this-lightweight-pattern-handled-more-traffic-40455f09c4fd


| **Feature**              | **Kafka**         | **Redis Streams**         |
|--------------------------|-------------------|---------------------------|
| Durability               | High              | Medium (with AOF)         |
| Replayability            | Yes               | Yes (via IDs)             |
| Consumer Groups          | Yes               | Yes                       |
| Setup Complexity         | High              | Low                       |
| Latency (p99)\*          | ~25ms             | ~3ms                      |
| Throughput (sustained)\* | 18K req/sec       | 42K req/sec               |
