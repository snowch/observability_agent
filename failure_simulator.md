| Method Name | Action | Available Options |
| :--- | :--- | :--- |
| `adFailure` | Fail ad service | `off`, `on` |
| `adHighCpu` | Triggers high cpu load in the ad service | `off`, `on` |
| `adManualGc` | Triggers full manual garbage collections in the ad service | `off`, `on` |
| `cartFailure` | Fail cart service | `off`, `on` |
| `emailMemoryLeak` | Memory leak in the email service | `off`, `1x`, `10x`, `100x`, `1000x`, `10000x` |
| `failedReadinessProbe` | Readiness probe failure for cart service | `off`, `on` |
| `imageSlowLoad` | Slow loading images in the frontend | `off`, `5sec`, `10sec` |
| `kafkaQueueProblems` | Overloads Kafka queue + consumer delay | `off`, `on` |
| `llmInaccurateResponse` | LLM returns an inaccurate product summary (ID L9ECAV7KIM) | `off`, `on` |
| `llmRateLimitError` | LLM intermittently returns a rate limit error | `off`, `on` |
| `loadGeneratorFloodHomepage` | Flood the frontend with a large amount of requests | `off`, `on` |
| `paymentFailure` | Fail payment service charge requests n% | `off`, `10%`, `25%`, `50%`, `75%`, `90%`, `100%` |
| `paymentUnreachable` | Payment service is unavailable | `off`, `on` |
| `productCatalogFailure` | Fail product catalog service on a specific product | `off`, `on` |
| `recommendationCacheFailure` | Fail recommendation service cache | `off`, `on` |
