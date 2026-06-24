# Shared event contract

Both engines consume the **same** input events and emit the **same** output shape,
so latency/CPU/memory/cost are measured on an identical workload.

## Input event (topic: `input-events`, key = `user_id` as UTF-8 string)

JSON, one event per Kafka record:

```json
{
  "event_id": 123456,
  "user_id": 42891,
  "event_type": "purchase",
  "country": "es",
  "amount": 219.99,
  "ts": 1750684800.123456
}
```

| field        | type   | notes                                                              |
|--------------|--------|--------------------------------------------------------------------|
| `event_id`   | long   | monotonic sequence from the producer                               |
| `user_id`    | long   | 1..100000, also the Kafka partition key                            |
| `event_type` | string | one of click, view, purchase, add_to_cart, logout, login          |
| `country`    | string | lowercase 2-letter (US, ES, DE, FR, BR, IN, JP, GB) emitted lower  |
| `amount`     | double | 0.0 .. 500.0                                                       |
| `ts`         | double | **epoch seconds (float)** stamped at produce time — latency anchor |

## Stateless transform (identical in Spark and Flink)

1. **filter**: keep only `event_type ∈ {purchase, add_to_cart}` AND `amount > 0`
2. `amount_with_tax = amount * 1.21`
3. `country = upper(country)`
4. `out_ts = <engine processing-time at write>` (epoch seconds, float)
5. carry `ts` (original produce time) through unchanged

## Output event (topic: `output-events`)

```json
{
  "event_id": 123456,
  "user_id": 42891,
  "event_type": "purchase",
  "country": "ES",
  "amount": 219.99,
  "amount_with_tax": 266.19,
  "ts": 1750684800.123456,
  "out_ts": 1750684800.148231
}
```

## Latency definitions

- **pipeline latency** = `out_ts − ts`  (Kafka-in → transform → Kafka-out, engine work)
- **end-to-end latency** = `consume_wallclock − ts`  (adds output-topic read by consumer)

Producer, consumer, and the engine containers share the host wall clock, so the
subtraction is meaningful. Reported as p50/p95/p99/max over a steady-state window
(first `WARMUP_SECONDS` dropped).
