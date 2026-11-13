| arm | n | p50 ms | p99 ms | max ms | kept (median) | correctness per run | peak CPU | peak mem GiB |
|---|---:|---:|---:|---:|---:|---|---:|---:|
| flink-ds-bt100-s4-at100k | 1 | 6.0 | 18.2 | 147.0 | 29,399,287 | n/a (replay) | 2.3 | 5.5 |
| flink-ds-bt100-s4 | 3 | 6.4 | 17.9 | 225.8 | 14,583,471 | n/a (replay), n/a (replay), n/a (replay) | 3.0 | 5.5 |
| flink-ds-bt100-s5-at54k | 1 | 130,096.6 | 228,452.6 | 240,613.8 | 3,509,629 | n/a (replay) | 6.1 | 7.0 |
| flink-ds-bt100-s5 | 3 | 5,924.9 | 73,145.0 | 88,248.5 | 4,923,436 | n/a (replay), n/a (replay), n/a (replay) | 6.1 | 6.6 |
| flink-ds-s1-update-kill | 1 | 7.1 | 61,018.1 | 63,636.1 | 17,231,657 | re-emits +2,350,516 | 5.6 | 6.5 |
| flink-ds-s4-at100k | 1 | 6.1 | 27.2 | 606.3 | 29,400,844 | n/a (replay) | 2.9 | 5.5 |
| flink-ds-s4 | 3 | 6.3 | 18.4 | 230.6 | 14,583,000 | n/a (replay), n/a (replay), n/a (replay) | 3.0 | 5.5 |
| flink-ds-s5-at54k | 1 | 126,187.1 | 227,000.5 | 237,103.1 | 3,565,276 | n/a (replay) | 6.1 | 7.0 |
| flink-ds-s5 | 3 | 8,380.7 | 81,482.0 | 92,857.9 | 4,757,668 | n/a (replay), n/a (replay), n/a (replay) | 6.0 | 6.7 |
| spark-mb-s5-at54k | 1 | 69,494.9 | 156,183.3 | 166,855.3 | 8,813,184 | n/a (fewer updates by design) | 6.0 | 11.8 |
| spark-mb-s5 | 3 | 10,528.6 | 17,386.5 | 19,989.9 | 6,456,691 | n/a (fewer updates by design), n/a (fewer updates by design), n/a (fewer updates by design) | 5.1 | 10.3 |
| spark-mb1s-s5-at54k | 1 | 69,024.5 | 157,070.8 | 171,136.2 | 8,947,403 | n/a (fewer updates by design) | 6.0 | 11.6 |
| spark-mb1s-s5 | 3 | 10,316.9 | 17,124.7 | 19,181.7 | 6,585,737 | n/a (fewer updates by design), n/a (fewer updates by design), n/a (fewer updates by design) | 5.0 | 10.3 |
| spark-rtm-s1-kill | 1 | 4,525.7 | 83,226.5 | 84,368.2 | 14,610,775 | re-emits +0 | 6.2 | 9.6 |
| spark-rtm-s4-at100k | 1 | 6.5 | 574.3 | 1,145.7 | 29,408,503 | n/a (replay) | 5.3 | 9.5 |
| spark-rtm-s4 | 3 | 6.6 | 505.5 | 1,137.8 | 14,580,913 | n/a (replay), n/a (replay), n/a (replay) | 5.0 | 9.6 |
