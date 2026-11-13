| arm | n | p50 ms | p99 ms | max ms | kept (median) | correctness per run | peak CPU | peak mem GiB |
|---|---:|---:|---:|---:|---:|---|---:|---:|
| flink-ds-bt100-s2-et-at100k | 1 | 50.6 | 1,297.3 | 2,675.1 | 29,999,666 | exact | 5.6 | 6.3 |
| flink-ds-bt100-s2-et | 3 | 54.5 | 1,730.5 | 2,976.2 | 14,880,069 | exact, exact, exact | 4.3 | 6.0 |
| flink-ds-bt100-s3-lt | 3 | 679.4 | 4,226.1 | 6,501.5 | 11,690,618 | n/a (replay), n/a (replay), n/a (replay) | 5.3 | 6.8 |
| flink-ds-eo-s2 | 1 | 6.9 | 344.4 | 1,952.6 | 14,880,399 | exact + markers | 6.0 | 5.7 |
| flink-ds-s1-update-ckpt300s-720s | 2 | 6.8 | 415.1 | 3,916.0 | 35,712,959 | exact, exact | 5.5 | 7.0 |
| flink-ds-s1-update-ckpt30s | 2 | 6.8 | 1,000.1 | 1,754.3 | 14,880,697 | exact, exact | 5.7 | 6.4 |
| flink-ds-s1-update-ckpt5s | 2 | 6.9 | 380.2 | 776.6 | 14,886,071 | exact, exact | 5.8 | 7.5 |
| flink-ds-s1-update-hot10 | 2 | 6.8 | 1,242.6 | 2,406.7 | 14,880,837 | exact, exact | 6.0 | 6.1 |
| flink-ds-s1-update-kill | 3 | 7.0 | 51,255.8 | 53,722.2 | 16,871,323 | re-emits +1,984,816, re-emits +2,002,518, re-emits +1,991,122 | 6.0 | 6.7 |
| flink-ds-s1-window | 1 | 10,236.2 | 16,410.0 | 18,720.4 | 11,732,322 | n/a (replay) | 5.5 | 6.7 |
| flink-ds-s2-et-at100k | 1 | 6.8 | 1,463.7 | 2,673.6 | 30,001,194 | exact | 6.0 | 6.4 |
| flink-ds-s2-et-ckpt30s | 2 | 6.8 | 1,017.7 | 1,927.2 | 14,881,089 | exact, exact | 5.8 | 5.9 |
| flink-ds-s2-et-ckpt5s | 2 | 6.8 | 320.2 | 621.7 | 14,880,444 | exact, exact | 6.1 | 6.5 |
| flink-ds-s2-et | 3 | 6.7 | 1,774.1 | 2,961.2 | 14,880,754 | exact, exact, exact | 6.0 | 6.0 |
| flink-ds-s3-lt | 3 | 634.6 | 4,034.1 | 6,306.8 | 11,688,907 | n/a (replay), n/a (replay), n/a (replay) | 6.0 | 6.8 |
| flink-p6 | 3 | 6.1 | 26.9 | 224.8 | 10,002,299 | -, -, - | 3.9 | 5.3 |
| flink-sql-bt100-s2-ttl60-at100k | 1 | 48.4 | 2,659.5 | 3,700.7 | 30,001,068 | exact | 5.4 | 7.2 |
| flink-sql-bt100-s2-ttl60 | 3 | 51.1 | 614.2 | 2,025.1 | 14,880,824 | exact, exact, exact | 4.7 | 6.5 |
| flink-sql-eo-s2 | 1 | 6.8 | 667.7 | 2,156.8 | 14,879,987 | exact + markers | 6.1 | 6.4 |
| flink-sql-p6 | 3 | 6.1 | 27.0 | 180.9 | 10,000,453 | -, -, - | 3.6 | 5.6 |
| flink-sql-s2-ttl60-at100k | 1 | 6.7 | 2,467.5 | 3,341.7 | 30,000,158 | exact | 5.8 | 7.1 |
| flink-sql-s2-ttl60 | 3 | 6.8 | 763.5 | 2,124.3 | 14,880,734 | exact, exact, exact | 6.0 | 6.5 |
| pyflink-p6 | 3 | 9.2 | 41.3 | 241.8 | 10,000,885 | -, -, - | 6.1 | 6.5 |
| pyspark-rtm-p6 | 3 | 6.2 | 36.9 | 234.9 | 10,001,143 | -, -, - | 6.0 | 8.0 |
| spark-mb-s1 | 1 | 3,640.1 | 5,572.0 | 6,596.0 | 13,964,888 | n/a (fewer updates by design) | 5.2 | 9.0 |
| spark-rtm-s1-ckpt300s-720s | 2 | 36.6 | 17,306.5 | 20,339.7 | 35,712,766 | exact, exact | 6.0 | 10.2 |
| spark-rtm-s1-ckpt30s | 2 | 40.7 | 4,668.3 | 5,385.3 | 14,766,019 | exact, exact | 5.9 | 9.1 |
| spark-rtm-s1-ckpt5s | 2 | 251.1 | 2,875.6 | 3,490.1 | 14,943,861 | exact, exact | 6.2 | 9.0 |
| spark-rtm-s1-hot10 | 2 | 37.4 | 6,288.9 | 7,842.3 | 14,897,743 | exact, exact | 6.0 | 9.2 |
| spark-rtm-s1-kill | 3 | 4,806.2 | 81,458.0 | 83,549.8 | 14,636,466 | re-emits +0, re-emits +107,990, re-emits +80,934 | 6.3 | 9.8 |
| spark-rtm-s2-ckpt30s | 2 | 39.9 | 4,349.9 | 5,097.3 | 14,817,650 | exact, exact | 5.8 | 8.8 |
| spark-rtm-s2-ckpt5s | 2 | 74.0 | 2,571.9 | 3,413.6 | 14,905,621 | exact, exact | 5.2 | 8.9 |
| spark-rtm-s2 | 1 | 38.5 | 7,921.8 | 9,506.3 | 14,880,787 | exact | 6.0 | 9.2 |
| spark-rtm-s3-lt | 3 | 97,167.9 | 161,374.0 | 166,535.7 | 7,100,924 | n/a (replay), n/a (replay), n/a (replay) | 6.0 | 9.8 |
| spark-rtm-scala-p6 | 1 | 6.2 | 33.3 | 236.5 | 9,999,952 | - | 3.6 | 8.0 |
| spark-rtm-syncckpt-p6 | 3 | 6.2 | 605.9 | 1,263.6 | 9,999,373 | -, -, - | 5.0 | 8.1 |
| spark-rtm-vt-s1 | 3 | 13.0 | 4,045.8 | 5,240.1 | 15,001,577 | exact, exact, exact | 6.0 | 9.3 |
| spark-rtm-vt-s2 | 3 | 12.7 | 4,268.3 | 5,336.0 | 14,880,587 | exact, exact, exact | 6.0 | 9.1 |
| spark-rtm-vt-s3-lt | 3 | 79,382.4 | 103,594.0 | 106,433.0 | 9,227,062 | n/a (replay), n/a (replay), n/a (replay) | 6.1 | 9.8 |
