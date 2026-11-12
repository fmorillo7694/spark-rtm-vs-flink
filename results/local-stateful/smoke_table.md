| arm (SMOKE ONLY) | out records | pipe p50 | p95 | p99 | max | e2e p50 | e2e p99 | window p50 | window p99 | CPU cores | mem GB | evidence |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| flink-ds-bt100-s1-update | 209995 | 53.8 | 99.0 | 104.7 | 120.4 | 63.0 | 112.8 | -4945.0 | -47.0 | 0.3 | 1.9 | ckpt total/completed/failed=3/2/1, EmbeddedRocksDBStateBackend |
| flink-ds-eo-s2 | 131460 | 6.5 | 8.5 | 22.2 | 146.7 | 22696 | 43983 |  |  | 0.6 | 2.9 | ckpt total/completed/failed=3/2/1, EmbeddedRocksDBStateBackend |
| flink-ds-s1-update | 210001 | 6.2 | 7.2 | 8.2 | 115.4 | 13.0 | 15.6 | -4995.0 | -93.0 | 0.5 | 1.9 | ckpt total/completed/failed=3/2/1, EmbeddedRocksDBStateBackend |
| flink-ds-s1-window | 103825 | 8538.5 | 14109 | 14907 | 15179 | 8568.0 | 14940 | 5110.0 | 5183.0 | 0.4 | 1.8 | ckpt total/completed/failed=3/2/1, EmbeddedRocksDBStateBackend |
| flink-ds-s2 | 210011 | 5.6 | 7.3 | 10.5 | 113.7 | 13.2 | 20.6 |  |  | 0.6 | 2.5 | ckpt total/completed/failed=3/2/1, EmbeddedRocksDBStateBackend |
| flink-ds-s3 | 96218 | 6.9 | 8.5 | 11.2 | 197.9 | 13.7 | 19.3 |  |  | 0.5 | 2.3 | ckpt total/completed/failed=3/2/1, EmbeddedRocksDBStateBackend |
| flink-sql-bt100-s1 | 103911 | 8645.0 | 14230 | 15014 | 15310 | 8669.5 | 15038 | 5207.0 | 5314.0 | 0.3 | 3.0 | ckpt total/completed/failed=3/2/1, EmbeddedRocksDBStateBackend |
| flink-sql-bt100-s2 | 210120 | 56.2 | 101.8 | 106.1 | 112.2 | 62.9 | 113.1 |  |  | 0.3 | 3.4 | ckpt total/completed/failed=3/2/1, EmbeddedRocksDBStateBackend |
| flink-sql-s1 | 103778 | 8611.5 | 14233 | 15007 | 15315 | 8636.9 | 15028 | 5217.0 | 5322.0 | 0.2 | 2.8 | ckpt total/completed/failed=3/2/1, EmbeddedRocksDBStateBackend |
| flink-sql-s2 | 209971 | 5.7 | 6.7 | 8.3 | 115.1 | 12.8 | 16.6 |  |  | 0.6 | 3.2 | ckpt total/completed/failed=3/2/1, EmbeddedRocksDBStateBackend |
| spark-microbatch-s1 | 190786 | 240.5 | 367.5 | 464.6 | 614.8 | 243.6 | 467.8 | -4784.0 | 199.0 | 3.9 | 2.6 | async-log-write=0, pipelined=not seen, sync CommitLog(stateUniqueIds)=yes |
| spark-rtm-s1 | 209910 | 32.0 | 55.5 | 734.2 | 1452.3 | 34.8 | 750.8 | -4964.0 | -66.0 | 0.6 | 1.7 | async-log-write=0, pipelined=yes, sync CommitLog(stateUniqueIds)=yes |
| spark-rtm-s1-sw1 | 210001 | 6.7 | 8.3 | 330.0 | 1041.2 | 8.8 | 335.9 | -4996.0 | -95.0 | 0.8 | 1.7 | async-log-write=0, pipelined=yes, sync CommitLog(stateUniqueIds)=yes |
| spark-rtm-s2 | 210015 | 33.0 | 56.6 | 894.6 | 1609.8 | 36.5 | 917.1 |  |  | 0.7 | 1.8 | async-log-write=0, pipelined=yes, sync CommitLog(stateUniqueIds)=yes |
| spark-rtm-s2-sw1 | 209766 | 7.4 | 9.5 | 584.9 | 1294.0 | 10.2 | 593.9 |  |  | 1.1 | 1.6 | async-log-write=0, pipelined=yes, sync CommitLog(stateUniqueIds)=yes |
| spark-rtm-s3 | 96379 | 48.8 | 97.4 | 1026.4 | 1464.9 | 51.5 | 1035.8 |  |  | 0.8 | 2.0 | async-log-write=0, pipelined=yes, sync CommitLog(stateUniqueIds)=yes |
| spark-rtm-s3-sw1 | 96386 | 7.3 | 18.9 | 776.6 | 1195.5 | 16.5 | 802.3 |  |  | 0.9 | 1.7 | async-log-write=0, pipelined=yes, sync CommitLog(stateUniqueIds)=yes |
