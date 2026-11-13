### Per arm (geomean over runs; window = arithmetic mean, it can be negative)

| workload | arm | runs | pipe p50 | pipe p99 | pipe max | e2e p50 | e2e p99 | window p50 | window p99 | p99 near boundary | p99 elsewhere | CPU cores | mem GB | count invariant |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| s1-update | Flink DataStream, buffer timeout 100 ms (default) | 1 | 54.9 | 2213.5 | 3492.2 | 139.9 | 2722.0 | -4913.0 | -49.0 | 3200.0 | 720.0 | 3.0 | 6.1 | PASS |
| s1-update | Flink DataStream, buffer timeout 0 ms | 2 | 6.9 | 2214.1 | 3595.8 | 64.8 | 2709.5 | -4952.0 | 609.0 | 3100.0 | 651.8 | 4.7 | 6.1 | PASS |
| s1 | Spark RTM, shuffle flush 1 ms | 1 | 65691 | 104229 | 110008 | 66388 | 104260 | 61049 | 100874 | 101400 | 104300 | 5.4 | 9.1 | PASS |
| s1 | Spark RTM, shuffle flush 50 ms (default) | 2 | 17814 | 27162 | 28232 | 19605 | 27686 | 13002 | 25124 | 27087 | 27073 | 5.5 | 8.7 | PASS |
| s1 | Flink SQL, buffer timeout 100 ms (default) | 1 | 12332 | 19092 | 22913 | 14107 | 21637 | 8368.0 | 11821 | 13900 | 19100 | 3.2 | 6.5 | n/a |
| s1 | Flink SQL, buffer timeout 0 ms | 1 | 12449 | 19260 | 22562 | 14385 | 22046 | 8457.0 | 11719 | 18900 | 19300 | 3.2 | 6.5 | n/a |
| s2 | Spark RTM, shuffle flush 1 ms | 1 | 68465 | 106175 | 108495 | 68962 | 106964 | n/a | n/a | 97000 | 106600 | 5.5 | 8.5 | PASS |
| s2 | Spark RTM, shuffle flush 50 ms (default) | 1 | 11305 | 22254 | 24318 | 11925 | 22353 | n/a | n/a | 23800 | 21900 | 5.5 | 8.5 | PASS |
| s2 | Flink DataStream, buffer timeout 100 ms (default) | 1 | 47.9 | 1290.7 | 5472.2 | 74.5 | 1632.7 | n/a | n/a | 2200.0 | 100.0 | 2.6 | 5.5 | FAIL |
| s2 | Flink DataStream, buffer timeout 0 ms | 1 | 6.7 | 1377.1 | 5650.4 | 22.2 | 1812.4 | n/a | n/a | 2200.0 | 24.0 | 4.5 | 5.4 | FAIL |
| s2 | Flink SQL, buffer timeout 100 ms (default) | 1 | 47.6 | 2252.8 | 5982.9 | 81.3 | 2434.6 | n/a | n/a | 2700.0 | 260.0 | 3.3 | 6.2 | FAIL |
| s2 | Flink SQL, buffer timeout 0 ms | 1 | 6.9 | 2472.2 | 6304.4 | 23.7 | 2748.0 | n/a | n/a | 3000.0 | 730.0 | 5.0 | 6.2 | FAIL |
| s3 | Spark RTM, shuffle flush 1 ms | 1 | 179651 | 264485 | 269503 | 179691 | 264530 | n/a | n/a | n/a | 264400 | 5.6 | 8.4 | n/a |
| s3 | Spark RTM, shuffle flush 50 ms (default) | 1 | 175194 | 260710 | 265678 | 175225 | 260741 | n/a | n/a | n/a | 260700 | 5.6 | 8.3 | n/a |
| s3 | Flink DataStream, buffer timeout 100 ms (default) | 1 | 263.0 | 4974.6 | 6728.1 | 290.7 | 5000.3 | n/a | n/a | 4500.0 | 5000.0 | 5.5 | 6.6 | n/a |
| s3 | Flink DataStream, buffer timeout 0 ms | 1 | 626.8 | 5145.5 | 7737.3 | 657.8 | 5183.0 | n/a | n/a | 4600.0 | 5100.0 | 5.5 | 6.5 | n/a |

### Default vs tuned flush, per workload (pipe p50 / p99 ms, geomean, n = runs)


**S1** (Flink DataStream arm: `s1-update`; Flink SQL S1 = TUMBLE, final-only)

| engine | default flush | tuned flush |
|---|---|---|
| Spark RTM | 17814 / 27162 (n=2) | 65691 / 104229 (n=1) |
| Flink DataStream | 54.9 / 2213.5 (n=1) | 6.9 / 2214.1 (n=2) |
| Flink SQL | 12332 / 19092 (n=1) | 12449 / 19260 (n=1) |

**S2** (Flink DataStream arm: `s2`; Flink SQL S1 = TUMBLE, final-only)

| engine | default flush | tuned flush |
|---|---|---|
| Spark RTM | 11305 / 22254 (n=1) | 68465 / 106175 (n=1) |
| Flink DataStream | 47.9 / 1290.7 (n=1) | 6.7 / 1377.1 (n=1) |
| Flink SQL | 47.6 / 2252.8 (n=1) | 6.9 / 2472.2 (n=1) |

**S3** (Flink DataStream arm: `s3`; Flink SQL S1 = TUMBLE, final-only)

| engine | default flush | tuned flush |
|---|---|---|
| Spark RTM | 175194 / 260710 (n=1) | 179651 / 264485 (n=1) |
| Flink DataStream | 263.0 / 4974.6 (n=1) | 626.8 / 5145.5 (n=1) |

### Per run

| arm | workload | run | records | eps | pipe p50 / p99 / max | e2e p50 / p99 | window p50 / p99 | near p99 / max | elsewhere p99 / max | CPU mean / peak | mem GB mean / peak | evidence | invariant |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| flink-ds-bt100 | s1-update | r1 | 29994792 | 99983 | 54.9 / 2213.5 / 3492.2 | 139.9 / 2722.0 | -4913.0 / -49.0 | 3200.0 / 3400.0 | 720.0 / 2200.0 | 3.0 / 5.5 | 6.1 / 7.0 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | PASS: updates 38000750 vs input 38000750 (diff 0) |
| flink-ds | s1-update | r0 | 30000889 | 100003 | 6.9 / 2222.9 / 3579.9 | 41.1 / 2708.4 | -4912.0 / -93.0 | 3100.0 / 3500.0 | 590.0 / 1910.0 | 4.7 / 6.0 | 6.1 / 6.8 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | PASS: updates 38000875 vs input 38000875 (diff 0) |
| flink-ds | s1-update | r1 | 29986339 | 99954 | 6.9 / 2205.3 / 3611.8 | 102.3 / 2710.6 | -4992.0 / 1311.0 | 3100.0 / 3600.0 | 720.0 / 2200.0 | 4.7 / 6.0 | 6.2 / 7.1 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | PASS: updates 38000625 vs input 38000625 (diff 0) |
| spark-rtm-sw1 | s1 | r1 | 22475095 | 74917 | 65691 / 104229 / 110008 | 66388 / 104260 | 61049 / 100874 | 101400 / 104000 | 104300 / 110000 | 5.4 / 6.0 | 9.1 / 10.5 | async-log-write=0, pipelined=yes | PASS: updates 38000750 vs input 38000750 (diff 0) |
| spark-rtm | s1 | r0 | 29778086 | 99260 | 14741 / 25956 / 26782 | 16789 / 26016 | 9427.0 / 23976 | 25300 / 26000 | 25900 / 26700 | 5.5 / 6.0 | 8.7 / 9.8 | async-log-write=0, pipelined=yes | PASS: updates 38000750 vs input 38000750 (diff 0) |
| spark-rtm | s1 | r1 | 29283364 | 97611 | 21528 / 28424 / 29761 | 22894 / 29463 | 16576 / 26273 | 29000 / 29700 | 28300 / 29700 | 5.5 / 6.0 | 8.8 / 10.2 | async-log-write=0, pipelined=yes | PASS: updates 38000875 vs input 38000875 (diff 0) |
| flink-sql-bt100 | s1 | r1 | 18964188 | 63214 | 12332 / 19092 / 22913 | 14107 / 21637 | 8368.0 / 11821 | 13900 / 14700 | 19100 / 22900 | 3.2 / 4.9 | 6.5 / 7.3 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | n/a: out 23765022, input 38000875 (count not checkable) |
| flink-sql | s1 | r1 | 18964260 | 63214 | 12449 / 19260 / 22562 | 14385 / 22046 | 8457.0 / 11719 | 18900 / 19900 | 19300 / 22500 | 3.2 / 5.2 | 6.5 / 7.6 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | n/a: out 23778905, input 38000875 (count not checkable) |
| spark-rtm-sw1 | s2 | r1 | 23361285 | 77871 | 68465 / 106175 / 108495 | 68962 / 106964 | n/a / n/a | 97000 / 99700 | 106600 / 108400 | 5.5 / 6.0 | 8.5 / 9.8 | async-log-write=0, pipelined=yes | PASS: out 38000500 vs unique 38000500 (diff 0) |
| spark-rtm | s2 | r1 | 28885310 | 96284 | 11305 / 22254 / 24318 | 11925 / 22353 | n/a / n/a | 23800 / 24200 | 21900 / 24300 | 5.5 / 6.0 | 8.5 / 9.7 | async-log-write=0, pipelined=yes | PASS: out 38000750 vs unique 38000750 (diff 0) |
| flink-ds-bt100 | s2 | r1 | 29998571 | 99995 | 47.9 / 1290.7 / 5472.2 | 74.5 / 1632.7 | n/a / n/a | 2200.0 / 2500.0 | 100.0 / 5400.0 | 2.6 / 5.3 | 5.5 / 5.8 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | FAIL: out 38001319 vs unique 38001000 (diff 319) |
| flink-ds | s2 | r1 | 30001935 | 100006 | 6.7 / 1377.1 / 5650.4 | 22.2 / 1812.4 | n/a / n/a | 2200.0 / 2700.0 | 24.0 / 5600.0 | 4.5 / 5.9 | 5.4 / 5.8 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | FAIL: out 38001348 vs unique 38000750 (diff 598) |
| flink-sql-bt100 | s2 | r1 | 30003989 | 100013 | 47.6 / 2252.8 / 5982.9 | 81.3 / 2434.6 | n/a / n/a | 2700.0 / 3000.0 | 260.0 / 5900.0 | 3.3 / 5.4 | 6.2 / 6.8 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | FAIL: out 38005650 vs unique 38000875 (diff 4775) |
| flink-sql | s2 | r1 | 30010622 | 100035 | 6.9 / 2472.2 / 6304.4 | 23.7 / 2748.0 | n/a / n/a | 3000.0 / 5300.0 | 730.0 / 6300.0 | 5.0 / 6.0 | 6.2 / 6.8 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | FAIL: out 38010313 vs unique 38000500 (diff 9813) |
| spark-rtm-sw1 | s3 | r1 | 6910217 | 23034 | 179651 / 264485 / 269503 | 179691 / 264530 | n/a / n/a | n/a / n/a | 264400 / 269500 | 5.6 / 6.0 | 8.4 / 9.6 | async-log-write=0, pipelined=yes | n/a: out 13891547, input 38000875 (count not checkable) |
| spark-rtm | s3 | r1 | 7101440 | 23672 | 175194 / 260710 / 265678 | 175225 / 260741 | n/a / n/a | n/a / n/a | 260700 / 265600 | 5.6 / 6.0 | 8.3 / 9.4 | async-log-write=0, pipelined=yes | n/a: out 15327210, input 38000875 (count not checkable) |
| flink-ds-bt100 | s3 | r1 | 19016261 | 63388 | 263.0 / 4974.6 / 6728.1 | 290.7 / 5000.3 | n/a / n/a | 4500.0 / 5400.0 | 5000.0 / 6700.0 | 5.5 / 6.0 | 6.6 / 7.6 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | n/a: out 24184725, input 38000750 (count not checkable) |
| flink-ds | s3 | r1 | 19032520 | 63442 | 626.8 / 5145.5 / 7737.3 | 657.8 / 5183.0 | n/a / n/a | 4600.0 / 5700.0 | 5100.0 / 7700.0 | 5.5 / 6.0 | 6.5 / 7.5 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | n/a: out 24165620, input 38000750 (count not checkable) |
