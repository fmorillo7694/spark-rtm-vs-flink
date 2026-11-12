### Per arm (geomean over runs; window = arithmetic mean, it can be negative)

| workload | arm | runs | pipe p50 | pipe p99 | pipe max | e2e p50 | e2e p99 | window p50 | window p99 | p99 near boundary | p99 elsewhere | CPU cores | mem GB | count invariant |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| s1-update | Flink DataStream, buffer timeout 100 ms (default) | 3 | 55.5 | 1472.8 | 2642.0 | 80.1 | 1498.7 | -4903.0 | -10.3 | 2295.8 | 249.9 | 2.0 | 5.7 | PASS |
| s1-update | Flink DataStream, buffer timeout 0 ms | 3 | 6.9 | 1420.5 | 2745.1 | 27.5 | 1447.2 | -4992.3 | 136.7 | 2298.5 | 206.6 | 3.4 | 5.7 | PASS |
| s1-window | Flink DataStream, buffer timeout 0 ms | 2 | 10077 | 15732 | 17560 | 11294 | 17639 | 5639.5 | 6995.5 | 13191 | 15700 | 3.1 | 6.0 | n/a |
| s1 | Spark RTM, shuffle flush 1 ms | 3 | 243.8 | 21364 | 23284 | 299.6 | 21396 | -2162.3 | 18210 | 22525 | 20715 | 5.6 | 8.4 | PASS |
| s1 | Spark RTM, shuffle flush 50 ms (default) | 3 | 39.3 | 6838.8 | 7963.6 | 65.7 | 6869.5 | -4485.7 | 3688.7 | 5884.3 | 6796.6 | 3.7 | 8.2 | PASS |
| s1 | Spark micro-batch | 2 | 3368.3 | 5220.2 | 6142.6 | 3764.4 | 5791.9 | -1779.0 | 4367.0 | n/a | n/a | 3.3 | 8.4 | n/a |
| s1 | Flink SQL, buffer timeout 100 ms (default) | 3 | 11234 | 16913 | 18476 | 12376 | 18750 | 6822.3 | 7952.0 | 13027 | 16900 | 2.1 | 6.1 | n/a |
| s1 | Flink SQL, buffer timeout 0 ms | 3 | 11229 | 16944 | 18318 | 12365 | 18774 | 6854.0 | 7905.0 | 12923 | 16900 | 2.1 | 6.1 | n/a |
| s2 | Spark RTM, shuffle flush 1 ms | 3 | 329.2 | 23208 | 25302 | 552.5 | 23260 | n/a | n/a | 17547 | 23056 | 4.7 | 8.2 | PASS |
| s2 | Spark RTM, shuffle flush 50 ms (default) | 3 | 38.3 | 7907.6 | 9317.5 | 64.0 | 7938.9 | n/a | n/a | 5666.5 | 7931.5 | 3.4 | 8.2 | PASS |
| s2 | Flink DataStream, buffer timeout 100 ms (default) | 3 | 51.8 | 259.3 | 1546.6 | 76.2 | 287.2 | n/a | n/a | 1133.1 | 100.0 | 1.6 | 5.3 | PASS |
| s2 | Flink DataStream, EXACTLY_ONCE sink | 2 | 6.8 | 321.8 | 1728.1 | 43906 | 61818 | n/a | n/a | 1319.8 | 21.5 | 3.3 | 5.4 | PASS |
| s2 | Flink DataStream, buffer timeout 0 ms | 3 | 6.8 | 321.2 | 1756.4 | 26.7 | 343.7 | n/a | n/a | 1269.7 | 15.7 | 3.2 | 5.3 | PASS |
| s2 | Flink SQL, buffer timeout 100 ms (default) | 3 | 50.9 | 565.5 | 1998.6 | 74.4 | 586.3 | n/a | n/a | 1486.3 | 100.0 | 2.0 | 5.8 | PASS |
| s2 | Flink SQL, EXACTLY_ONCE sink | 2 | 6.9 | 705.1 | 3230.5 | 44667 | 62186 | n/a | n/a | 1708.6 | 26.0 | 3.7 | 5.9 | PASS |
| s2 | Flink SQL, buffer timeout 0 ms | 3 | 6.9 | 689.2 | 2795.3 | 27.0 | 712.5 | n/a | n/a | 1679.7 | 19.9 | 3.6 | 5.8 | FAIL |
| s3 | Spark RTM, shuffle flush 1 ms | 3 | 130082 | 183945 | 193082 | 130113 | 184005 | n/a | n/a | 152781 | 184000 | 5.6 | 8.6 | n/a |
| s3 | Spark RTM, shuffle flush 50 ms (default) | 3 | 109292 | 181564 | 185289 | 109327 | 181606 | n/a | n/a | 121616 | 181594 | 5.5 | 8.5 | n/a |
| s3 | Flink DataStream, buffer timeout 100 ms (default) | 3 | 69.2 | 4402.9 | 6252.0 | 94.1 | 4451.3 | n/a | n/a | 4500.0 | 4300.0 | 3.4 | 6.0 | n/a |
| s3 | Flink DataStream, buffer timeout 0 ms | 3 | 8.5 | 4441.3 | 5801.7 | 34.4 | 4481.6 | n/a | n/a | 4464.9 | 4331.6 | 4.9 | 6.0 | n/a |

### Default vs tuned flush, per workload (pipe p50 / p99 ms, geomean, n = runs)


**S1** (Flink DataStream arm: `s1-update`; Flink SQL S1 = TUMBLE, final-only)

| engine | default flush | tuned flush |
|---|---|---|
| Spark RTM | 39.3 / 6838.8 (n=3) | 243.8 / 21364 (n=3) |
| Flink DataStream | 55.5 / 1472.8 (n=3) | 6.9 / 1420.5 (n=3) |
| Flink SQL | 11234 / 16913 (n=3) | 11229 / 16944 (n=3) |

**S2** (Flink DataStream arm: `s2`; Flink SQL S1 = TUMBLE, final-only)

| engine | default flush | tuned flush |
|---|---|---|
| Spark RTM | 38.3 / 7907.6 (n=3) | 329.2 / 23208 (n=3) |
| Flink DataStream | 51.8 / 259.3 (n=3) | 6.8 / 321.2 (n=3) |
| Flink SQL | 50.9 / 565.5 (n=3) | 6.9 / 689.2 (n=3) |

**S3** (Flink DataStream arm: `s3`; Flink SQL S1 = TUMBLE, final-only)

| engine | default flush | tuned flush |
|---|---|---|
| Spark RTM | 109292 / 181564 (n=3) | 130082 / 183945 (n=3) |
| Flink DataStream | 69.2 / 4402.9 (n=3) | 8.5 / 4441.3 (n=3) |

### Per run

| arm | workload | run | records | eps | pipe p50 / p99 / max | e2e p50 / p99 | window p50 / p99 | near p99 / max | elsewhere p99 / max | CPU mean / peak | mem GB mean / peak | evidence | invariant |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| flink-ds-bt100 | s1-update | r1 | 14879972 | 49600 | 55.3 / 1378.8 / 2510.7 | 79.3 / 1402.3 | -4943.0 / 52.0 | 2200.0 / 2500.0 | 260.0 / 470.0 | 2.0 / 3.6 | 5.6 / 6.1 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | PASS: updates 18848496 vs input 18848496 (diff 0) |
| flink-ds-bt100 | s1-update | r2 | 14881516 | 49605 | 56.1 / 1728.0 / 2908.2 | 81.7 / 1755.7 | -4825.0 / -42.0 | 2500.0 / 2900.0 | 240.0 / 520.0 | 2.1 / 4.5 | 5.7 / 6.2 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | PASS: updates 18848496 vs input 18848496 (diff 0) |
| flink-ds-bt100 | s1-update | r3 | 14881131 | 49604 | 55.2 / 1341.0 / 2525.8 | 79.3 / 1367.1 | -4941.0 / -41.0 | 2200.0 / 2500.0 | 250.0 / 510.0 | 1.9 / 4.2 | 5.7 / 6.2 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | PASS: updates 18848496 vs input 18848496 (diff 0) |
| flink-ds | s1-update | r1 | 14880764 | 49602 | 6.8 / 1499.9 / 2901.7 | 27.5 / 1525.1 | -4992.0 / -92.0 | 2400.0 / 2900.0 | 210.0 / 470.0 | 3.4 / 5.2 | 5.7 / 6.3 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | PASS: updates 18848496 vs input 18848496 (diff 0) |
| flink-ds | s1-update | r2 | 14880661 | 49602 | 6.9 / 1360.3 / 2582.5 | 27.4 / 1390.3 | -4992.0 / -22.0 | 2200.0 / 2500.0 | 210.0 / 470.0 | 3.6 / 6.0 | 5.7 / 6.1 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | PASS: updates 18848372 vs input 18848372 (diff 0) |
| flink-ds | s1-update | r3 | 14880386 | 49601 | 6.9 / 1404.9 / 2760.4 | 27.4 / 1429.6 | -4993.0 / 524.0 | 2300.0 / 2700.0 | 200.0 / 440.0 | 3.4 / 6.0 | 5.6 / 6.1 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | PASS: updates 18848434 vs input 18848434 (diff 0) |
| flink-ds | s1-window | r1 | 11732167 | 39107 | 10113 / 15800 / 17988 | 11307 / 17658 | 5658.0 / 7260.0 | 12700 / 13300 | 15800 / 17900 | 3.1 / 5.1 | 6.0 / 6.7 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | n/a: out 14459757, input 18848496 (count not checkable) |
| flink-ds | s1-window | r2 | 11732150 | 39107 | 10041 / 15665 / 17141 | 11280 / 17620 | 5621.0 / 6731.0 | 13700 / 14100 | 15600 / 17100 | 3.1 / 5.5 | 5.9 / 6.7 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | n/a: out 14456325, input 18848434 (count not checkable) |
| spark-rtm-sw1 | s1 | r1 | 15562629 | 51875 | 112.6 / 21455 / 23213 | 143.4 / 21488 | -2350.0 / 16978 | 22500 / 22800 | 20400 / 23200 | 5.6 / 6.1 | 8.4 / 9.4 | async-log-write=0, pipelined=yes | PASS: updates 18848372 vs input 18848372 (diff 0) |
| spark-rtm-sw1 | s1 | r2 | 15639284 | 52131 | 66.3 / 19067 / 21072 | 93.4 / 19098 | -2579.0 / 16389 | 20400 / 21000 | 18700 / 20600 | 5.6 / 6.2 | 8.4 / 9.5 | async-log-write=0, pipelined=yes | PASS: updates 18848372 vs input 18848372 (diff 0) |
| spark-rtm-sw1 | s1 | r3 | 15640710 | 52136 | 1942.4 / 23835 / 25806 | 2008.7 / 23868 | -1558.0 / 21264 | 24900 / 25800 | 23300 / 25700 | 5.6 / 6.0 | 8.4 / 9.5 | async-log-write=0, pipelined=yes | PASS: updates 18848434 vs input 18848434 (diff 0) |
| spark-rtm | s1 | r1 | 14879743 | 49599 | 39.0 / 6614.3 / 7418.7 | 65.6 / 6643.6 | -4549.0 / 2821.0 | 4700.0 / 5100.0 | 6600.0 / 7400.0 | 3.6 / 6.0 | 8.2 / 9.2 | async-log-write=0, pipelined=yes | PASS: updates 18848496 vs input 18848496 (diff 0) |
| spark-rtm | s1 | r2 | 15193328 | 50644 | 40.2 / 7200.6 / 8983.3 | 66.5 / 7232.9 | -4411.0 / 4391.0 | 8500.0 / 8900.0 | 7100.0 / 8600.0 | 3.7 / 6.0 | 8.2 / 9.3 | async-log-write=0, pipelined=yes | PASS: updates 18848434 vs input 18848434 (diff 0) |
| spark-rtm | s1 | r3 | 14991570 | 49972 | 38.8 / 6715.5 / 7578.2 | 64.9 / 6746.3 | -4497.0 / 3854.0 | 5100.0 / 5800.0 | 6700.0 / 7500.0 | 3.9 / 6.0 | 8.3 / 9.2 | async-log-write=0, pipelined=yes | PASS: updates 18848434 vs input 18848434 (diff 0) |
| spark-mb | s1 | r1 | 14091774 | 46973 | 3256.8 / 5103.8 / 5906.4 | 3637.8 / 5635.4 | -1867.0 / 4179.0 | n/a | n/a | 3.2 / 4.4 | 8.4 / 9.0 | async-log-write=0, pipelined=no | n/a: out 17739118, input 18848496 (count not checkable) |
| spark-mb | s1 | r2 | 14025259 | 46751 | 3483.6 / 5339.3 / 6388.3 | 3895.4 / 5952.8 | -1691.0 / 4555.0 | n/a | n/a | 3.3 / 5.1 | 8.4 / 9.0 | async-log-write=0, pipelined=no | n/a: out 17702301, input 18848496 (count not checkable) |
| flink-sql-bt100 | s1 | r1 | 11682551 | 38942 | 11235 / 16911 / 18554 | 12411 / 18790 | 6822.0 / 7961.0 | 16900 / 17500 | 16900 / 18500 | 2.1 / 4.2 | 6.0 / 6.7 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | n/a: out 14635099, input 18848496 (count not checkable) |
| flink-sql-bt100 | s1 | r2 | 11659191 | 38864 | 11250 / 16936 / 18519 | 12364 / 18732 | 6840.0 / 8016.0 | 12000 / 12500 | 16900 / 18500 | 2.0 / 4.3 | 6.1 / 6.6 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | n/a: out 14491229, input 18848434 (count not checkable) |
| flink-sql-bt100 | s1 | r3 | 11730988 | 39103 | 11216 / 16891 / 18355 | 12352 / 18729 | 6805.0 / 7879.0 | 10900 / 11400 | 16900 / 18300 | 2.0 / 4.7 | 6.0 / 6.7 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | n/a: out 14650663, input 18848496 (count not checkable) |
| flink-sql | s1 | r1 | 11731254 | 39104 | 11252 / 16918 / 18390 | 12369 / 18733 | 6849.0 / 7837.0 | 10900 / 11400 | 16900 / 18300 | 2.1 / 4.3 | 6.1 / 6.7 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | n/a: out 14681242, input 18848496 (count not checkable) |
| flink-sql | s1 | r2 | 11731611 | 39105 | 11158 / 16961 / 18092 | 12315 / 18804 | 6854.0 / 7800.0 | 16500 / 17000 | 16900 / 18000 | 2.2 / 4.9 | 6.1 / 6.8 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | n/a: out 14421938, input 18848496 (count not checkable) |
| flink-sql | s1 | r3 | 11732990 | 39110 | 11278 / 16954 / 18473 | 12411 / 18785 | 6859.0 / 8078.0 | 12000 / 12500 | 16900 / 18400 | 2.1 / 4.5 | 6.0 / 6.6 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | n/a: out 14327288, input 18848434 (count not checkable) |
| spark-rtm-sw1 | s2 | r1 | 15298290 | 50994 | 21.5 / 9301.9 / 10228 | 47.4 / 9329.7 | n/a / n/a | 9400.0 / 9900.0 | 9200.0 / 10200 | 5.5 / 6.1 | 8.2 / 9.3 | async-log-write=0, pipelined=yes | PASS: out 18848434 vs unique 18848434 (diff 0) |
| spark-rtm-sw1 | s2 | r2 | 10713294 | 35711 | 74451 / 146434 / 148395 | 74484 / 146465 | n/a / n/a | 61800 / 62400 | 146400 / 148300 | 2.8 / 6.1 | 8.1 / 9.3 | async-log-write=0, pipelined=yes | PASS: out 18848434 vs unique 18848434 (diff 0) |
| spark-rtm-sw1 | s2 | r3 | 15287480 | 50958 | 22.3 / 9177.3 / 10673 | 47.7 / 9209.2 | n/a / n/a | 9300.0 / 10300 | 9100.0 / 10600 | 5.6 / 6.0 | 8.3 / 9.3 | async-log-write=0, pipelined=yes | PASS: out 18848434 vs unique 18848434 (diff 0) |
| spark-rtm | s2 | r1 | 14881247 | 49604 | 38.4 / 8055.8 / 9401.8 | 65.3 / 8086.6 | n/a / n/a | 5600.0 / 6100.0 | 8100.0 / 9400.0 | 3.4 / 5.6 | 8.1 / 9.1 | async-log-write=0, pipelined=yes | PASS: out 18848434 vs unique 18848434 (diff 0) |
| spark-rtm | s2 | r2 | 14880360 | 49601 | 38.3 / 8008.5 / 9450.0 | 63.2 / 8035.8 | n/a / n/a | 5700.0 / 6200.0 | 8000.0 / 9400.0 | 3.5 / 5.8 | 8.2 / 9.1 | async-log-write=0, pipelined=yes | PASS: out 18848434 vs unique 18848434 (diff 0) |
| spark-rtm | s2 | r3 | 14881389 | 49605 | 38.1 / 7664.4 / 9104.6 | 63.5 / 7700.0 | n/a / n/a | 5700.0 / 6500.0 | 7700.0 / 9100.0 | 3.4 / 5.7 | 8.3 / 9.3 | async-log-write=0, pipelined=yes | PASS: out 18848434 vs unique 18848434 (diff 0) |
| flink-ds-bt100 | s2 | r1 | 14881809 | 49606 | 51.8 / 263.7 / 1572.2 | 75.8 / 294.9 | n/a / n/a | 1140.0 / 1570.0 | 100.0 / 210.0 | 1.6 / 4.2 | 5.3 / 5.6 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | PASS: out 18848496 vs unique 18848496 (diff 0) |
| flink-ds-bt100 | s2 | r2 | 14881026 | 49603 | 51.8 / 281.4 / 1598.4 | 76.4 / 303.4 | n/a / n/a | 1160.0 / 1590.0 | 100.0 / 180.0 | 1.7 / 4.3 | 5.3 / 5.6 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | PASS: out 18848496 vs unique 18848496 (diff 0) |
| flink-ds-bt100 | s2 | r3 | 14880158 | 49600 | 51.9 / 235.0 / 1472.1 | 76.5 / 264.9 | n/a / n/a | 1100.0 / 1470.0 | 100.0 / 170.0 | 1.6 / 3.1 | 5.3 / 5.6 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | PASS: out 18848496 vs unique 18848496 (diff 0) |
| flink-ds-eo | s2 | r1 | 14880266 | 49601 | 6.8 / 335.9 / 1688.1 | 43948 / 61784 | n/a / n/a | 1340.0 / 1680.0 | 21.0 / 130.0 | 3.4 / 6.0 | 5.4 / 5.7 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | PASS: out 18848562 - unique 18848310 = 252 (markers <= 324) |
| flink-ds-eo | s2 | r2 | 14879578 | 49599 | 6.8 / 308.2 / 1769.0 | 43865 / 61853 | n/a / n/a | 1300.0 / 1760.0 | 22.0 / 100.0 | 3.3 / 6.0 | 5.4 / 5.7 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | PASS: out 18848624 - unique 18848372 = 252 (markers <= 324) |
| flink-ds | s2 | r1 | 14880326 | 49601 | 6.8 / 313.0 / 1723.3 | 26.5 / 334.9 | n/a / n/a | 1250.0 / 1720.0 | 20.0 / 100.0 | 3.1 / 4.7 | 5.3 / 5.7 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | PASS: out 18848496 vs unique 18848496 (diff 0) |
| flink-ds | s2 | r2 | 14880590 | 49602 | 6.9 / 337.9 / 1828.3 | 27.0 / 361.1 | n/a / n/a | 1310.0 / 1820.0 | 13.0 / 230.0 | 3.4 / 6.0 | 5.3 / 5.6 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | PASS: out 18848496 vs unique 18848496 (diff 0) |
| flink-ds | s2 | r3 | 14880512 | 49602 | 6.8 / 313.3 / 1719.8 | 26.7 / 335.8 | n/a / n/a | 1250.0 / 1710.0 | 15.0 / 130.0 | 3.2 / 5.2 | 5.3 / 5.6 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | PASS: out 18848496 vs unique 18848496 (diff 0) |
| flink-sql-bt100 | s2 | r1 | 14879930 | 49600 | 51.1 / 556.5 / 1886.3 | 75.1 / 578.7 | n/a / n/a | 1470.0 / 1880.0 | 100.0 / 180.0 | 1.9 / 4.0 | 5.8 / 6.3 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | PASS: out 18848496 vs unique 18848496 (diff 0) |
| flink-sql-bt100 | s2 | r2 | 14880962 | 49603 | 51.0 / 534.6 / 2028.8 | 73.9 / 553.9 | n/a / n/a | 1460.0 / 2000.0 | 100.0 / 250.0 | 1.9 / 4.9 | 5.8 / 6.3 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | PASS: out 18848496 vs unique 18848496 (diff 0) |
| flink-sql-bt100 | s2 | r3 | 14880497 | 49602 | 50.8 / 608.0 / 2086.0 | 74.2 / 628.9 | n/a / n/a | 1530.0 / 2000.0 | 100.0 / 160.0 | 2.0 / 4.6 | 5.8 / 6.3 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | PASS: out 18848496 vs unique 18848496 (diff 0) |
| flink-sql-eo | s2 | r1 | 14879862 | 49600 | 6.9 / 661.2 / 2063.4 | 44734 / 62107 | n/a / n/a | 1640.0 / 2000.0 | 27.0 / 140.0 | 3.6 / 6.1 | 5.9 / 6.4 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | PASS: out 18848748 - unique 18848496 = 252 (markers <= 324) |
| flink-sql-eo | s2 | r2 | 14879901 | 49600 | 6.8 / 752.0 / 5057.6 | 44600 / 62264 | n/a / n/a | 1780.0 / 2200.0 | 25.0 / 5000.0 | 3.8 / 6.0 | 5.8 / 6.3 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | PASS: out 18848750 - unique 18848496 = 254 (markers <= 324) |
| flink-sql | s2 | r1 | 14880883 | 49603 | 6.9 / 724.5 / 5099.3 | 27.2 / 747.9 | n/a / n/a | 1690.0 / 2100.0 | 20.0 / 5000.0 | 3.7 / 6.1 | 5.8 / 6.3 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | FAIL: out 18848502 vs unique 18848496 (diff 6) |
| flink-sql | s2 | r2 | 14881429 | 49605 | 6.9 / 624.3 / 2088.5 | 26.9 / 647.0 | n/a / n/a | 1640.0 / 2000.0 | 22.0 / 94.0 | 3.5 / 6.0 | 5.8 / 6.3 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | PASS: out 18848434 vs unique 18848434 (diff 0) |
| flink-sql | s2 | r3 | 14881035 | 49603 | 6.8 / 723.8 / 2050.8 | 26.7 / 747.3 | n/a / n/a | 1710.0 / 2000.0 | 18.0 / 100.0 | 3.6 / 5.9 | 5.8 / 6.3 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | PASS: out 18848496 vs unique 18848496 (diff 0) |
| spark-rtm-sw1 | s3 | r1 | 6473155 | 21577 | 130800 / 184293 / 193324 | 130833 / 184347 | n/a / n/a | 158200 / 158500 | 184300 / 193300 | 5.6 / 6.0 | 8.8 / 9.8 | async-log-write=0, pipelined=yes | n/a: out 13903631, input 18848434 (count not checkable) |
| spark-rtm-sw1 | s3 | r2 | 6404040 | 21347 | 128027 / 184064 / 192271 | 128058 / 184132 | n/a / n/a | 151800 / 152900 | 184200 / 192200 | 5.5 / 6.0 | 8.6 / 9.8 | async-log-write=0, pipelined=yes | n/a: out 14052909, input 18848434 (count not checkable) |
| spark-rtm-sw1 | s3 | r3 | 6411086 | 21370 | 131444 / 183478 / 193655 | 131474 / 183537 | n/a / n/a | 148500 / 149400 | 183500 / 193600 | 5.7 / 6.2 | 8.5 / 9.5 | async-log-write=0, pipelined=yes | n/a: out 13955020, input 18848496 (count not checkable) |
| spark-rtm | s3 | r1 | 6464757 | 21549 | 109841 / 182823 / 183943 | 109873 / 182854 | n/a / n/a | 118100 / 119300 | 182800 / 183900 | 5.5 / 6.0 | 8.4 / 9.6 | async-log-write=0, pipelined=yes | n/a: out 15552678, input 18848434 (count not checkable) |
| spark-rtm | s3 | r2 | 6496839 | 21656 | 106597 / 179522 / 184740 | 106632 / 179553 | n/a / n/a | 111500 / 112000 | 179500 / 184700 | 5.5 / 6.0 | 8.5 / 9.5 | async-log-write=0, pipelined=yes | n/a: out 15691471, input 18848372 (count not checkable) |
| spark-rtm | s3 | r3 | 6324237 | 21081 | 111494 / 182365 / 187201 | 111533 / 182428 | n/a / n/a | 136600 / 138700 | 182500 / 187200 | 5.5 / 6.0 | 8.5 / 9.6 | async-log-write=0, pipelined=yes | n/a: out 15323059, input 18848434 (count not checkable) |
| flink-ds-bt100 | s3 | r1 | 11737745 | 39126 | 70.0 / 4404.2 / 6069.6 | 95.1 / 4454.7 | n/a / n/a | 4500.0 / 4800.0 | 4300.0 / 6000.0 | 3.4 / 5.6 | 6.0 / 6.7 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | n/a: out 14898073, input 18848496 (count not checkable) |
| flink-ds-bt100 | s3 | r2 | 11737367 | 39125 | 69.4 / 4399.4 / 6496.0 | 94.2 / 4444.1 | n/a / n/a | 4500.0 / 5000.0 | 4300.0 / 6400.0 | 3.3 / 5.6 | 6.0 / 6.8 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | n/a: out 14896709, input 18848496 (count not checkable) |
| flink-ds-bt100 | s3 | r3 | 11739755 | 39132 | 68.3 / 4405.1 / 6198.0 | 93.0 / 4455.1 | n/a / n/a | 4500.0 / 4800.0 | 4300.0 / 6100.0 | 3.5 / 5.7 | 6.0 / 6.8 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | n/a: out 14901944, input 18848434 (count not checkable) |
| flink-ds | s3 | r1 | 11718423 | 39061 | 8.4 / 4438.8 / 5787.5 | 34.4 / 4477.2 | n/a / n/a | 4500.0 / 4700.0 | 4300.0 / 5700.0 | 4.9 / 6.0 | 6.0 / 6.8 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | n/a: out 14889694, input 18848434 (count not checkable) |
| flink-ds | s3 | r2 | 11716478 | 39055 | 8.4 / 4305.6 / 5568.2 | 34.4 / 4348.7 | n/a / n/a | 4300.0 / 4600.0 | 4200.0 / 5500.0 | 4.8 / 6.0 | 6.0 / 6.7 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | n/a: out 14888029, input 18848434 (count not checkable) |
| flink-ds | s3 | r3 | 11717122 | 39057 | 8.5 / 4583.9 / 6059.8 | 34.6 / 4623.1 | n/a / n/a | 4600.0 / 4800.0 | 4500.0 / 6000.0 | 4.9 / 6.0 | 6.0 / 6.8 | {'restored': 0, 'total': 8, 'in_progress': 0, 'completed': 7, 'failed': 1} | n/a: out 14881351, input 18848310 (count not checkable) |
