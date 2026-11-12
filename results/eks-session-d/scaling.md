# Session D scaling: per run

Phases: 1 = 50k (incl. job warm-up), 2 = 100k, 3 = back to 50k evt/s; -scalein rows: 1 = 100k (incl. warm-up), 2 = 25k. steady = s from the step until every later 60 s window has pipeline p99 < 2 s (- = never in the phase); degraded = seconds with own p99 >= 2 s; spike = worst per-second p99; stall = longest output gap (< 10 % of input). Node-hours over the load (engine nodes, 10 s samples).

| run | steady s (1/2/3) | degraded s (1/2/3) | spike p99 ms (1/2/3) | p99 ms (1/2/3) | stall s (1/2/3) | rescales | nodes mean/max | node-h | vs peak | $ | lost / over / settled | dup records |
|---|---|---|---|---|---|---:|---|---:|---:|---:|---|---:|
| flink-auto-ro-s1-update-steps-r1 | 60 / 860 / 0 | 8 / 45 / 19 | 2700 / 3500 / 2700 | 1500 / 2000 / 1420 | 4 / 1 / 1 | 1 | 2.0/2 | 0.997 | 0.5 | 0.38 | 0 / 0 / 901244 | 196924 |
| flink-auto-s1-update-steps-r1 | 89 / 979 / 559 | 16 / 53 / 38 | 2800 / 8200 / 7200 | 1640 / 2800 / 1830 | 3 / 1 / 1 | 2 | 2.0/2 | 1.528 | 0.5 | 0.59 | 0 / 0 / 1309574 | 475141 |
| flink-auto-s1-update-steps-r2 | 90 / - / 690 | 12 / 62 / 49 | 2800 / 7500 / 6600 | 1530 / 2700 / 1790 | 3 / 1 / 1 | 3 | 2.0/2 | 1.53 | 0.5 | 0.59 | 0 / 0 / 1309214 | 703041 |
| flink-peak-s1-update-steps-r1 | 59 / 0 / 0 | 0 / 16 / 0 | 1440 / 2200 / 1770 | 450 / 1140 / 410 | 4 / 0 / 0 | 0 | 4.0/4 | 3.057 | 1.0 | 1.17 | 0 / 0 / 1309660 | 0 |
| spark-mb-da-s1-steps-r1 | - / - / - | 503 / 1020 / 1199 | 25300 / 45300 / 7800 | 14300 / 31100 / 6200 | 23 / 8 / 2 | 1 | 3.0/3 | 2.3 | 0.73 | 0.88 | 0 / 0 / 1305336 | -28346898 |
| spark-mb-da-s1-steps-r2 | - / - / - | 504 / 1020 / 1197 | 24300 / 31300 / 8900 | 12200 / 19900 / 6200 | 22 / 6 / 2 | 1 | 3.0/3 | 2.3 | 0.73 | 0.88 | 0 / 0 / 1305466 | -27680740 |
| spark-rtm-manual-s1-steps-r1 | - / - / - | 186 / 486 / 821 | 213600 / 222400 / 222200 | 209600 / 166300 / 209200 | 10 / 150 / 145 | 2 | 3.25/4 | 2.493 | 0.79 | 0.96 | 0 / 0 / 1305084 | 3686080 |
| spark-rtm-manual-s1-steps-r2 | - / - / - | 279 / 484 / 892 | 225800 / 225100 / 224100 | 222100 / 171500 / 214600 | 11 / 155 / 138 | 2 | 3.34/4 | 2.559 | 0.81 | 0.98 | 0 / 0 / 1309362 | 4048402 |
| spark-rtm-peak-s1-steps-r1 | - / - / - | 166 / 151 / 82 | 76500 / 9600 / 7100 | 71400 / 6500 / 4400 | 59 / 7 / 5 | 3 | 4.11/5 | 3.146 | 1.0 | 1.21 | 0 / 0 / 1305229 | 0 |

Scaling paths (Flink vertex parallelism / Spark running executors / HPA current/desired):

- flink-auto-ro-s1-update-steps-r1: 3,3->6,6; start nodes 1 after 0 s wait
- flink-auto-s1-update-steps-r1: 6,6->3,6->3,4; start nodes 2 after 0 s wait
- flink-auto-s1-update-steps-r2: 3,3->6,6->3,6->3,4; start nodes 3 after 482 s wait
- flink-peak-s1-update-steps-r1: 12,12; start nodes 2 after 286 s wait
- spark-mb-da-s1-steps-r1: 2->3; start nodes 2 after 0 s wait
- spark-mb-da-s1-steps-r2: 2->3; start nodes 2 after 452 s wait
- spark-rtm-manual-s1-steps-r1: 2->0->2->0->2->3->4->0->2; manual restart (stop -> query started) s: [101, 320]; start nodes 2 after 0 s wait
- spark-rtm-manual-s1-steps-r2: 2->0->2->0->2->4->0->2; manual restart (stop -> query started) s: [320, 25]; start nodes 2 after 0 s wait
- spark-rtm-peak-s1-steps-r1: 0->1->2->4; start nodes 2 after 361 s wait
