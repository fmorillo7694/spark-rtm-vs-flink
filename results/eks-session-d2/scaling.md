# Session D scaling: per run

Phases: 1 = 50k (incl. job warm-up), 2 = 100k, 3 = back to 50k evt/s; -scalein rows: 1 = 100k (incl. warm-up), 2 = 25k. steady = s from the step until every later 60 s window has pipeline p99 < 2 s (- = never in the phase); degraded = seconds with own p99 >= 2 s; spike = worst per-second p99; stall = longest output gap (< 10 % of input). Node-hours over the load (engine nodes, 10 s samples).

| run | steady s (1/2/3) | degraded s (1/2/3) | spike p99 ms (1/2/3) | p99 ms (1/2/3) | stall s (1/2/3) | rescales | nodes mean/max | node-h | vs peak | $ | lost / over / settled | dup records |
|---|---|---|---|---|---|---:|---|---:|---:|---:|---|---:|
| flink-auto-s1-update-scalein-r1 | 589 / 759 | 103 / 26 | 20900 / 4700 | 13500 / 1450 | 3 / 1 | 4 | 3.08/4 | 2.36 | - | 0.91 | 0 / 0 / 1009565 | 530393 |
| flink-reactive-s1-update-scalein-r1 | 640 / 380 | 158 / 50 | 22000 / 46000 | 17500 / 29700 | 3 / 1 | 4 | 3.37/4 | 2.578 | - | 0.99 | 0 / 0 / 1010869 | 1733743 |
| flink-reactive-s1-update-steps-r1 | 389 / 289 / 0 | 61 / 57 / 0 | 16700 / 13100 / 1850 | 11900 / 4500 / 590 | 4 / 1 / 0 | 3 | 3.9/5 | 2.991 | - | 1.15 | 0 / 0 / 1305786 | 575657 |
| flink-reactive-s1-update-steps-r2 | 460 / 290 / 0 | 70 / 54 / 0 | 19600 / 12200 / 1660 | 14200 / 3900 / 540 | 3 / 1 / 0 | 3 | 3.67/4 | 2.806 | - | 1.08 | 0 / 0 / 1305506 | 543910 |

Scaling paths (Flink vertex parallelism / Spark running executors / HPA current/desired):

- flink-auto-s1-update-scalein-r1: 3,3->6,6->12,8->6,6->3,3; start nodes 2 after 452 s wait
- flink-reactive-s1-update-scalein-r1: 3,3->6,6->9,9->12,12->9,9; HPA /0->2/2->2/3->3/3->3/4->4/4->4/3->3/3; start nodes 1 after 0 s wait
- flink-reactive-s1-update-steps-r1: 3,3->6,6->9,9->12,12; HPA /0->2/2->2/3->3/3->3/4->4/4; start nodes 2 after 0 s wait
- flink-reactive-s1-update-steps-r2: 3,3->6,6->9,9->12,12; HPA /0->2/2->2/3->3/3->3/4->4/4; start nodes 2 after 437 s wait
