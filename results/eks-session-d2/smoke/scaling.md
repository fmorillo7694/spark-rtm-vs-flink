# Session D scaling: per run

Phases: 1 = 50k (incl. job warm-up), 2 = 100k, 3 = back to 50k evt/s; -scalein rows: 1 = 100k (incl. warm-up), 2 = 25k. steady = s from the step until every later 60 s window has pipeline p99 < 2 s (- = never in the phase); degraded = seconds with own p99 >= 2 s; spike = worst per-second p99; stall = longest output gap (< 10 % of input). Node-hours over the load (engine nodes, 10 s samples).

| run | steady s (1/2/3) | degraded s (1/2/3) | spike p99 ms (1/2/3) | p99 ms (1/2/3) | stall s (1/2/3) | rescales | nodes mean/max | node-h | vs peak | $ | lost / over / settled | dup records |
|---|---|---|---|---|---|---:|---|---:|---:|---:|---|---:|

Scaling paths (Flink vertex parallelism / Spark running executors / HPA current/desired):

