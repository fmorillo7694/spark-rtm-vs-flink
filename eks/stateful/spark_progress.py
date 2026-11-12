#!/usr/bin/env python3
"""
Spark driver log (stdin) -> one compact JSON line per "Streaming query made progress" block
(stdout): the Spark side of the checkpoint metrics. Per batch (= one RTM checkpoint):
batchId, batchDuration, durationMs.walCommit / commitOffsets, and per state operator
commitTimeMs (state-store commit, summed over partitions), memoryUsedBytes, numRowsTotal and
the RocksDB bytes written / changelog commit latency.

Used by eks/stateful/run_matrix_incluster.sh (evidence) and by bench/resource_summary.py tests.
Usage: kubectl logs <driver> | python3 spark_progress.py > progress.jsonl
"""
import json
import sys

MARK = "Streaming query made progress: "
KEEP_OP = ("operatorName", "numRowsTotal", "commitTimeMs", "memoryUsedBytes")
KEEP_CM = ("rocksdbTotalBytesWritten", "rocksdbChangeLogWriterCommitLatencyMs",
           "rocksdbCommitFileSyncLatencyMs", "rocksdbSstFileSize",
           # final run: native memory and checkpoint upload size (kept only when 4.3 reports them)
           "rocksdbPinnedBlocksMemoryUsage", "rocksdbFilesCopiedBytes", "rocksdbFilesReused")


def blocks(lines):
    buf = None
    for line in lines:
        if buf is None:
            i = line.find(MARK)
            if i >= 0:
                buf = [line[i + len(MARK):]]
        else:
            buf.append(line)
        if buf is not None and line.rstrip() == "}":
            try:
                yield json.loads("".join(buf))
            except ValueError:
                pass
            buf = None


def compact(p):
    ops = []
    for o in p.get("stateOperators") or []:
        d = {k: o.get(k) for k in KEEP_OP}
        cm = o.get("customMetrics") or {}
        d.update({k: cm[k] for k in KEEP_CM if k in cm})
        ops.append(d)
    dm = p.get("durationMs") or {}
    return {"batchId": p.get("batchId"), "timestamp": p.get("timestamp"),
            "batchDuration": p.get("batchDuration"), "numInputRows": p.get("numInputRows"),
            "walCommit": dm.get("walCommit"), "commitOffsets": dm.get("commitOffsets"),
            "stateOperators": ops}


def main():
    for p in blocks(sys.stdin):
        print(json.dumps(compact(p), separators=(",", ":")))


if __name__ == "__main__":
    main()
