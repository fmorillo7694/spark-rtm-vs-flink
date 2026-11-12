#!/usr/bin/env python3
"""
Estimate the Docker VM clock offset vs the macOS host clock (NTP-style, min-RTT sample).

Why: `ts` is stamped by the host producer and `out_ts` by the engine inside the Docker VM,
so pipeline latency = true latency + (vm_clock - host_clock). At ~5 ms latencies a few ms
of VM clock offset/jitter is material; this probe records it next to each run.

Starts a tiny UDP echo-time server in a container (127.0.0.1-only published port) if one
isn't running, sends N probes, prints JSON {offset_ms, rtt_ms, ...} for the min-RTT sample.
offset_ms > 0 means the VM clock is AHEAD of the host (inflates measured latency).

Usage: clock_probe.py [--n 200] [--image pyflink-bench:2.3.0] [--stop]
"""
import argparse
import json
import socket
import struct
import subprocess
import time

NAME = "bench-clock-probe"
PORT = 39999
SERVER = (
    "import socket,struct,time\n"
    "s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM);s.bind(('0.0.0.0',%d))\n"
    "while True:\n"
    "  d,a=s.recvfrom(64);s.sendto(d+struct.pack('!d',time.time()),a)\n" % PORT
)


def ensure_server(image):
    running = subprocess.run(["docker", "ps", "-q", "-f", f"name=^{NAME}$"],
                             capture_output=True, text=True).stdout.strip()
    if not running:
        subprocess.run(["docker", "run", "-d", "--rm", "--name", NAME, "--entrypoint", "python3",
                        "-p", f"127.0.0.1:{PORT}:{PORT}/udp", image, "-c", SERVER],
                       check=True, capture_output=True)
        time.sleep(2)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=200)
    ap.add_argument("--image", default="pyflink-bench:2.3.0")
    ap.add_argument("--stop", action="store_true")
    args = ap.parse_args()
    if args.stop:
        subprocess.run(["docker", "rm", "-f", NAME], capture_output=True)
        return
    ensure_server(args.image)
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(0.5)
    samples = []
    for i in range(args.n):
        t0 = time.time()
        s.sendto(struct.pack("!I", i), ("127.0.0.1", PORT))
        try:
            d, _ = s.recvfrom(64)
        except socket.timeout:
            continue
        t1 = time.time()
        (tv,) = struct.unpack("!d", d[4:12])
        samples.append((t1 - t0, tv - (t0 + t1) / 2))
        time.sleep(0.005)
    if not samples:
        print(json.dumps({"error": "no replies"}))
        return
    samples.sort()
    rtt, off = samples[0]
    best = [o for _, o in samples[: max(1, len(samples) // 10)]]  # 10% lowest-RTT
    print(json.dumps({
        "at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "offset_ms": round(off * 1000, 3),
        "rtt_ms": round(rtt * 1000, 3),
        "offset_ms_low_rtt_range": [round(min(best) * 1000, 3), round(max(best) * 1000, 3)],
        "samples": len(samples),
    }))


if __name__ == "__main__":
    main()
