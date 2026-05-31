#!/usr/bin/env python3
"""Before/after for the P6 zero-alloc change: parse opus-baseline.txt and
opus-p6.txt, average each benchmark's runs, and plot B/op and sec/op side by
side. Writes p6-compare.png.

Run: python3 docs/bench/plot_p6_compare.py
"""
import os
import re
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

here = os.path.dirname(__file__)
LINE = re.compile(r"^(Benchmark\w+)-\d+\s+\d+\s+([\d.]+)\s+ns/op\s+(\d+)\s+B/op")


def parse(path):
    ns, bop = defaultdict(list), defaultdict(list)
    with open(os.path.join(here, path)) as f:
        for line in f:
            m = LINE.match(line.strip())
            if m:
                name = m.group(1).replace("Benchmark", "")
                ns[name].append(float(m.group(2)))
                bop[name].append(float(m.group(3)))
    avg = lambda d: {k: sum(v) / len(v) for k, v in d.items()}
    return avg(ns), avg(bop)

base_ns, base_b = parse("opus-baseline.txt")
p6_ns, p6_b = parse("opus-p6.txt")
ops = ["Encode", "Decode", "Roundtrip"]
x = range(len(ops))
w = 0.35

fig, (axb, axn) = plt.subplots(1, 2, figsize=(10, 4.2))

axb.bar([i - w / 2 for i in x], [base_b[o] / 1024 for o in ops], w, label="baseline", color="tab:gray")
axb.bar([i + w / 2 for i in x], [p6_b[o] / 1024 for o in ops], w, label="P6 (reuse)", color="tab:green")
axb.set_xticks(list(x)); axb.set_xticklabels(ops)
axb.set_ylabel("KiB allocated / frame")
axb.set_title("Heap per frame: 1.25/6/7.25 KiB -> 0  (-100%)")
axb.legend()

axn.bar([i - w / 2 for i in x], [base_ns[o] / 1000 for o in ops], w, label="baseline", color="tab:gray")
axn.bar([i + w / 2 for i in x], [p6_ns[o] / 1000 for o in ops], w, label="P6 (reuse)", color="tab:green")
axn.set_xticks(list(x)); axn.set_xticklabels(ops)
axn.set_ylabel("µs / frame")
axn.set_title("Latency: unchanged (Decode -4.6%, no regression)")
axn.legend()

fig.suptitle("P6 — zero-alloc audio hot path (benchstat, count=10)")
fig.tight_layout()
out = os.path.join(here, "p6-compare.png")
fig.savefig(out, dpi=120)
print("wrote", out)
