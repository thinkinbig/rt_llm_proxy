#!/usr/bin/env python3
"""Plot the local capacity sweep: pacing health (% frames >=30ms) and proxy CPU
vs concurrent sessions. Reads capacity-local.csv, writes capacity-local.png.

Run: python3 docs/bench/plot_capacity.py
"""
import csv
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

here = os.path.dirname(__file__)
n, drift, cpu = [], [], []
with open(os.path.join(here, "capacity-local.csv")) as f:
    for row in csv.DictReader(f):
        n.append(int(row["n"]))
        drift.append(float(row["ge30ms_pct"]))
        cpu.append(float(row["proxy_cpu_pct"]))

fig, ax1 = plt.subplots(figsize=(7, 4.5))
ax1.set_xlabel("concurrent sessions (n)")
ax1.set_ylabel("% frames >= 30ms (pacing drift)", color="tab:red")
ax1.plot(n, drift, "o-", color="tab:red", label="pacing drift")
ax1.tick_params(axis="y", labelcolor="tab:red")
ax1.axhline(5, ls="--", color="tab:red", alpha=0.4)
ax1.annotate("SLO budget ~5%", (n[0], 6), color="tab:red", fontsize=8)

ax2 = ax1.twinx()
ax2.set_ylabel("proxy CPU (%, 100 = 1 core)", color="tab:blue")
ax2.plot(n, cpu, "s-", color="tab:blue", label="proxy CPU")
ax2.tick_params(axis="y", labelcolor="tab:blue")

plt.title("rt-llm-proxy capacity (LOCAL, loadgen co-located — pessimistic)")
fig.tight_layout()
out = os.path.join(here, "capacity-local.png")
fig.savefig(out, dpi=120)
print("wrote", out)
