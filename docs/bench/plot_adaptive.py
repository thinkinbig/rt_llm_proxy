#!/usr/bin/env python3
"""Adaptive complexity A/B: Opus complexity over time as load ramps to 150
sessions, for static / session-proactive / drift-reactive. Writes adaptive-ab.png.

Data from the live experiment, sampled every 0.5s from t=0 (so the idle c=10
start and the staircase are captured, not missed by coarse sampling).
"""
import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

t = [i * 0.5 for i in range(17)]  # 0.0 .. 8.0s
static   = [10] * 17
# A: starts at 10 (idle), steps down as sessions cross 40 then 90, then stable.
sessions = [10, 10, 10, 5, 5, 5, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3]
# B: drops to the floor fast, then hunts 3<->5 in steady state under load.
drift    = [10, 10, 5, 5, 3, 3, 3, 3, 3, 3, 3, 3, 5, 3, 3, 3, 5]

fig, ax = plt.subplots(figsize=(8, 4.4))
ax.plot(t, static, "--", color="tab:gray", label="static (drift 21.2%)")
ax.step(t, sessions, "-", where="post", color="tab:green", marker="s", ms=4,
        label="A sessions (drift 15.6%, stable)")
ax.step(t, drift, "-", where="post", color="tab:red", marker="^", ms=4,
        label="B drift (drift 14.8%, oscillates 3<->5)")
ax.axhline(10, ls=":", color="tab:gray", alpha=0.4)
ax.annotate("both start at c=10 when idle", (0.1, 10.2), fontsize=8, color="dimgray")
ax.set_xlabel("time (s) — load ramping to 150 sessions")
ax.set_ylabel("Opus complexity")
ax.set_yticks([0, 3, 5, 8, 10])
ax.set_ylim(0, 11.5)
ax.set_title("Adaptive complexity under load: A steps down & stays; B oscillates")
ax.legend(loc="center right")
ax.grid(True, alpha=0.3)
fig.tight_layout()
out = os.path.join(os.path.dirname(__file__), "adaptive-ab.png")
fig.savefig(out, dpi=120)
print("wrote", out)
