#!/usr/bin/env python3
"""out/raw/*.meta + *.log -> out/results.csv + out/results.md (median per label)."""
import csv, glob, os, re, statistics, sys

out = sys.argv[1]
KEY_COLS = ["upstream_done_p50_s", "read_done_p50_s", "rss_kb_per_stream", "cpu_ms_per_mib",
            "cg_mem_max_mb", "cg_anon_max_mb", "cg_file_max_mb", "cg_sock_max_mb",
            "loop_stall_max_ms", "pg_cpu_max", "pg_mem_max_mb"]
rows, order = [], []
for meta in sorted(glob.glob(os.path.join(out, "raw", "*.meta"))):
    r = {}
    for line in open(meta):
        k, _, v = line.rstrip("\n").partition("=")
        r[k] = v
    res = {}
    log = meta[:-5] + ".log"
    if os.path.exists(log):
        last = [l for l in open(log, errors="replace") if l.startswith("RESULT ")]
        if last:
            for kv in last[-1].split()[1:]:
                k, _, v = kv.partition("=")
                res[k] = v
    r["result"] = res
    rows.append(r)

base = ["label", "run", "status", "exit", "wall_s", "pg_cpu_max", "pg_mem_max_mb", "pg_mem_before_mb",
        "pg_mem_after_mb", "args"]
extra = []
for r in rows:
    for k in r["result"]:
        if k not in extra and k not in base:
            extra.append(k)
with open(os.path.join(out, "results.csv"), "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(base + extra)
    for r in rows:
        w.writerow([r.get(c, "") for c in base[:-1]] + [r.get("args", "")] + [r["result"].get(k, "") for k in extra])

def num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None

def med(vals):
    v = [x for x in map(num, vals) if x is not None]
    return statistics.median(v) if v else None

def fmt(x):
    if x is None:
        return "-"
    return f"{x:.0f}" if abs(x) >= 1000 else f"{x:.2f}".rstrip("0").rstrip(".")

labels = []
for r in rows:
    if r["label"] not in labels:
        labels.append(r["label"])
def get(r, k):
    return r["result"].get(k, r.get(k))
md = ["# slow_reader matrix (median over runs)", "", f"io_uring transport: {os.environ.get('URING', 'ON')}", "",
      "status = count per status over runs. Memory columns are whole-container cgroup peaks "
      "(the Docker VM, 2C/2G, is the box). pg_* = spill-pg container during postgres rows.", ""]
hdr = ["label", "status"] + KEY_COLS
md += ["| " + " | ".join(hdr) + " |", "|" + "|".join(["---"] * len(hdr)) + "|"]
for lab in labels:
    rs = [r for r in rows if r["label"] == lab]
    st = {}
    for r in rs:
        st[r["status"]] = st.get(r["status"], 0) + 1
    cells = [lab, ",".join(f"{v}x{k}" if len(rs) > 1 else k for k, v in st.items())]
    cells += [fmt(med([get(r, k) for r in rs])) for k in KEY_COLS]
    md.append("| " + " | ".join(cells) + " |")
more = [k for k in extra if k not in KEY_COLS and any(num(r["result"].get(k)) is not None for r in rows)
        and k not in ("streams", "mib", "read_kibps", "threshold_kib", "offload", "ok", "failed")]
if more:
    md += ["", "## other result keys (median)", "", "| label | " + " | ".join(more) + " |",
           "|" + "|".join(["---"] * (len(more) + 1)) + "|"]
    for lab in labels:
        rs = [r for r in rows if r["label"] == lab]
        md.append("| " + " | ".join([lab] + [fmt(med([r["result"].get(k) for r in rs])) for k in more]) + " |")
md += ["", "## runs", "", "| label | run | status | exit | wall_s | args |", "|---|---|---|---|---|---|"]
for r in rows:
    md.append(f"| {r['label']} | {r['run']} | {r['status']} | {r['exit']} | {r['wall_s']} | `{r.get('args','')}` |")
open(os.path.join(out, "results.md"), "w").write("\n".join(md) + "\n")
print("\n".join(md[:len(labels) + 6]))
