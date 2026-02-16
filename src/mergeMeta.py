#!/usr/bin/env python3
"""
Merge progressive count summaries

Input:
  ../output/file_check.tsv
  ../output/log.txt

Output:
  ../output/all_cohorts_progressive_counts.tsv
  ../output/<Cohort>_progressive_counts.tsv
"""
import re
import sys
from pathlib import Path
import pandas as pd

OUTDIR = Path("../output")
FILE_CHECK = OUTDIR / "file_check.tsv"
LOGTXT = OUTDIR / "log.txt"

HEADER = [
    "SampleID","Age","Ancestry","IQ","Cohort","GZ_File","Filtered_GZ_File",
    "N_Records","After_GT_Het","After_DP","After_GQ","Het_Count"
]

KV = re.compile(r"(\w+)=([^\s]+)")
def parse_log(log_path):
    rows = []
    for line in log_path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.startswith("[OK]"):
            continue
        d = dict(KV.findall(line))
        if "in" not in d:
            continue
        rows.append({
            "GZ_File": d["in"],
            "Filtered_GZ_File": d.get("out", "NA"),
            "N_Records": d.get("N_Records", "NA"),
            "After_GT_Het": d.get("After_GT_Het", "NA"),
            "After_DP": d.get("After_DP", "NA"),
            "After_GQ": d.get("After_GQ", "NA"),
            "Het_Count": d.get("Het_Count", "NA"),
        })
    return pd.DataFrame(rows).drop_duplicates("GZ_File", keep="last")

def main():
    if not FILE_CHECK.exists():
        print(f"[ERROR] Missing file: {FILE_CHECK}", file=sys.stderr)
        return 2
    if not LOGTXT.exists():
        print(f"[ERROR] Missing file: {LOGTXT}", file=sys.stderr)
        return 3

    meta = pd.read_csv(FILE_CHECK, sep="\t", dtype=str).fillna("NA")
    meta["GZ_File"] = meta["GZ_File"].map(lambda s: Path(str(s)).name)
    logdf = parse_log(LOGTXT)

    df = meta.merge(logdf, on="GZ_File", how="left", suffixes=("", "_log"))
    if "Filtered_GZ_File_log" in df.columns:
        df["Filtered_GZ_File"] = df["Filtered_GZ_File"].where(
            df["Filtered_GZ_File"].notna() & (df["Filtered_GZ_File"] != "NA"),
            df["Filtered_GZ_File_log"]
        )
        df = df.drop(columns=["Filtered_GZ_File_log"])

    for c in ["N_Records","After_GT_Het","After_DP","After_GQ","Het_Count","Filtered_GZ_File"]:
        if c not in df.columns:
            df[c] = "NA"
        df[c] = df[c].fillna("NA")

    df = df[HEADER]
    out_all = OUTDIR / "all_cohorts_progressive_counts.tsv"
    df.to_csv(out_all, sep="\t", index=False)

    for cohort, sub in df.groupby("Cohort"):
        sub.to_csv(OUTDIR / f"{cohort}_progressive_counts.tsv", sep="\t", index=False)
    print(f"[OK] Wrote merged and per-cohort progressive TSVs to: {OUTDIR}")
    return 0


if __name__ == "__main__":
    sys.exit(main())