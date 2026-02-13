#!/usr/bin/env python3
"""
Chao-Jung Wu
2026-02-13

Convert metadata tsv to parquet format

Input:
    ../output/Cohort_X_progressive_counts.tsv

Output:
    ../output/Cohort_X.parquet
"""
import sys
from pathlib import Path
import pandas as pd

REQUIRED_COLUMNS = ["SampleID", "Age", "Ancestry", "IQ", "Cohort", "Het_Count"]

def tsv_to_parquet_for_cohort(tsv_path, parquet_path):
    """
    Read a cohort *_progressive_counts.tsv, keep only REQUIRED_COLUMNS,
    and write Cohort_X.parquet
    """
    df = pd.read_csv(tsv_path, sep="\t", dtype={"SampleID": str})

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"{tsv_path.name}: missing required columns: {missing}")

    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    df[REQUIRED_COLUMNS].to_parquet(parquet_path, index=False)

def main(tsv_root="../output/", parquet_root="../output/"):
    """
    Automatically finds per-cohort TSVs produced by your pipeline:
      Cohort_*_progressive_counts.tsv
    Then writes:
      Cohort_*.parquet
    """
    tsv_dir = Path(tsv_root).resolve()
    out_dir = Path(parquet_root).resolve()

    if not tsv_dir.exists():
        print(f"[ERROR] TSV folder not found: {tsv_dir}", file=sys.stderr)
        return 2

    # Match the TSVs your script produces
    tsvs = sorted(tsv_dir.glob("Cohort_*_progressive_counts.tsv"))
    if not tsvs:
        print(f"[ERROR] No TSVs found matching 'Cohort_*_progressive_counts.tsv' in {tsv_dir}", file=sys.stderr)
        return 3

    for tsv_path in tsvs:
        # cohort name is everything before "_progressive_counts.tsv"
        cohort_name = tsv_path.name.replace("_progressive_counts.tsv", "")
        parquet_path = out_dir / f"{cohort_name}.parquet"

        try:
            tsv_to_parquet_for_cohort(tsv_path, parquet_path)
            print(f"[OK] Wrote {parquet_path}")
        except Exception as e:
            print(f"[ERROR] Failed for {tsv_path.name}: {e}", file=sys.stderr)
            return 4

    return 0


if __name__ == "__main__":
    sys.exit(main())