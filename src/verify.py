#!/usr/bin/env python3
"""
Chao-Jung Wu
2026-02-13

Verify gVCF file presence and line counts per sample.

Input:
    ../input/Cohort_*/metadata.tsv
    ../input/Cohort_*/*.gz

Output:
    ../output/file_check.tsv
"""
import csv
import gzip
import sys
from pathlib import Path
import shutil


def line_count_of_gz(p: Path) -> int:
    n = 0
    with gzip.open(p, "rb") as f:
        for _ in f:
            n += 1
    return n

def main(input_root="../input/", output_root="../output/"):
    in_root = Path(input_root)
    out_root = Path(output_root)

    # Clean output directory if exists
    if out_root.exists():
        shutil.rmtree(out_root)
    out_root.mkdir(parents=True, exist_ok=True)
    out_file = out_root / "file_check.tsv"

    with out_file.open("w", newline="") as out_fh:
        w = csv.writer(out_fh, delimiter="\t")
        w.writerow(
            ["SampleID", "Age", "Ancestry", "IQ",
             "Cohort", "GZ_File", "Line_Count", "Note"]
        )

        for cohort in sorted(in_root.glob("Cohort_*")):
            meta = cohort / "metadata.tsv"
            if not meta.exists():
                continue

            with meta.open() as fh:
                r = csv.DictReader(fh, delimiter="\t")
                for rec in r:
                    sid = rec["SampleID"]
                    hits = sorted(cohort.glob(f"{sid}*.gz"))

                    note = "NORMAL" if len(hits) == 1 else (
                        "MISSING" if len(hits) == 0 else "MULTIPLE_MATCH"
                    )
                    gz = hits[0] if len(hits) == 1 else None

                    w.writerow([
                        sid,
                        rec.get("Age", ""),
                        rec.get("Ancestry", ""),
                        rec.get("IQ", ""),
                        cohort.name,
                        gz.name if gz else "",
                        line_count_of_gz(gz) if gz else "",
                        note
                    ])

    print(f"[OK] Wrote line count stats to: {out_file}")

if __name__ == "__main__":
    if len(sys.argv) == 1:
        sys.exit(main())
    elif len(sys.argv) == 3:
        sys.exit(main(sys.argv[1], sys.argv[2]))
    else:
        print("Usage: python verify.py [input_dir] [output_dir]")
        sys.exit(1)