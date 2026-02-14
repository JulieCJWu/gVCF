#!/usr/bin/env python3
"""
Chao-Jung Wu
2026-02-13

filter gVCF by GT, DP, GQ

Input:
  ../input/Cohort_*/metadata.tsv
  ../input/Cohort_*/*.gz

Output:
  ../output/
    filtered_gvcf/Cohort_X/*.vcf.gz
    Cohort_X_progressive_counts.tsv
"""
import gzip
import sys
from pathlib import Path
import pandas as pd

# ----------------------------
# Function per criterion
# ----------------------------

def is_heterozygous(gt):
    """True for 0|1, 1|0"""
    return gt in {"0|1", "1|0"}

def dp_passes(dp):
    """True if DP is present and DP > 20"""
    return dp is not None and dp.isdigit() and int(dp) > 20

def gq_passes(gq):
    """True if GQ is present and GQ >= 30"""
    return gq is not None and gq.isdigit() and int(gq) >= 30


# ----------------------------
# gVCF helpers
# ----------------------------

def extract_gt_dp_gq(fmt, sample):
    """
    Given FORMAT (col 9) and sample column (col 10), extract GT, DP, GQ
    """
    keys = fmt.split(":")
    vals = sample.split(":")
    m = dict(zip(keys, vals))
    return m.get("GT"), m.get("DP"), m.get("GQ")
    
def find_sample_gz(cohort_dir, sample_id):
    hits = sorted(cohort_dir.glob(f"{sample_id}*.gz"))
    if len(hits) == 1:
        return hits[0]
    if len(hits) > 1:
        raise RuntimeError(f"[ERROR] Multiple .gz match SampleID={sample_id} in {cohort_dir}: "
                           + ", ".join(p.name for p in hits))
    return None


# ----------------------------
# Filter + progressive counting
# ----------------------------

def filter_and_count(in_gz, out_gz):
    """
    Writes a filtered gz VCF (keeps headers) with variants passing:
      - heterozygous GT
      - DP > 20
      - GQ >= 30

    Also returns progressive counts:
      - N_Records (non-header lines seen)
      - GT_Missing (missing GT before filtering)
      - After_GT_Het
      - After_DP
      - After_GQ (same as Het_Count)
    """
    n_records = gt_missing = after_gt = after_dp = after_gq = 0
    out_gz.parent.mkdir(parents=True, exist_ok=True)

    with gzip.open(in_gz, "rt", encoding="utf-8", errors="replace") as fin, \
         gzip.open(out_gz, "wt", encoding="utf-8") as fout:

        for line in fin:
            if line.startswith("#"):
                fout.write(line)
                continue

            parts = line.rstrip("\n").split("\t")
            if len(parts) < 10:
                continue

            n_records += 1
            gt, dp, gq = extract_gt_dp_gq(parts[8], parts[9])

            if gt in (None, ".", "./.", ".|."):
                gt_missing += 1

            if not is_heterozygous(gt):
                continue
            after_gt += 1

            if not dp_passes(dp):
                continue
            after_dp += 1

            if not gq_passes(gq):
                continue
            after_gq += 1

            fout.write(line)

    counts = {
        "N_Records": n_records,
        "GT_Missing": gt_missing,
        "After_GT_Het": after_gt,
        "After_DP": after_dp,
        "After_GQ": after_gq,
        "Het_Count": after_gq,
    }
    return counts


def main(input_root="../input/", output_root="../output/"):
    """
    for each cohort:
        read metadata.tsv
        for each SampleID:
            filter gz file
        write cohort progressive metadata TSV
    """
    in_root = Path(input_root)
    out_root = Path(output_root)
    cohort_dirs = sorted(p for p in in_root.iterdir() if p.is_dir() and p.name.startswith("Cohort_"))
    # Per-Cohort processing
    for cohort in cohort_dirs:
        metadata_path = cohort / "metadata.tsv"
        filtered_dir = out_root / "filtered_gvcf" / cohort.name
        filtered_dir.mkdir(parents=True, exist_ok=True)
        df = pd.read_csv(metadata_path, sep="\t", dtype={"SampleID": str})
        rows = []
        # Per-GZ Sample processing
        for _, r in df.iterrows():
            sid = str(r["SampleID"])
            in_gz = find_sample_gz(cohort, sid)
            out_gz = filtered_dir / f"{sid}.het_dp20_gq30.vcf.gz"
            counts = filter_and_count(in_gz, out_gz)
            rows.append({
                "SampleID": sid,
                "Age": r["Age"],
                "Ancestry": r["Ancestry"],
                "IQ": r["IQ"],
                "Cohort": cohort.name,
                "GZ_File": in_gz.name,
                "Filtered_GZ_File": out_gz.name,
                **counts, # ** to unpack dict
            })
        # Write cohort progressive metadata TSV
        out_df = pd.DataFrame(rows)
        out_tsv = out_root / f"{cohort.name}_progressive_counts.tsv"
        out_df.to_csv(out_tsv, sep="\t", index=False)
    print(f"[OK] Wrote filtered gVCFs to: {output_root}/filtered_gvcf")
    print(f"[OK] Wrote per-cohort metadata TSVs to: {output_root}")
    return 0


if __name__ == "__main__":
    sys.exit(main())