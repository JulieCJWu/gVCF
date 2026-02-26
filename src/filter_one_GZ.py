#!/usr/bin/env python3
"""
process_1gz.py

Filter ONE gVCF/VCF .gz by:
  - heterozygous phased GT: 0|1 or 1|0
  - DP > 20
  - GQ >= 30

Keeps all header lines.

Usage:
  ./process_1gz.py IN_GZ OUT_GZ

Example:
  ./process_1gz.py ../input/Cohort_A/001.g.vcf.gz \
                   ../output/filtered_gvcf/Cohort_A/001.het_dp20_gq30.vcf.gz
"""

import gzip
import sys
from pathlib import Path


# ----------------------------
# Criterion helpers
# ----------------------------
"""
#CHROM  POS     ID      REF     ALT     QUAL    FILTER  INFO    FORMAT  6iegzyVR
chr1    49272   .       G       A       148.77  PASS    .       GT:AD:DP:GQ     1|0:14,7:21:99
chr1    732021  .       C       T       26.78   PASS    .       GT:AD:DP:GQ     0|1:15,3:18:55
chr1    873542  .       G       A       659.77  PASS    .       GT:AD:DP:GQ     1|1:0,16:16:48
"""
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
# VCF helpers
# ----------------------------

def extract_gt_dp_gq(fmt, sample):
    """Given FORMAT (col 9) and sample column (col 10), extract GT, DP, GQ."""
    keys = fmt.split(":")
    vals = sample.split(":")
    m = dict(zip(keys, vals))
    return m.get("GT"), m.get("DP"), m.get("GQ")


# ----------------------------
# Filter + progressive counting
# ----------------------------

def filter_and_count(in_gz, out_gz):
    """
    Writes a filtered gz VCF (keeps headers) with variants passing:
      - heterozygous GT
      - DP > 20
      - GQ >= 30

    Returns progressive counts.
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

    return {
        "N_Records": n_records,
        "GT_Missing": gt_missing,
        "After_GT_Het": after_gt,
        "After_DP": after_dp,
        "After_GQ": after_gq,
        "Het_Count": after_gq,
    }


def usage_and_exit():
    prog = Path(sys.argv[0]).name
    msg = (
        "Usage:\n"
        f"  {prog} IN_GZ OUT_GZ\n\n"
        "Example:\n"
        f"  {prog} ../input/Cohort_A/001.g.vcf.gz "
        "../output/filtered_gvcf/Cohort_A/001.het_dp20_gq30.vcf.gz\n"
    )
    print(msg, file=sys.stderr)
    sys.exit(1)


def main():
    if len(sys.argv) != 3:
        usage_and_exit()

    in_gz = Path(sys.argv[1])
    out_gz = Path(sys.argv[2])

    if not in_gz.exists():
        print(f"[ERROR] Input not found: {in_gz}", file=sys.stderr)
        return 2

    try:
        counts = filter_and_count(in_gz, out_gz)
    except OSError as e:
        print(f"[ERROR] I/O failure: {e}", file=sys.stderr)
        return 3

    # concise summary for logs
    print(
        "[OK] "
        f"in={in_gz.name} out={out_gz.name} "
        f"N_Records={counts['N_Records']} "
        f"After_GT_Het={counts['After_GT_Het']} "
        f"After_DP={counts['After_DP']} "
        f"After_GQ={counts['After_GQ']} "
        f"Het_Count={counts['Het_Count']}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""
file=../input/Cohort_A/6iegzyVR.gvcf.gz
outfile=../output/filtered_gvcf/Cohort_A/6iegzyVR.gvcf.gz
python3 filter_one_GZ.py $file $outfile
"""

    
