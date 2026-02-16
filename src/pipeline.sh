#!/usr/bin/env bash
# Author: Chao-Jung Wu
# Created: 2026-02-13
#
# Pipeline steps:
#       1) verify.py: Generates gz_index.txt and file_check.tsv from cohort metadata
#       2) filter.py: Filters each gVCF by GT, DP, and GQ;
#					  Writes filtered gz files;
#					  Appends summary lines to log.txt
#   	3) mergeMeta.py: Merges file_check.tsv and log.txt; Writes all_cohorts_progressive_counts.tsv
#          and per-cohort progressive TSV files.
#       4) parquet.py: Converts progressive TSV files to Parquet format
#       5) makepdf.py: Generates a PDF report with boxplots
#
# The pipeline runs sequentially and stops on error (set -e).
#
# Usage:
#   ./pipeline.sh
#   (optional) PYTHON_BIN=python ./pipeline.sh

set -e # exit if a step returns a non-zero status

PYTHON_BIN="${PYTHON_BIN:-python}"

script=verify.py
echo "[INFO] Running $script"
"$PYTHON_BIN" "$script"
echo "[OK] Finished $script"


script=filter_one_GZ.py
INDEX_FILE="../output/gz_index.txt"
OUT_ROOT="../output/filtered_gvcf"
LOG_FILE="../output/log.txt"
while IFS= read -r infile; do
    [[ -z "$infile" ]] && continue
    cohort="$(basename "$(dirname "$infile")")"
    filename="$(basename "$infile")"
    base="${filename%.gz}"
    newname="${base}.het_dp20_gq30.gz"
    outfile="$OUT_ROOT/$cohort/$newname"
    mkdir -p "$OUT_ROOT/$cohort"
    python3 "$script" "$infile" "$outfile" &>> "$LOG_FILE"
done < "$INDEX_FILE"



SCRIPTS=("mergeMeta.py" "parquet.py" "makePDF.py")
for script in "${SCRIPTS[@]}"; do
    echo "[INFO] Running $script"
    "$PYTHON_BIN" "$script"
    echo "[OK] Finished $script"
done

echo "[DONE] All scripts completed successfully."

#Windows:
# dos2unix pipeline.sh | PYTHON_BIN=python3 ./pipeline.sh