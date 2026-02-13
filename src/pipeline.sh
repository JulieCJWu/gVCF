#!/usr/bin/env bash
# Author: Chao-Jung Wu
# Created: 2026-02-13
# The pipeline sequentially executes:
#       1) verify.py: Reads cohort metadata, matches SampleID to .gz files, and computes per-file line counts.
#       2) filter.py: Filters gVCF files based on GT, DP, and GQ;
#                     writes filtered VCFs, and outputs progressive per-sample counts merged with metadata.
#       3) parquet.py: Converts per-cohort progressive TSVs to Parquet format, retaining only the required columns.
#       4) makepdf.py: Generates a PDF report with summary visualizations (e.g., boxplots of Age and Het_Count) from the merged cohort data.
#
#   Each stage is executed in order, and the pipeline stops immediately if
#   any step fails (set -e). The Python interpreter can be specified via
#   the PYTHON_BIN environment variable (default: python3).
#
# Usage:
#   ./pipeline.sh

set -e

PYTHON_BIN="${PYTHON_BIN:-python3}"

SCRIPTS=("verify.py" "filter.py" "parquet.py" "makePDF.py")

for script in "${SCRIPTS[@]}"; do
    echo "[INFO] Running $script"
    "$PYTHON_BIN" "$script"
    echo "[OK] Finished $script"
done

echo "[DONE] All scripts completed successfully."

#how to run:
# dos2unix pipeline.sh | ./pipeline.sh
#(optional) PYTHON_BIN=python3 ./pipeline.sh