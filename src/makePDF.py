#!/usr/bin/env python3
"""
Chao-Jung Wu
2026-02-13

Generate a PDF report from per-cohort Parquet files.

The script reads all .parquet files in a directory (one per cohort),
concatenates them, and produces:

- Cohort-wise numeric summaries
- Boxplots (one box per cohort)
- Scatter plots colored by cohort

Behavior:
- Supports any number of cohorts
- Columns to summarize and plot are hard-coded via NUM_COLS

Input:
  ../output/Cohort_*.parquet

Output:
  ../output/report.pdf
"""
import sys
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

NUM_COLS = ["Age", "IQ", "Het_Count"]


def read_parquets_from_dir(parquet_dir):
    parquet_dir = Path(parquet_dir)

    if not parquet_dir.exists():
        raise FileNotFoundError("Input directory not found: %s" % parquet_dir)

    files = sorted(parquet_dir.glob("*.parquet"))
    if not files:
        raise FileNotFoundError("No .parquet files found in: %s" % parquet_dir)

    dfs = [pd.read_parquet(fp) for fp in files]
    return pd.concat(dfs, ignore_index=True, sort=False)


def coerce_numeric(df, cols):
    df = df.copy()
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def render_text_page(pdf, lines, title=None):
    fig = plt.figure(figsize=(8.27, 11.69))  # A4 portrait
    ax = fig.add_axes([0.08, 0.06, 0.84, 0.90])
    ax.axis("off")

    if title:
        content = [title, ""] + lines
    else:
        content = lines

    ax.text(
        0.0,
        1.0,
        "\n".join(content),
        va="top",
        ha="left",
        fontsize=10,
        family="monospace",
    )

    pdf.savefig(fig)
    plt.close(fig)


def add_summary_pages(pdf, df, input_dir):
    if "Cohort" not in df.columns:
        render_text_page(pdf, ["ERROR: 'Cohort' column not found."], "Summary Report")
        return

    cohort_str = df["Cohort"].astype(str)
    cohorts = sorted(cohort_str.unique())

    n_rows = len(df)
    n_cols = df.shape[1]
    n_samples = df["SampleID"].nunique() if "SampleID" in df.columns else None
    n_cohorts = len(cohorts)

    max_lines = 70 #maximum number of text lines allowed per PDF page
    header = [
        "Input directory of .parquet: %s" % input_dir,
        "",
        "Total Rows: %d" % n_rows,
        "Total Columns: %d" % n_cols,
    ]

    if n_samples is not None:
        header.append("Unique SampleID: %d" % n_samples)

    header.append("Number of Cohorts: %d" % n_cohorts)
    header.extend(["", "Numeric Summary by Cohort", "=" * 40, ""])

    pages = []
    current = list(header)
    for cohort in cohorts:
        subset = df[cohort_str == cohort]
        block = []
        block.append("Cohort: %s" % cohort)
        block.append("-" * 30)

        for var in NUM_COLS:
            if var not in subset.columns:
                continue

            s = pd.to_numeric(subset[var], errors="coerce").dropna()
            if s.empty:
                continue

            block.extend([
                "%s:" % var,
                "  n     = %d" % len(s),
                "  mean  = %.2f" % s.mean(),
                "  std   = %.2f" % s.std(),
                "  min   = %.2f" % s.min(),
                "  25%%   = %.2f" % s.quantile(0.25),
                "  50%%   = %.2f" % s.median(),
                "  75%%   = %.2f" % s.quantile(0.75),
                "  max   = %.2f" % s.max(),
                "",
            ])

        block.append("")

        if len(current) + len(block) > max_lines:
            pages.append(current)
            current = ["Numeric Summary by Cohort (continued)", "=" * 40, ""]
        current.extend(block)

    pages.append(current)

    for i, page in enumerate(pages):
        if i == 0:
            title = "Merged Cohort Summary Report"
        else:
            title = "Merged Cohort Summary (page %d)" % (i + 1)
        render_text_page(pdf, page, title)


def page_boxplot_by_cohort(pdf, df, ycol):
    if "Cohort" not in df.columns or ycol not in df.columns:
        return

    d = df[["Cohort", ycol]].dropna()
    if d.empty:
        return

    cohort_str = d["Cohort"].astype(str)
    cohorts = sorted(cohort_str.unique())
    data = [d[cohort_str == c][ycol].values for c in cohorts]

    fig = plt.figure(figsize=(11.69, 8.27))
    ax = fig.add_subplot(111)

    ax.boxplot(data, labels=cohorts, showfliers=True)
    ax.set_title("%s by Cohort" % ycol)
    ax.set_xlabel("Cohort")
    ax.set_ylabel(ycol)
    ax.tick_params(axis="x", labelrotation=45)

    fig.tight_layout()
    pdf.savefig(fig)
    plt.close(fig)

def page_scatter_by_cohort(pdf, df, x, y, title):
    if "Cohort" not in df.columns:
        return
    if x not in df.columns or y not in df.columns:
        return

    d = df[["Cohort", x, y]].dropna()
    if d.empty:
        return

    cohort_str = d["Cohort"].astype(str)
    cohorts = sorted(cohort_str.unique())

    cmap = plt.get_cmap("tab10")
    colors = cmap.colors  # tuple of RGB colors

    fig = plt.figure(figsize=(11.69, 8.27))
    ax = fig.add_subplot(111)

    for i, cohort in enumerate(cohorts):
        sub = d[cohort_str == cohort]
        ax.scatter(
            sub[x],
            sub[y],
            s=30,
            label=cohort,
            color=colors[i % len(colors)],
            alpha=0.85,
            edgecolors="black",
            linewidth=0.3,
        )

    ax.set_xlabel(x)
    ax.set_ylabel(y)
    ax.set_title(title)
    ax.legend(title="Cohort", fontsize=9)

    fig.tight_layout()
    pdf.savefig(fig)
    plt.close(fig)


def main(input_dir="../output/", output_pdf="../output/report.pdf"):
    input_dir = Path(input_dir)
    output_pdf = Path(output_pdf)
    output_pdf.parent.mkdir(parents=True, exist_ok=True)

    df = read_parquets_from_dir(input_dir)
    df = coerce_numeric(df, NUM_COLS)

    with PdfPages(output_pdf) as pdf:
        add_summary_pages(pdf, df, input_dir)

        for col in NUM_COLS:
            if col in df.columns:
                page_boxplot_by_cohort(pdf, df, col)

        page_scatter_by_cohort(pdf, df, "Age", "Het_Count", "Het_Count vs Age")
        page_scatter_by_cohort(pdf, df, "IQ", "Het_Count", "Het_Count vs IQ")

    print("[OK] Wrote PDF report to:", output_pdf)


if __name__ == "__main__":
    sys.exit(main())