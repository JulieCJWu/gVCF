# gVCF Cohort Processing Pipeline

This repository implements a workflow for processing multiple cohorts of `gVCF` files together with their corresponding metadata tables.
For each individual, variants are filtered to retain heterozygous sites based on **GT**, **DP**, and **GQ** fields.
The pipeline computes the number of such sites per individual (**Het_Count**) and merges these counts with the associated metadata.

Samples are automatically grouped by cohort.  
One `Parquet` file is generated per cohort containing the final structured dataset with the following columns:

`SampleID, Age, Ancestry, IQ, Cohort, Het_Count`

A **PDF** report is also generated, summarizing the merged cohort results.

## Run the Pipeline

```bash
cd src
./pipeline.sh
```

## Expected Repository Structure

```
project/
├── src/
│   └── pipeline.sh
│
├── input/
│   ├── Cohort_A/
│   │   ├── metadata.tsv
│   │   └── *.gvcf.gz
│   └── Cohort_B/
│       ├── metadata.tsv
│       └── *.gvcf.gz
│
└── output/
    ├── Cohort_A.parquet
    ├── Cohort_B.parquet
    └── report.pdf
```

## Dependencies

```
python>=3.10
pandas>=2.0
pyarrow>=14.0
matplotlib>=3.7
```
