# 2026-02-13, Chao-Jung Wu
# bash commands for exploring the files

# install pyarrow for supporting paquet
# however, I am not able to install virtual env so I can not create virtual env to install pyarrow.
python3 -m pip install --user pyarrow


zcat file.gz
zcat file.gz | head
zcat file.gz | tail


# What is the difference between the two versions below?
wc -l <(zcat file.gz)
zcat file.gz | wc -l




dir1=../input/Cohort_A
dir2=../input/Cohort_B
file=$dir1/6iegzyVR.gvcf.gz
zcat $file | wc -l
zcat $file | head
zcat $file | tail

#column names: head -n 206
#CHROM  POS     ID      REF     ALT     QUAL    FILTER  INFO    FORMAT  6iegzyVR
#chr1    49272   .       G       A       148.77  PASS    .       GT:AD:DP:GQ     1|0:14,7:21:99


dir1=../input/Cohort_A
file=6iegzyVR.gvcf.gz
zcat $dir1/$file | wc -l
file=flwxQ4S6.gvcf.gz
zcat $dir1/$file | wc -l
file=l1m7WayG.gvcf.gz
zcat $dir1/$file | wc -l

dir2=../input/Cohort_B    
file=TXBZa1Rf.gvcf.gz
zcat $dir2/$file | wc -l
file=p0vXFG9j.gvcf.gz
zcat $dir2/$file | wc -l
file=y9NrF4aH.gvcf.gz
zcat $dir2/$file | wc -l










#terminal command to “see” heterozygous lines
dir1=../input/Cohort_A
file=6iegzyVR.gvcf.gz
zcat $dir1/$file \
  | grep -v '^##' \
  | awk 'BEGIN{FS=OFS="\t"} NR==1 || $10 ~ /^(0[\/|]1|1[\/|]0):/ {print}'



#terminal command to “see” heterozygous lines + DP > 20 + GQ ≥ 30
zcat $dir1/$file \
| awk -F'\t' '
  BEGIN{OFS="\t"}
  /^##/ {next}
  /^#CHROM/ {print; next}
  {
    split($9, fmt, ":")
    split($10, val, ":")

    gt=dp=gq=""
    for (i=1; i<=length(fmt); i++) {
      if (fmt[i]=="GT") gt=val[i]
      else if (fmt[i]=="DP") dp=val[i]
      else if (fmt[i]=="GQ") gq=val[i]
    }

    if (gt ~ /^(0[\/|]1|1[\/|]0)$/ && dp+0 > 20 && gq+0 >= 30) print
  }' \
> output.txt

