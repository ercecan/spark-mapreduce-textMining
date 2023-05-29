for retstart in {0..25000..1000}
do
  esearch -db pubmed -query "cancer[Title]"  -retmax 1000 | efetch -format abstract
done > out_file.txt
