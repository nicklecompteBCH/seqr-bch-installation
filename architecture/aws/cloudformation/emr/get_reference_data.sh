aws s3 cp s3://seqr-resources/GRCh37/eigen/EIGEN_coding_noncoding.grch37.vcf.gz /tmp/eigen/EIGEN_coding_noncoding.grch37.vcf.gz

aws s3 cp s3://seqr-resources/GRCh37/CADD/whole_genome_SNVs.v1.4.tsv.bgz /tmp/CADD/whole_genome_SNVs.v1.4.tsv.bgz

aws s3 cp s3://seqr-resources/GRCh37/CADD/InDels.v1.4.tsv.bgz /tmp/CADD/InDels.v1.4.tsv.bgz

aws s3 cp s3://seqr-resources/GRCh37/primate_ai/PrimateAI_scores_v0.2.vcf.gz /tmp/primate_ai/PrimateAI_scores_v0.2.vcf.gz

aws s3 cp s3://seqr-resources/gnomad/37/* /tmp/gnomad/ --recursive

aws s3 cp s3://seqr-resources