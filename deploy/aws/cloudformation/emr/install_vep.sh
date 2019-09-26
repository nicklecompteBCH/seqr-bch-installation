#!/bin/bash

# Install gcloud utils
# TODO: Figure out how to load google key here.
export CLOUDSDK_PYTHON=/usr/bin/python3
curl https://sdk.cloud.google.com | bash

gcloud init


# PERL setup
sudo wget https://raw.github.com/miyagawa/cpanminus/master/cpanm -O /usr/bin/cpanm 
sudo chmod o+x /usr/bin/cpanm
# VEP dependencies
sudo /usr/bin/cpanm --notest Set::IntervalTree
sudo /usr/bin/cpanm --notest PerlIO::gzip
sudo /usr/bin/cpanm --notest DBI
sudo /usr/bin/cpanm --notest CGI
sudo /usr/bin/cpanm --notest JSON
sudo /usr/bin/cpanm --notest  Compress::Zlib
# LoFTEE dependencies
sudo /usr/bin/cpanm --notest DBD::SQLite
sudo /usr/bin/cpanm --notest  List::MoreUtils


# More VEP dependencies
cd $HOME
mkdir vep
cd vep
wget https://bch-seqr-deployment.s3.amazonaws.com/vep85-loftee-gcloud.json


hdfs dfs -put vep85-loftee-gcloud.json /hail-common/vep/vep/vep85-loftee-gcloud.json

# # Download large files for Loftee into extra EBS mount
cd /mnt
wget https://personal.broadinstitute.org/konradk/loftee_data/GRCh37/phylocsf_gerp.sql
aws s3 cp s3://seqr-resources/GRCh37/loftee/human_ancestor.fa.gz .
aws s3 cp s3://seqr-resources/GRCh37/loftee/GERP_scores.final.sorted.txt.gz .

# install requirement

sudo su
python2.7 -m pip install --no-cache-dir -U crcmod
exit


# Copy VEP
sudo mkdir -p /mnt/vep/homo_sapiens
sudo chown hadoop -R /mnt/vep/
sudo chmod 777 /mnt/vep/* -R
sudo ln -s /mnt/vep /vep
gsutil -m cp -r gs://hail-common/vep/vep/loftee /vep
gsutil -m cp -r gs://hail-common/vep/vep/ensembl-tools-release-85 /vep
gsutil -m cp -r gs://hail-common/vep/vep/loftee_data /vep
gsutil -m cp -r gs://hail-common/vep/vep/Plugins /vep
gsutil -m cp -r gs://hail-common/vep/vep/homo_sapiens/85_GRCh37 /vep/homo_sapiens
gsutil cp gs://hail-common/vep/vep/vep85-gcloud.properties /vep/vep-gcloud.properties

# Create VEP cache
mkdir $HOME/.vep
ln -s /mnt/vep/homo_sapiens $HOME/.vep/
sudo chmod 777 $HOME/.vep/homo_sapiens/

#Give perms
sudo chmod -R 777 /vep

# Copy perl JSON module
#gsutil -m cp -r gs://hail-common/vep/perl-JSON/* /usr/share/perl5/

#Copy perl DBD::SQLite module
#gsutil -m cp -r gs://hail-common/vep/perl-SQLITE/* /usr/share/perl5/


# Copy htslib and samtools
mkdir $HOME/htslib
gsutil cp gs://hail-common/vep/htslib/* $HOME/htslib
sudo cp $HOME/htslib/* /usr/bin
mkdir $HOME/samtools
gsutil cp gs://hail-common/vep/samtools $HOME/samtools
sudo cp $HOME/samtools/* /usr/bin

sudo chmod a+rx  /usr/bin/tabix
sudo chmod a+rx  /usr/bin/bgzip
sudo chmod a+rx  /usr/bin/htsfile
sudo chmod a+rx  /usr/bin/samtools

#Run VEP on the 1-variant VCF to create fasta.index file -- caution do not make fasta.index file writeable afterwards!
gsutil cp gs://hail-common/vep/vep/1var.vcf /vep
gsutil cp gs://hail-common/vep/vep/run_hail_vep85_vcf.sh /vep
chmod a+rx /vep/run_hail_vep85_vcf.sh

/vep/run_hail_vep85_vcf.sh /vep/1var.vcf