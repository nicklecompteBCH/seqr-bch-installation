#!/bin/bash

# install requirement
sudo python2.7 -m pip install --no-cache-dir -U crcmod

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

# Copy VEP
sudo mkdir -p /mnt/vep/homo_sapiens
sudo chown hadoop -R /mnt/vep/
sudo chmod 777 /mnt/vep/* -R
sudo ln -s /mnt/vep /vep
sudo mkdir /vep/loftee
aws s3 cp s3://seqr-resources/vep/loftee /vep/loftee --recursive
sudo mkdir /vep/ensembl_tools_release_85
aws s3 cp s3://seqr-resources/vep/ensembl-tools-release-85/ /vep/ensembl-tools-release-85 --recursive
sudo mkdir /vep/loftee_data
aws s3 cp s3://seqr-resources/GRCh37/loftee/ /vep/loftee_data --recursive
sudo mkdir /vep/Plugins
aws s3 cp s3://seqr-resources/vep/Plugins /vep/Plugins --recursive
sudo mkdir /vep/homo_sapiens
aws s3 cp s3://seqr-resources/homo_sapiens/ /vep/homo_sapiens --recursive
aws s3 cp s3://seqr-resources/vep/vep-gcloud.properties /vep/vep-gcloud.properties

# Create VEP cache
mkdir $HOME/.vep
ln -s /mnt/vep/homo_sapiens $HOME/.vep/
sudo chmod 777 $HOME/.vep/homo_sapiens/

#Give perms
sudo chmod -R 777 /vep


# Copy htslib and samtools
mkdir $HOME/htslib
aws s3 cp s3://seqr-resources/vep/htslib/ $HOME/htslib/ --recursive
sudo cp $HOME/htslib/* /usr/bin
mkdir $HOME/samtools
aws s3 cp s3://seqr-resources/vep/samtools $HOME/samtools/
sudo cp $HOME/samtools/* /usr/bin

sudo chmod a+rx  /usr/bin/tabix
sudo chmod a+rx  /usr/bin/bgzip
sudo chmod a+rx  /usr/bin/htsfile
sudo chmod a+rx  /usr/bin/samtools

# Make symlink hail uses everywhere :(
ln -s /vep/ensembl-tools-release-85/scripts/variant_effect_predictor /vep/variant_effect_predictor

#Run VEP on the 1-variant VCF to create fasta.index file -- caution do not make fasta.index file writeable afterwards!
aws s3 cp s3://seqr-resources/vep/1var.vcf /vep
aws s3 cp s3://seqr-resources/vep/run_hail_vep85_vcf.sh /vep
chmod a+rx /vep/run_hail_vep85_vcf.sh

/vep/run_hail_vep85_vcf.sh /vep/1var.vcf