#!/bin/bash

# install requirement
sudo python2.7 -m pip install --no-cache-dir -U crcmod

# PERL setup
sudo wget https://raw.github.com/miyagawa/cpanminus/master/cpanm -O /usr/bin/cpanm
sudo chmod o+x /usr/bin/cpanm

sudo yum install -y perl-core

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

# EFS dependency
sudo yum install -y amazon-efs-utils
sudo mkdir /vepefs
sudo mount -t efs fs-471ac2c7 /vepefs
sudo mkdir /mnt/vep
sudo cp -vr /vepefs/* /mnt/vep
sudo chown hadoop:hadoop /vep
sudo ln -s /mnt/vep /vep
sudo chmod -R 777 /vep