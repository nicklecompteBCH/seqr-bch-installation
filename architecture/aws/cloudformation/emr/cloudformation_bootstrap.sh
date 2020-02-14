#!/bin/bash

aws s3 cp s3://bch-seqr-deployment/install_vep.sh /tmp/install_vep.sh

chmod 777 /tmp/install_vep.sh

screen -d -m /tmp/install_vep.sh