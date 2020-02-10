#!/bin/bash

aws s3 cp s3://bch-seqr-deployment/vep_os_dependencies.sh /tmp/vep_os_dependencies.sh

chmod 777 /tmp/vep_os_dependencies.sh

screen -d -m /tmp/vep_os_dependencies.sh