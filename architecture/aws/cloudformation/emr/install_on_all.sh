for SLAVEIP in `sudo grep -i privateip /mnt/var/lib/info/*.txt | sort -u | cut -d "\"" -f 2`
do
   scp -i nick.pem hail.sh hadoop@${SLAVEIP}:/tmp/install_hail_and_python36.sh
   ssh -i nick.pem hadoop@${SLAVEIP} "sudo /tmp/install_hail_and_python36.sh"
   ssh -i nick.pem hadoop@${SLAVEIP} "python3 --version"
done