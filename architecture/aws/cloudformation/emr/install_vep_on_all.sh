for SLAVEIP in `sudo grep -i privateip /mnt/var/lib/info/*.txt | sort -u | cut -d "\"" -f 2`
do
   scp -i nick.pem vep.sh hadoop@${SLAVEIP}:/tmp/vep.sh
   ssh -i nick.pem hadoop@${SLAVEIP} screen -d -m "sudo ./tmp/vep.sh"
done