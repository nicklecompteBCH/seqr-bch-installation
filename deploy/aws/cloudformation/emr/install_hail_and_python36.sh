#!/bin/bash
exec 3>&1 4>&2
trap 'exec 2>&4 1>&3' 0 1 2 3
exec 1>/tmp/cloudcreation_log.out 2>&1

set -x

export HAIL_HOME="/opt/hail"
export HASH="current"

# Error message
error_msg ()
{
  echo 1>&2 "Error: $1"
  exit 1
}

# Usage
usage()
{
echo "Usage: cloudformation.sh [-v | --version <git hash>] [-h | --help]

Options:
-v | --version <git hash>
    This option takes either the abbreviated (8-12 characters) or the full size hash (40 characters).
    When provided, the command uses a pre-compiled Hail version for the EMR cluster. If the hash (sha1)
    version exists in the pre-compiled list, that specific hash will be used.
    If no version is given or if the hash was not found, Hail will be compiled from scratch using the most
    up to date version available in the repository (https://github.com/hail-is/hail)

-h | --help
	Displays this menu"
    exit 1
}

# Add hail to the master node
sudo mkdir -p /opt
sudo chmod 777 /opt/
sudo chown hadoop:hadoop /opt
cd /opt
git clone https://github.com/hms-dbmi/hail-on-AWS-spot-instances.git
cd $HAIL_HOME/src

# Compile Hail

echo "Running Hail installation with option: $HASH"
sudo rm -r hail
sudo rm /etc/alternatives/jre/include/include

OUTPUT_PATH=""
HAIL_VERSION="master"
SPARK_VERSION="2.4.3"
COMPILE=true
IS_MASTER=false
GRADLE_DEPRECATION=1566593776
SELECTED_VERSION=$GRADLE_DEPRECATION
export TEST=""
export CXXFLAGS=-march=native

if grep isMaster /mnt/var/lib/info/instance.json | grep true;
then
  IS_MASTER=true
fi

echo "Building Hail from $HASH"

if [ "$IS_MASTER" = true ]; then
    sudo yum install g++ cmake git -y
    sudo yum -y install gcc72-c++ # Fixes issue with c++14 incompatibility in Amazon Linux
    sudo yum install -y lz4 # Fixes issue of missing lz4
    sudo yum install -y lz4-devel
    git clone https://github.com/broadinstitute/hail.git
    cd hail/hail/
    git checkout $HAIL_VERSION
    GIT_HASH="$(git log --pretty=format:"%H" | grep $HASH | cut -f 1 -d ' ')"

    if [ ${#HASH} -lt 7 ]; then
    	if [ $HASH = "current" ]; then
    		echo "Hail will be compiled using the latest repository version available"
    	else
    		echo "The git hash provided has less than 7 characters. The latest version of Hail will be compiled!"
    		# exit 1
    	fi
    else
    	export TEST="$(aws s3 ls s3://hms-dbmi-docs/hail-versions/ | grep $HASH | sed -e 's/^[ \t]*//' | cut -d " " -f 2)"
    	if [ -z "$TEST" ] || [-z "$GIT_HASH" ]; then
    		echo "Hail pre-compiled version not found!"
            echo "Compiling Hail with git hash: $GIT_HASH"
            git reset --hard $GIT_HASH
            SELECTED_VERSION=`git show -s --format=%ct $GIT_HASH`
    	else
    		echo "Hail pre-compiled version found: $TEST"
            aws s3 cp s3://hms-dbmi-docs/hail-versions/$TEST $HOME/ --recursive
            GIT_HASH="$(echo $TEST | cut -d "-" -f 1)"
            git reset --hard $GIT_HASH
            COMPILE=false
    	fi
    fi

    LATEST_JDK=`ls  /usr/lib/jvm/ | grep "java-1.8.0-openjdk-1.8"`
    sudo  ln -s /usr/lib/jvm/$LATEST_JDK/include /etc/alternatives/jre/include
     

    if [ "$COMPILE" = true ]; then
        # Compile with Spark 2.4.0
        if [ $SELECTED_VERSION -ge $GRADLE_DEPRECATION ];then
          echo "Compiling with Wheel..."
          make clean
          make wheel
          HAIL_WHEEL=`ls /opt/hail-on-AWS-spot-instances/src/hail/hail/build/deploy/dist | grep "whl"`
          sudo python3 -m pip install --no-deps /opt/hail-on-AWS-spot-instances/src/hail/hail/build/deploy/dist/$HAIL_WHEEL

      else  ./gradlew -Dspark.version=$SPARK_VERSION -Dbreeze.version=0.13.2 -Dpy4j.version=0.10.6 shadowJar archiveZip
            cp $PWD/build/distributions/hail-python.zip $HOME
            cp $PWD/build/libs/hail-all-spark.jar $HOME
        fi
    fi 
fi

sudo cp /usr/share/zoneinfo/America/New_York /etc/localtime

# Get IPs and names of EC2 instances (workers) to monitor if a worker dropped  
sudo grep -i privateip /mnt/var/lib/info/*.txt | sort -u | cut -d "\"" -f 2 > /tmp/t1.txt
CLUSTERID="$(jq -r .jobFlowId /mnt/var/lib/info/job-flow.json)"
aws emr list-instances --cluster-id ${CLUSTERID} | jq -r .Instances[].Ec2InstanceId > /tmp/ec2list1.txt

# Setup crontab to check dropped instances every minute and install SW as needed in new instances 
sudo echo "* * * * * /opt/hail-on-AWS-spot-instances/src/run_when_new_instance_added.sh >> /tmp/cloudcreation_log.out 2>&1 # min hr dom month dow" | crontab -

# Install user-level python packages
python3 -m pip install boto3 --user
python3 -m pip install hail --user
python3 -m pip install elasticsearch --user

