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
sudo mkdir -p /opt/hail/src
sudo chmod -r 777 /opt/
sudo chown -R hadoop:hadoop /opt
cd /opt
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

sudo yum install g++ cmake git -y
sudo yum -y install gcc72-c++ # Fixes issue with c++14 incompatibility in Amazon Linux
sudo yum install -y lz4 # Fixes issue of missing lz4
sudo yum install -y lz4-devel
git clone https://github.com/broadinstitute/hail.git
cd hail/hail/
git checkout $HAIL_VERSION
GIT_HASH="$(git log --pretty=format:"%H" | grep $HASH | cut -f 1 -d ' ')"

LATEST_JDK=`ls  /usr/lib/jvm/ | grep "java-1.8.0-openjdk-1.8"`
sudo  ln -s /usr/lib/jvm/$LATEST_JDK/include /etc/alternatives/jre/include

# Compile with Spark 2.4.0
echo "Compiling with Wheel..."
make clean
make wheel
HAIL_WHEEL=`ls /opt/hail/src/hail/hail/build/deploy/dist | grep "whl"`
sudo python3 -m pip install --no-deps /opt/hail/src/hail/hail/build/deploy/dist/$HAIL_WHEEL

# else  ./gradlew -Dspark.version=$SPARK_VERSION -Dbreeze.version=0.13.2 -Dpy4j.version=0.10.6 shadowJar archiveZip
#     cp $PWD/build/distributions/hail-python.zip $HOME
#     cp $PWD/build/libs/hail-all-spark.jar $HOME
# fi

sudo cp /usr/share/zoneinfo/America/New_York /etc/localtime

# Install user-level python packages
python3 -m pip install boto3 --user
python3 -m pip install elasticsearch --user
