#!/bin/bash
set -e 

if [ $# -ne 1 ] ; then
   echo "Requirements: clone hopshadoop/clusterj-native, also make sure that wagon-ssh-common.jar and wagon-ssh-external.jar are added to maven lib folder"
   echo "Usage: <prog> ndb_version"
   echo "./upgrade-ndb.sh 7.5.7"
   exit 1
fi

V=$1

MAJOR=$(echo $V | cut -d "." -f 1)
MINOR=$(echo $V | cut -d "." -f 2)

TMP=/tmp/mysql-bld
SRC="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

rm -rf $TMP
mkdir $TMP
cd $TMP

#download mysql cluster source code
wget https://dev.mysql.com/get/Downloads/MySQL-Cluster-${MAJOR}.${MINOR}/mysql-cluster-gpl-"$V".tar.gz
tar xvf mysql-cluster-gpl-"$V".tar.gz 
cd mysql-cluster-gpl-"$V"

#apply hops clusterj fix
sed s/VERSION/$V/g $SRC/clusterj-fixes/clusterj-fix.patch > clusterj-fix.patch
patch -p1 < clusterj-fix.patch

#build
BLD=$TMP/mysql-cluster-gpl-"$V"/bld
mkdir $BLD
cd $BLD
cmake .. -DBUILD_CONFIG=mysql_release -DCPACK_MONOLITHIC_INSTALL=true -DDOWNLOAD_BOOST=1 -DWITH_BOOST=/tmp
make -j$(expr $(nproc))

#deploy clusterj to kompics repo
#make sure to add wagon-ssh-common.jar and wagon-ssh-external.jar to your maven lib folder for this step to work
mvn deploy:deploy-file -Dfile=storage/ndb/clusterj/clusterj-"$V".jar -DgroupId=com.mysql.ndb -DartifactId=clusterj-hops-fix -Dversion=$V -Dpackaging=jar -DrepositoryId=sics-release-repository -Durl=scpexe://kompics.i.sics.se/home/maven/repository

#deploy libndbclient to kompics
cd $SRC/../../
if [ ! -d clusterj-native ]; then
  git clone git@github.com:hopshadoop/clusterj-native.git
fi

cd clusterj-native
rm src/main/resources/libndbclient.so*
cp $BLD/storage/ndb/src/libndbclient.so* src/main/resources/
sed -i "0,/<version>.*<\/version>/s//<version>$V<\/version>/g" pom.xml

git commit -am "Upgrade to $V"
git push
mvn deploy
