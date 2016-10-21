#!/bin/bash

set -e

if [ $# -ne 2 ] ; then
  echo "usage: <prog> hadoop_version ndb_version (e.g., 2.4.0 7.4.6)"
  exit 1
fi

if [ ! -f /usr/local/mysql/lib/libndbclient.so ] ; then

  echo "Error!"
  echo "You need to have mysql cluster installed in /usr/local/mysql to deploy binaries"
  echo "Exiting.."
  exit 1
fi

export LIBNDBPATH=/usr/local/mysql/lib

VERSION=`grep -o -a -m 1 -h -r "version>.*</version" pom.xml | head -1 | sed "s/version//g" | sed "s/>//" | sed "s/<\///g"`


server=glassfish@snurran.sics.se:/var/www/hops

mvn clean install assembly:assembly -DskipTests

echo "Deploying Hops - NDB connector...."
scp target/hops-metadata-dal-impl-ndb-${VERSION}-jar-with-dependencies.jar $server/ndb-dal-$1-$2.jar

echo "Deploying HopsYARN native libraries...."
scp target/classes/libhopsyarn.so $server/libhopsyarn-$1-$2.so

echo "Deploying Hops Schema...."
scp schema/schema.sql $server/hops.sql

