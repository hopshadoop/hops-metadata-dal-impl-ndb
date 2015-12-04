#!/bin/bash

if [ $# -ne 2 ] ; then
  echo "usage: <prog> hadoop_version ndb_version (e.g., 2.4.0 7.4.6)"
  exit 1
fi

server=glassfish@snurran.sics.se:/var/www/hops

mvn clean install assembly:assembly -DskipTests
scp target/hops-metadata-dal-impl-ndb-1.0-SNAPSHOT-jar-with-dependencies.jar $server/ndb-dal-$1-$2.jar
scp target/classes/libhopsyarn.so $server/libhopsyarn-$1-$2.so
