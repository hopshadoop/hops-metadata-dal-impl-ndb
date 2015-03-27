#!/bin/bash

if [ $# -ne 2 ] ; then
  echo "usage: <prog> hadoop_version ndb_version (e.g., 2.4.0 7.4.3)"
  exit 1
fi
mvn clean assembly:assembly
scp target/hops-metadata-dal-impl-ndb-1.0-SNAPSHOT-jar-with-dependencies.jar glassfish@snurran.sics.se:/var/www/hops/ndb-dal-$1-$2.jar

