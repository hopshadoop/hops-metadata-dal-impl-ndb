#!/bin/bash

if [ $# -ne 1 ] ; then
 echo "usage: $0 hop-dal-version"
 exit 1
fi


version=$1
proj=${PWD##*/}
mvn  deploy:deploy-file -Durl=scpexe://kompics.i.sics.se/home/maven/repository \
                       -DrepositoryId=sics-release-repository \
                       -Dfile=./target/$proj-$version.jar \
                       -DgroupId=io.hops.metadata.$proj \
                       -DartifactId=$proj \
                       -Dversion=$version \
                       -Dpackaging=jar \
                       -DpomFile=./pom.xml \
                       -DgeneratePom.description="Hops DAL Interface" \

scp target/hops-metadata-dal-impl-ndb-1.0-SNAPSHOT.jar glassfish@snurran.sics.se:/var/www/hops/ndb-dal-2.4.0.jar
