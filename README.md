Hops Metadata DAL NDB Implementation
===

Hops Database abstraction layer for storing the hops metadata in MySQL Cluster

# How to configure
Refer to the `docs/` directory for information on how to configure this.


How to build
===

```
mvn clean install -DskipTests
```

If you get an error that LIBNDBPATH is not set (or not correct), go to the [Hops](https://github.com/hopshadoop/hops) folder, and then the /target/lib folder. Copy the complete path (find it with pwd), and add it to your .bashrc file:

```
export LIBNDBPATH=<your path here, e.g. /home/user/hops/hops/target/lib>
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$LIBNDBPATH
```

And reload bashrc with:

```
source ~/.bashrc
```

deploys the jar file as an artifact to the kompics maven repository.

```
./deploy.sh
```

Development Notes
===
Updates to schema/schema.sql should be copied to:
hops-hadoop-chef/templates/default/hops.sql.erb

Update to src/main/native/streaming/resources/RM_EventAPIConfig.ini and src/main/native/streaming/resources/RT_EventAPIConfig.ini should be copied to:
hops-hadoop-chef/templates/default/RM_EventAPIConfig.ini.erb and hops-hadoop-chef/templates/default/RT_EventAPIConfig.ini.erb

# License

Hops-Metadata-dal-impl-ndb is released under an [GPL 2.0 license](LICENSE.txt).
