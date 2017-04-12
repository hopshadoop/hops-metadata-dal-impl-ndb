/*
 * Copyright (C) 2016 Hops.io
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */

/* 
 * File:   TableTailer.cpp
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#include "JniNdbEventStreamingImp.h"


JniNdbEventStreamingImp::JniNdbEventStreamingImp(JNIEnv *env,jboolean jIsLeader, jstring jConnectionString,
        jstring jDatabaseName){
  LOG_INFO("start table tailers");
  mPollMaxTimeToWait=100;
  const char *connectionString=env->GetStringUTFChars(jConnectionString, 0);
  mClusterConnection = connect_to_cluster(connectionString);

  const char *databaseName=env->GetStringUTFChars(jDatabaseName, 0);

  JavaVM* jvm;
  env->GetJavaVM(&jvm);

  isLeader=(bool) jIsLeader;
  
  if(isLeader){
    Ndb* rmnode_tailer_connection = create_ndb_connection(databaseName);
    rmNodeTailer = new RMNodeTableTailer(rmnode_tailer_connection, mPollMaxTimeToWait,jvm);
    rmNodeTailer->start();
    
    Ndb* pendingEvent_tailer_connection = create_ndb_connection(databaseName);
    pendingEventTailer = new PendingEventTableTailer(pendingEvent_tailer_connection, mPollMaxTimeToWait,jvm);
    pendingEventTailer->start();
    
    Ndb* resource_tailer_connection = create_ndb_connection(databaseName);
    resourceTailer = new ResourceTableTailer(resource_tailer_connection, mPollMaxTimeToWait,jvm);
    resourceTailer->start();
    
    Ndb* updatedContainerInfo_tailer_connection = create_ndb_connection(databaseName);
    updatedContainerInfoTailer = new UpdatedContainerInfoTableTailer(updatedContainerInfo_tailer_connection, mPollMaxTimeToWait,jvm);
    updatedContainerInfoTailer->start();
    
    Ndb* containerStatus_tailer_connection = create_ndb_connection(databaseName);
    containerStatusTailer = new ContainerStatusTableTailer(containerStatus_tailer_connection, mPollMaxTimeToWait,jvm);
    containerStatusTailer->start();
  }else{
    Ndb* containerIdToClean_tailer_connection = create_ndb_connection(databaseName);
    containerIdToCleanTailer = new ContainerIdToCleanTableTailer(containerIdToClean_tailer_connection, mPollMaxTimeToWait,jvm);
    containerIdToCleanTailer->start();

    Ndb* nextHeartBeat_tailer_connection = create_ndb_connection(databaseName);
    nextHeartBeatTailer = new NextHeartBeatTableTailer(nextHeartBeat_tailer_connection, mPollMaxTimeToWait,jvm);
    nextHeartBeatTailer->start();

    Ndb* finishedApplications_tailer_connection = create_ndb_connection(databaseName);
    finishedApplicationsTailer = new FinishedApplicationsTableTailer(finishedApplications_tailer_connection, mPollMaxTimeToWait,jvm);
    finishedApplicationsTailer->start();
  }
}


JniNdbEventStreamingImp::~JniNdbEventStreamingImp(){
  if(isLeader){
    LOG_INFO("delete tabletailer 1");
    rmNodeTailer->stop();
    LOG_INFO("delete tabletailer 2");
    pendingEventTailer->stop();
    LOG_INFO("delete tabletailer 3");
    resourceTailer->stop();
    LOG_INFO("delete tabletailer 4");
    updatedContainerInfoTailer->stop();
    LOG_INFO("delete tabletailer 5");
    containerStatusTailer->stop();
  }else{
    containerIdToCleanTailer->stop();
    nextHeartBeatTailer->stop();
    finishedApplicationsTailer->stop();
  }
  LOG_INFO("delete tabletailer 6");
  delete mClusterConnection;
  LOG_INFO("delete tabletailer 7");
  ndb_end(0);
}

Ndb* JniNdbEventStreamingImp::create_ndb_connection(const char* database) {
    Ndb* ndb = new Ndb(mClusterConnection, database);
    if (ndb->init() == -1) {

        LOG_NDB_API_ERROR(ndb->getNdbError());
    }

    return ndb;
}


Ndb_cluster_connection* JniNdbEventStreamingImp::connect_to_cluster(const char *connection_string) {
    Ndb_cluster_connection* c;

    if (ndb_init())
        exit(EXIT_FAILURE);

    c = new Ndb_cluster_connection(connection_string);

    if (c->connect(RETRIES, DELAY_BETWEEN_RETRIES, VERBOSE)) {
        fprintf(stderr, "Unable to connect to cluster.\n\n");
        exit(EXIT_FAILURE);
    }

    if (c->wait_until_ready(WAIT_UNTIL_READY, WAIT_UNTIL_READY) < 0) {

        fprintf(stderr, "Cluster was not ready.\n\n");
        exit(EXIT_FAILURE);
    }

    return c;
}
