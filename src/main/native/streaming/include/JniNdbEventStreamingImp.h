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


#ifndef _Included_NdbEventStreamingImp
#define _Included_NdbEventStreamingImp

#include "NdbApi.hpp"
#include "RMNodeTableTailer.h"
#include "PendingEventTableTailer.h"
#include "ResourceTableTailer.h"
#include "UpdatedContainerInfoTableTailer.h"
#include "ContainerStatusTableTailer.h"
#include "ContainerIdToCleanTableTailer.h"
#include "ContainerToSignalTableTailer.h"
#include "ContainerToDecreaseTableTailer.h"
#include "NextHeartBeatTableTailer.h"
#include "RmNodeApplicationsTableTailer.h"

class JniNdbEventStreamingImp {
 public:
  JniNdbEventStreamingImp(JNIEnv *env,jboolean jIsLeader, jstring jConnectionString,
			  jstring jDatabaseName);
  JniNdbEventStreamingImp();
  virtual ~JniNdbEventStreamingImp();
 private:
  int mPollMaxTimeToWait;
  bool isLeader;
  
  Ndb_cluster_connection *mClusterConnection;
  RMNodeTableTailer *rmNodeTailer;
  PendingEventTableTailer *pendingEventTailer;
  ResourceTableTailer *resourceTailer;
  UpdatedContainerInfoTableTailer *updatedContainerInfoTailer;
  ContainerStatusTableTailer *containerStatusTailer;
  ContainerIdToCleanTableTailer *containerIdToCleanTailer;
  ContainerToSignalTableTailer *containerToSignalTailer;
  ContainerToDecreaseTableTailer *containerToDecreaseTailer;
  NextHeartBeatTableTailer *nextHeartBeatTailer;
  RmNodeApplicationsTableTailer *rmNodeApplicationsTailer;
  
  Ndb* create_ndb_connection(const char* database);
  Ndb_cluster_connection* connect_to_cluster(const char *connection_string);
};


#endif
