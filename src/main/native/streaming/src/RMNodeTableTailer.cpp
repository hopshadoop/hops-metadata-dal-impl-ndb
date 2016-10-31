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
 * File:   DatasetTableTailer.cpp
 * Author: Mahmoud Ismail<maism@kth.se>
 * 
 */

#include "RMNodeTableTailer.h"

using namespace Utils::NdbC;

const string _rmnode_table= "yarn_rmnode";
const int _rmnode_noCols= 9;
const string _rmnode_cols[_rmnode_noCols]=
    {"rmnodeid",
     "hostname",
     "commandport",
     "httpport",
     "healthreport",
     "lasthealthreporttime",
     "currentstate",
     "nodemanager_version",
     "pendingeventid"
    };

const int _rmnode_noEvents = 2; 
const NdbDictionary::Event::TableEvent _rmnode_events[_rmnode_noEvents] = 
    { NdbDictionary::Event::TE_INSERT, 
      NdbDictionary::Event::TE_UPDATE
    };

const WatchTable RMNodeTableTailer::TABLE = {_rmnode_table, _rmnode_cols, _rmnode_noCols , _rmnode_events, _rmnode_noEvents};

const int RMNODE_ID = 0;
const int HOST_NAME = 1;
const int COMMAND_PORT = 2;
const int HTTP_PORT = 3;
const int HEALTH_REPORT = 4;
const int LAST_HEALTH_REPORT_TIME = 5;
const int CURRENT_STATE = 6;
const int NODEMANAGER_VERSION = 7;
const int PENDING_EVENT_ID = 8;

RMNodeTableTailer::RMNodeTableTailer(Ndb* ndb, const int poll_maxTimeToWait,JavaVM* jvm) 
  : TableTailer(ndb, TABLE, poll_maxTimeToWait, jvm){
  
  
}

void RMNodeTableTailer::handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]) {
  if(!attached){
    if (jvm->AttachCurrentThread((void**)&env, NULL)!= 0) {
      std::cout << "Failed to attach" << std::endl;
    }
    cls = env->FindClass("io/hops/streaming/RMNodeEventReceiver");
    jmethodID midInit = env->GetMethodID( cls, "<init>", "()V");
    rmNodeEventReceiver = env->NewObject( cls, midInit);
  
    midCreateAndAddToQueue = env->GetMethodID(cls, "createAndAddToQueue","(Ljava/lang/String;Ljava/lang/String;IILjava/lang/String;JLjava/lang/String;Ljava/lang/String;I)V");
    attached = true;
  }
  //get the values
  jstring rmnodeId = env->NewStringUTF(get_string(value[RMNODE_ID]).c_str());
  jstring hostName = env->NewStringUTF(get_string(value[HOST_NAME]).c_str());
  int commandPort = value[COMMAND_PORT]->int32_value();
  int httpPort = value[HTTP_PORT]->int32_value();
  jstring healthReport = env->NewStringUTF(get_string(value[HEALTH_REPORT]).c_str());
  long lastHealthReportTime = value[LAST_HEALTH_REPORT_TIME]->int64_value();
  jstring currentState = env->NewStringUTF(get_string(value[CURRENT_STATE]).c_str());
  jstring nodeManagerVersion = env->NewStringUTF(get_string(value[NODEMANAGER_VERSION]).c_str());
  int pendingEventId = value[PENDING_EVENT_ID]->int32_value();
  //create the rmnode object and put it in the event queue
  LOG_INFO("create event");
  env->CallVoidMethod(rmNodeEventReceiver,midCreateAndAddToQueue,rmnodeId, hostName,commandPort,httpPort,healthReport,lastHealthReportTime,currentState,nodeManagerVersion,pendingEventId);
  env->DeleteLocalRef(rmnodeId);
  env->DeleteLocalRef(hostName);
  env->DeleteLocalRef(healthReport);
  env->DeleteLocalRef(currentState);
  env->DeleteLocalRef(nodeManagerVersion);
   
}

RMNodeTableTailer::~RMNodeTableTailer() {
  LOG_INFO("delete rmnode table tailer");
}

