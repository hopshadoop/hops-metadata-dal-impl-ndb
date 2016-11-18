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

#include "ContainerStatusTableTailer.h"

using namespace Utils::NdbC;

const string _containerStatus_table= "yarn_containerstatus";
const int _containerStatus_noCols= 7;
const string _containerStatus_cols[_containerStatus_noCols]=
    {"containerid",
     "rmnodeid",
     "state",
     "diagnostics",
     "exitstatus",
     "uciid",
     "pendingeventid"
    };

const int _containerStatus_noEvents = 2; 
const NdbDictionary::Event::TableEvent _containerStatus_events[_containerStatus_noEvents] = 
    { NdbDictionary::Event::TE_INSERT, 
      NdbDictionary::Event::TE_UPDATE
    };

const WatchTable ContainerStatusTableTailer::TABLE = {_containerStatus_table, _containerStatus_cols, _containerStatus_noCols , _containerStatus_events, _containerStatus_noEvents};

const int CONTAINER_ID = 0;
const int RMNODE_ID = 1;
const int STATE = 2;
const int DIAGNOSTICS = 3;
const int EXIT_STATUS = 4;
const int UCIID = 5;
const int PENDING_EVENT_ID = 6;

ContainerStatusTableTailer::ContainerStatusTableTailer(Ndb* ndb, const int poll_maxTimeToWait,JavaVM* jvm) 
  : TableTailer(ndb, TABLE, poll_maxTimeToWait, jvm){
  
  
}

void ContainerStatusTableTailer::handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]) {
  if(!attached){
    if (jvm->AttachCurrentThread((void**)&env, NULL)!= 0) {
      std::cout << "Failed to attach" << std::endl;
    }
    cls = env->FindClass("io/hops/streaming/ContainerStatusEventReceiver");
    jmethodID midInit = env->GetMethodID( cls, "<init>", "()V");
    containerStatusEventReceiver = env->NewObject( cls, midInit);
  
    midCreateAndAddToQueue = env->GetMethodID(cls, "createAndAddToQueue","(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;III)V");
    attached = true;
  }
  //get the values
  jstring containerId = env->NewStringUTF(get_string(value[CONTAINER_ID]).c_str());
  jstring rmnodeId = env->NewStringUTF(get_string(value[RMNODE_ID]).c_str());
  jstring state = env->NewStringUTF(get_string(value[STATE]).c_str());
  jstring diagnostics = env->NewStringUTF(get_string(value[DIAGNOSTICS]).c_str());
  int exitStatus = value[EXIT_STATUS]->int32_value();
  int uciId = value[UCIID]->int32_value();
  int pendingEventId = value[PENDING_EVENT_ID]->int32_value();
  //create the rmnode object and put it in the event queue
    LOG_INFO("create event");
    env->CallVoidMethod(containerStatusEventReceiver,midCreateAndAddToQueue,containerId, rmnodeId, state,diagnostics,exitStatus,uciId,pendingEventId);
  env->DeleteLocalRef(rmnodeId);
  env->DeleteLocalRef(containerId);
  env->DeleteLocalRef(state);
  env->DeleteLocalRef(diagnostics);
   
}

ContainerStatusTableTailer::~ContainerStatusTableTailer() {
}

