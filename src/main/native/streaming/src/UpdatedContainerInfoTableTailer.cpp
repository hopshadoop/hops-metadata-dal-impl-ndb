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

#include "UpdatedContainerInfoTableTailer.h"

using namespace Utils::NdbC;

const string _updatedContainerInfo_table= "yarn_updatedcontainerinfo";
const int _updatedContainerInfo_noCols= 4;
const string _updatedContainerInfo_cols[_updatedContainerInfo_noCols]=
    {"rmnodeid",
     "containerid",
     "updatedcontainerinfoid",
     "pendingeventid",
    };

const int _updatedContainerInfo_noEvents = 1; 
const NdbDictionary::Event::TableEvent _updatedContainerInfo_events[_updatedContainerInfo_noEvents] = 
    { NdbDictionary::Event::TE_INSERT
    };

const WatchTable UpdatedContainerInfoTableTailer::TABLE = {_updatedContainerInfo_table, _updatedContainerInfo_cols, _updatedContainerInfo_noCols , _updatedContainerInfo_events, _updatedContainerInfo_noEvents};

const int RMNODE_ID = 0;
const int CONTAINER_ID = 1;
const int UPDATED_CONTAINER_INFO_ID = 2;
const int PENDING_EVENT_ID = 3;

UpdatedContainerInfoTableTailer::UpdatedContainerInfoTableTailer(Ndb* ndb, const int poll_maxTimeToWait,JavaVM* jvm) 
  : TableTailer(ndb, TABLE, poll_maxTimeToWait, jvm){
  
  
}

void UpdatedContainerInfoTableTailer::handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]) {
  if(!attached){
    if (jvm->AttachCurrentThread((void**)&env, NULL)!= 0) {
      std::cout << "Failed to attach" << std::endl;
    }
    cls = env->FindClass("io/hops/streaming/UpdatedContainerInfoEventReceiver");
    jmethodID midInit = env->GetMethodID( cls, "<init>", "()V");
    updatedContainerInfoEventReceiver = env->NewObject( cls, midInit);
  
    midCreateAndAddToQueue = env->GetMethodID(cls, "createAndAddToQueue","(Ljava/lang/String;Ljava/lang/String;II)V");
    attached = true;
  }
  //get the values
  jstring rmnodeId = env->NewStringUTF(get_string(value[RMNODE_ID]).c_str());
  jstring containerId = env->NewStringUTF(get_string(value[CONTAINER_ID]).c_str());
  int updatedContainerInfoId = value[UPDATED_CONTAINER_INFO_ID]->int32_value();
  int pendingEventId = value[PENDING_EVENT_ID]->int32_value();
  //create the rmnode object and put it in the event queue
    LOG_INFO("create event");
    env->CallVoidMethod(updatedContainerInfoEventReceiver,midCreateAndAddToQueue,rmnodeId, containerId,updatedContainerInfoId,pendingEventId);
  env->DeleteLocalRef(rmnodeId);
  env->DeleteLocalRef(containerId);
   
}

UpdatedContainerInfoTableTailer::~UpdatedContainerInfoTableTailer() {
}

