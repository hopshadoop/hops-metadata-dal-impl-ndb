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

#include "ContainerToDecreaseTableTailer.h"

using namespace Utils::NdbC;

const string _containerToDecrease_table= "yarn_container_to_decrease";
const int _containerToDecrease_noCols= 8;
const string _containerToDecrease_cols[_containerToDecrease_noCols]=
    {"rmnodeid",
     "containerid",
     "http_address",
     "priority",
     "memory_size",
     "virtual_cores",
     "gpus",
     "version"
    };

const int _containerToDecrease_noEvents = 2; 
const NdbDictionary::Event::TableEvent _containerToDecrease_events[_containerToDecrease_noEvents] = 
    { NdbDictionary::Event::TE_INSERT, 
      NdbDictionary::Event::TE_UPDATE
    };

const WatchTable ContainerToDecreaseTableTailer::TABLE = {_containerToDecrease_table, _containerToDecrease_cols, _containerToDecrease_noCols , _containerToDecrease_events, _containerToDecrease_noEvents};
const int RMNODE_ID = 0;
const int CONTAINER_ID = 1;
const int HTTPADDRESS = 2;
const int PRIORITY = 3;
const int MEMSIZE = 4;
const int VIRTUALCORES = 5;
const int GPUS = 6;
const int VERSION = 7;

ContainerToDecreaseTableTailer::ContainerToDecreaseTableTailer(Ndb* ndb, const int poll_maxTimeToWait,JavaVM* jvm) 
  : TableTailer(ndb, TABLE, poll_maxTimeToWait, jvm){
  
  
}

void ContainerToDecreaseTableTailer::handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]) {
  if(!attached){
    if (jvm->AttachCurrentThread((void**)&env, NULL)!= 0) {
      std::cout << "Failed to attach" << std::endl;
    }
    cls = env->FindClass("io/hops/streaming/ContainerToDecreaseEventReceiver");
    jmethodID midInit = env->GetMethodID( cls, "<init>", "()V");
    containerToDecreaseEventReceiver = env->NewObject( cls, midInit);
  
    midCreateAndAddToQueue = env->GetMethodID(cls, "createAndAddToQueue","(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;IJIII)V");
    attached = true;
  }
  //get the values
  jstring rmnodeId = env->NewStringUTF(get_string(value[RMNODE_ID]).c_str());
  jstring containerId = env->NewStringUTF(get_string(value[CONTAINER_ID]).c_str());
  jstring httpAddress = env->NewStringUTF(get_string(value[HTTPADDRESS]).c_str());
  int priority = value[PRIORITY]->int32_value();
  long memSize = value[MEMSIZE]->int64_value();
  int virtualCores = value[VIRTUALCORES]->int32_value();
  int gpus = value[GPUS]->int32_value();
  int version = value[VERSION]->int32_value();

  //create the rmnode object and put it in the event queue
  LOG_INFO("create event");
  env->CallVoidMethod(containerToDecreaseEventReceiver,midCreateAndAddToQueue,rmnodeId, containerId, httpAddress, priority, memSize, virtualCores, gpus, version);
  env->DeleteLocalRef(rmnodeId);
  env->DeleteLocalRef(containerId);
  env->DeleteLocalRef(httpAddress);
}

ContainerToDecreaseTableTailer::~ContainerToDecreaseTableTailer() {
}

