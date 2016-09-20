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

#include "ResourceTableTailer.h"

using namespace Utils::NdbC;

const string _resource_table= "yarn_resource";
const int _resource_noCols= 4;
const string _resource_cols[_resource_noCols]=
    {"id",
     "memory",
     "virtualcores",
     "pendingeventid"
    };

const int _resource_noEvents = 2; 
const NdbDictionary::Event::TableEvent _resource_events[_resource_noEvents] = 
    { NdbDictionary::Event::TE_INSERT, 
      NdbDictionary::Event::TE_UPDATE
    };

const WatchTable ResourceTableTailer::TABLE = {_resource_table, _resource_cols, _resource_noCols , _resource_events, _resource_noEvents};

const int ID = 0;
const int MEMORY = 1;
const int VIRTUALCORES = 2;
const int PENDING_EVENT_ID = 3;


ResourceTableTailer::ResourceTableTailer(Ndb* ndb, const int poll_maxTimeToWait,JavaVM* jvm) 
  : TableTailer(ndb, TABLE, poll_maxTimeToWait, jvm){
  
  
}

void ResourceTableTailer::handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]) {
  if(!attached){
    if (jvm->AttachCurrentThread((void**)&env, NULL)!= 0) {
      std::cout << "Failed to attach" << std::endl;
    }
    cls = env->FindClass("io/hops/streaming/ResourceEventReceiver");
    jmethodID midInit = env->GetMethodID( cls, "<init>", "()V");
    resourceEventReceiver = env->NewObject( cls, midInit);
  
    midCreateAndAddToQueue = env->GetMethodID(cls, "createAndAddToQueue","(Ljava/lang/String;III)V");
    attached = true;
  }
  //get the values
  jstring id = env->NewStringUTF(get_string(value[ID]).c_str());
  int memory = value[MEMORY]->int32_value();
  int virtualcores = value[VIRTUALCORES]->int32_value();
  int pendingEventId = value[PENDING_EVENT_ID]->int32_value();
  //create the rmnode object and put it in the event queue
  env->CallVoidMethod(resourceEventReceiver,midCreateAndAddToQueue,id,memory,virtualcores,pendingEventId);
  env->DeleteLocalRef(id);   
}

ResourceTableTailer::~ResourceTableTailer() {
}

