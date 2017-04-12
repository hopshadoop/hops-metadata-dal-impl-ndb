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

#include "PendingEventTableTailer.h"

using namespace Utils::NdbC;

const string _pendingEvents_table= "yarn_pendingevents";
const int _pendingEvents_noCols= 5;
const string _pendingEvents_cols[_pendingEvents_noCols]=
    {"id",
     "rmnodeid",
     "type",
     "status",
     "contains",
    };

const int _pendingEvents_noEvents = 2; 
const NdbDictionary::Event::TableEvent _pendingEvents_events[_pendingEvents_noEvents] = 
    { NdbDictionary::Event::TE_INSERT, 
      NdbDictionary::Event::TE_UPDATE
    };

const WatchTable PendingEventTableTailer::TABLE = {_pendingEvents_table, _pendingEvents_cols, _pendingEvents_noCols , _pendingEvents_events, _pendingEvents_noEvents};

const int ID = 0;
const int RMNODE_ID = 1;
const int TYPE = 2;
const int STATUS = 3;
const int CONTAINS = 4;

PendingEventTableTailer::PendingEventTableTailer(Ndb* ndb, const int poll_maxTimeToWait,JavaVM* jvm) 
  : TableTailer(ndb, TABLE, poll_maxTimeToWait, jvm){
  
  
}

void PendingEventTableTailer::handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]) {
  if(!attached){
    if (jvm->AttachCurrentThread((void**)&env, NULL)!= 0) {
      std::cout << "Failed to attach" << std::endl;
    }
    cls = env->FindClass("io/hops/streaming/PendingEventReceiver");
    jmethodID midInit = env->GetMethodID( cls, "<init>", "()V");
    PendingEventReceiver = env->NewObject( cls, midInit);
  
    midCreateAndAddToQueue = env->GetMethodID(cls, "createAndAddToQueue","(ILjava/lang/String;Ljava/lang/String;Ljava/lang/String;I)V");
    attached = true;
  }
  //get the values
  int id = value[ID]->int32_value();
  jstring rmnodeId = env->NewStringUTF(get_string(value[RMNODE_ID]).c_str());
  jstring type = env->NewStringUTF(get_string(value[TYPE]).c_str());
  jstring status = env->NewStringUTF(get_string(value[STATUS]).c_str());
  int contains = value[CONTAINS]->int32_value();
  //create the pending object and put it in the event queue
  LOG_INFO("create event");
  env->CallVoidMethod(PendingEventReceiver,midCreateAndAddToQueue,id, rmnodeId, type, status,contains);
  env->DeleteLocalRef(rmnodeId);
  env->DeleteLocalRef(type);
  env->DeleteLocalRef(status);   
}

PendingEventTableTailer::~PendingEventTableTailer() {
}

