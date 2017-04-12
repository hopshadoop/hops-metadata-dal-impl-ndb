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

#include "NextHeartBeatTableTailer.h"

using namespace Utils::NdbC;

const string _nextHeartBeat_table= "yarn_nextheartbeat";
const int _nextHeartBeat_noCols= 2;
const string _nextHeartBeat_cols[_nextHeartBeat_noCols]=
    {"rmnodeid",
     "nextheartbeat"
    };

const int _nextHeartBeat_noEvents = 2; 
const NdbDictionary::Event::TableEvent _nextHeartBeat_events[_nextHeartBeat_noEvents] = 
    { NdbDictionary::Event::TE_INSERT, 
      NdbDictionary::Event::TE_UPDATE
    };

const WatchTable NextHeartBeatTableTailer::TABLE = {_nextHeartBeat_table, _nextHeartBeat_cols, _nextHeartBeat_noCols , _nextHeartBeat_events, _nextHeartBeat_noEvents};

const int RMNODE_ID = 0;
const int NEXT_HEARTBEAT = 1;

NextHeartBeatTableTailer::NextHeartBeatTableTailer(Ndb* ndb, const int poll_maxTimeToWait,JavaVM* jvm) 
  : TableTailer(ndb, TABLE, poll_maxTimeToWait, jvm){
  
  
}

void NextHeartBeatTableTailer::handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]) {
  if(!attached){
    if (jvm->AttachCurrentThread((void**)&env, NULL)!= 0) {
      std::cout << "Failed to attach" << std::endl;
    }
    cls = env->FindClass("io/hops/streaming/NextHeartBeatEventReceiver");
    jmethodID midInit = env->GetMethodID( cls, "<init>", "()V");
    nextHeartBeatEventReceiver = env->NewObject( cls, midInit);
  
    midCreateAndAddToQueue = env->GetMethodID(cls, "createAndAddToQueue","(Ljava/lang/String;I)V");
    attached = true;
  }
  //get the values
  jstring rmnodeId = env->NewStringUTF(get_string(value[RMNODE_ID]).c_str());
  int nextHeartBeat = value[NEXT_HEARTBEAT]->int32_value();
  //create the rmnode object and put it in the event queue
    LOG_INFO("create event");
  env->CallVoidMethod(nextHeartBeatEventReceiver,midCreateAndAddToQueue,rmnodeId, nextHeartBeat);
  env->DeleteLocalRef(rmnodeId);
   
}

NextHeartBeatTableTailer::~NextHeartBeatTableTailer() {
  LOG_INFO("delete rmnode table tailer");
}

