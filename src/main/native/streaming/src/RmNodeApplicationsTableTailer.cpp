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

#include "RmNodeApplicationsTableTailer.h"

using namespace Utils::NdbC;

const string _rmNodeApplications_table= "yarn_rmnode_applications";
const int _rmNodeApplications_noCols= 3;
const string _rmNodeApplications_cols[_rmNodeApplications_noCols]=
    {"rmnodeid",
     "applicationid",
     "status"
    };

const int _rmNodeApplications_noEvents = 2; 
const NdbDictionary::Event::TableEvent _rmNodeApplications_events[_rmNodeApplications_noEvents] = 
    { NdbDictionary::Event::TE_INSERT, 
      NdbDictionary::Event::TE_UPDATE
    };

const WatchTable RmNodeApplicationsTableTailer::TABLE = {_rmNodeApplications_table, _rmNodeApplications_cols, _rmNodeApplications_noCols , _rmNodeApplications_events, _rmNodeApplications_noEvents};

const int RMNODE_ID = 0;
const int APPLICATION_ID = 1;
const int STATUS = 2;

RmNodeApplicationsTableTailer::RmNodeApplicationsTableTailer(Ndb* ndb, const int poll_maxTimeToWait,JavaVM* jvm) 
  : TableTailer(ndb, TABLE, poll_maxTimeToWait, jvm){
  
  
}

void RmNodeApplicationsTableTailer::handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]) {
  if(!attached){
    if (jvm->AttachCurrentThread((void**)&env, NULL)!= 0) {
      std::cout << "Failed to attach" << std::endl;
    }
    cls = env->FindClass("io/hops/streaming/RmNodeApplicationsEventReceiver");
    jmethodID midInit = env->GetMethodID( cls, "<init>", "()V");
    rmNodeApplicationsEventReceiver = env->NewObject( cls, midInit);
  
    midCreateAndAddToQueue = env->GetMethodID(cls, "createAndAddToQueue","(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V");
    attached = true;
  }
  //get the values
  jstring rmnodeId = env->NewStringUTF(get_string(value[RMNODE_ID]).c_str());
  jstring applicationId = env->NewStringUTF(get_string(value[APPLICATION_ID]).c_str());
  jstring status = env->NewStringUTF(get_string(value[STATUS]).c_str());
  //create the rmnode object and put it in the event queue
    LOG_INFO("create event");
    env->CallVoidMethod(rmNodeApplicationsEventReceiver,midCreateAndAddToQueue,rmnodeId, applicationId, status);
  env->DeleteLocalRef(rmnodeId);
  env->DeleteLocalRef(applicationId);
  env->DeleteLocalRef(status);
   
}

RmNodeApplicationsTableTailer::~RmNodeApplicationsTableTailer() {

}

