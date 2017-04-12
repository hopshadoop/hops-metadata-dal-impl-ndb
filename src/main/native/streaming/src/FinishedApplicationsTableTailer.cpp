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

#include "FinishedApplicationsTableTailer.h"

using namespace Utils::NdbC;

const string _finishedApplications_table= "yarn_rmnode_finishedapplications";
const int _finishedApplications_noCols= 2;
const string _finishedApplications_cols[_finishedApplications_noCols]=
    {"rmnodeid",
     "applicationid"
    };

const int _finishedApplications_noEvents = 2; 
const NdbDictionary::Event::TableEvent _finishedApplications_events[_finishedApplications_noEvents] = 
    { NdbDictionary::Event::TE_INSERT, 
      NdbDictionary::Event::TE_UPDATE
    };

const WatchTable FinishedApplicationsTableTailer::TABLE = {_finishedApplications_table, _finishedApplications_cols, _finishedApplications_noCols , _finishedApplications_events, _finishedApplications_noEvents};

const int RMNODE_ID = 0;
const int APPLICATION_ID = 1;

FinishedApplicationsTableTailer::FinishedApplicationsTableTailer(Ndb* ndb, const int poll_maxTimeToWait,JavaVM* jvm) 
  : TableTailer(ndb, TABLE, poll_maxTimeToWait, jvm){
  
  
}

void FinishedApplicationsTableTailer::handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]) {
  if(!attached){
    if (jvm->AttachCurrentThread((void**)&env, NULL)!= 0) {
      std::cout << "Failed to attach" << std::endl;
    }
    cls = env->FindClass("io/hops/streaming/FinishedApplicationsEventReceiver");
    jmethodID midInit = env->GetMethodID( cls, "<init>", "()V");
    finishedApplicationsEventReceiver = env->NewObject( cls, midInit);
  
    midCreateAndAddToQueue = env->GetMethodID(cls, "createAndAddToQueue","(Ljava/lang/String;Ljava/lang/String;)V");
    attached = true;
  }
  //get the values
  jstring rmnodeId = env->NewStringUTF(get_string(value[RMNODE_ID]).c_str());
  jstring applicationId = env->NewStringUTF(get_string(value[APPLICATION_ID]).c_str());
  //create the rmnode object and put it in the event queue
    LOG_INFO("create event");
  env->CallVoidMethod(finishedApplicationsEventReceiver,midCreateAndAddToQueue,rmnodeId, applicationId);
  env->DeleteLocalRef(rmnodeId);
  env->DeleteLocalRef(applicationId);
   
}

FinishedApplicationsTableTailer::~FinishedApplicationsTableTailer() {

}

