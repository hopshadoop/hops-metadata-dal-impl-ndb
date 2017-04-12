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
 * File:   TableTailer.cpp
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#include "TableTailer.h"

using namespace Utils;
using namespace Utils::NdbC;

TableTailer::TableTailer(Ndb* ndb, const WatchTable table, const int poll_maxTimeToWait,JavaVM* jvm) : mNdbConnection(ndb), mStarted(false),
        mEventName(concat("tail-", table.mTableName)), mTable(table), mPollMaxTimeToWait(poll_maxTimeToWait){
  this->jvm=jvm;
  attached=false;
}

void TableTailer::start() {
    if (mStarted) {
        return;
    }
    
    LOG_INFO("start table tailer " + mTable.mTableName);
    
    createListenerEvent();

    pthread_create(&mThread, NULL,(void*(*)(void*)) TableTailer::Run,this);
    mStarted = true;
}

void *TableTailer::Run(void *_tableTailer){
  ((TableTailer*)_tableTailer)->run();
  return NULL;
}

void TableTailer::run() {
  running = true;
  waitForEvents();
  stopped = true;
  LOG_INFO("Thread is stopped");
  return;
}

void TableTailer::createListenerEvent() {
    NdbDictionary::Dictionary *myDict = mNdbConnection->getDictionary();
    if (!myDict) LOG_NDB_API_ERROR(mNdbConnection->getNdbError());

    const NdbDictionary::Table *table = myDict->getTable(mTable.mTableName.c_str());
    if (!table) LOG_NDB_API_ERROR(myDict->getNdbError());

    NdbDictionary::Event myEvent(mEventName.c_str(), *table);
    
    for(int i=0; i< mTable.mNoEvents; i++){
        myEvent.addTableEvent(mTable.mWatchEvents[i]);
    }
    const char* columns[mTable.mNoColumns];
    for(int i=0; i< mTable.mNoColumns; i++){
        columns[i] = mTable.mColumnNames[i].c_str();
    }
    myEvent.addEventColumns(mTable.mNoColumns, columns);
    //myEvent.mergeEvents(merge_events);

    // Add event to database
    if (myDict->createEvent(myEvent) == 0)
      myEvent.print();
    else if (myDict->getNdbError().classification ==
            NdbError::SchemaObjectExists) {
        LOG_ERROR("Event creation failed, event exists, dropping Event...");
        if (myDict->dropEvent(mEventName.c_str())) LOG_NDB_API_ERROR(myDict->getNdbError());
        // try again
        // Add event to database
        if (myDict->createEvent(myEvent)) LOG_NDB_API_ERROR(myDict->getNdbError());
    } else
        LOG_NDB_API_ERROR(myDict->getNdbError());
}

void TableTailer::removeListenerEvent() {
    NdbDictionary::Dictionary *myDict = mNdbConnection->getDictionary();
    if (!myDict) LOG_NDB_API_ERROR(mNdbConnection->getNdbError());
    // remove event from database
    if (myDict->dropEvent(mEventName.c_str(), 1)) LOG_NDB_API_ERROR(myDict->getNdbError());
}

void TableTailer::waitForEvents() {
    
    LOG_INFO("create EventOperation for [" << mEventName << "]");
    if ((ndbOp = mNdbConnection->createEventOperation(mEventName.c_str())) == NULL)
        LOG_NDB_API_ERROR(mNdbConnection->getNdbError());

    NdbRecAttr * recAttr[mTable.mNoColumns];
    NdbRecAttr * recAttrPre[mTable.mNoColumns];

    // primary keys should always be a part of the result
    for (int i = 0; i < mTable.mNoColumns; i++) {
        recAttr[i] = ndbOp->getValue(mTable.mColumnNames[i].c_str());
        recAttrPre[i] = ndbOp->getPreValue(mTable.mColumnNames[i].c_str());
    }

    LOG_INFO("Execute " << mEventName);
    // This starts changes to "start flowing"
    if (ndbOp->execute())
        LOG_NDB_API_ERROR(ndbOp->getNdbError());
    while (running) {
        int r = mNdbConnection->pollEvents2(mPollMaxTimeToWait);
        if (r > 0) {
	  NdbEventOperation* op;
            while ((op = mNdbConnection->nextEvent2())) {
                NdbDictionary::Event::TableEvent event = op->getEventType2();
                if(event != NdbDictionary::Event::TE_EMPTY){
		  LOG_TRACE(mEventName << " Got Event [" << event << ","  << getEventName(event) << "] Epoch " << op->getEpoch());
                }
                switch (event) {
                    case NdbDictionary::Event::TE_INSERT:
                    case NdbDictionary::Event::TE_DELETE:
                    case NdbDictionary::Event::TE_UPDATE:
                        handleEvent(event, recAttrPre, recAttr);
                        break;
                    default:
                        break;
                }

            }
        }
    }

}

const char* TableTailer::getEventName(NdbDictionary::Event::TableEvent event) {
    switch (event) {
        case NdbDictionary::Event::TE_INSERT:
            return "INSERT";
        case NdbDictionary::Event::TE_DELETE:
            return "DELETE";
        case NdbDictionary::Event::TE_UPDATE:
            return "UPDATE";
        case NdbDictionary::Event::TE_DROP:
            return "DROP";
        case NdbDictionary::Event::TE_ALTER:
            return "ALTER";
        case NdbDictionary::Event::TE_CREATE:
            return "CREATE";
        case NdbDictionary::Event::TE_GCP_COMPLETE:
            return "GCP_COMPLETE";
        case NdbDictionary::Event::TE_CLUSTER_FAILURE:
            return "CLUSTER_FAILURE";
        case NdbDictionary::Event::TE_STOP:
            return "STOP";
        case NdbDictionary::Event::TE_NODE_FAILURE:
            return "NODE_FAILURE";
        case NdbDictionary::Event::TE_SUBSCRIBE:
            return "SUBSCRIBE";
        case NdbDictionary::Event::TE_UNSUBSCRIBE:
            return "UNSUBSCRIBE";    
        case NdbDictionary::Event::TE_EMPTY:
            return "EMPTY";
        case NdbDictionary::Event::TE_INCONSISTENT:
            return "INCONSISTENT";
        case NdbDictionary::Event::TE_OUT_OF_MEMORY:
            return "OUT_OF_MEMORY";
        case NdbDictionary::Event::TE_ALL:
            return "ALL";      
    }
    return "UNKOWN";
}

void TableTailer::stop(){
  LOG_INFO("stop tabletailer");
  stopped = false;
  running = false;
  while(!stopped){
    usleep(mPollMaxTimeToWait);
  }
  removeListenerEvent();
  delete this;
}

TableTailer::~TableTailer() {
  LOG_INFO("delete tabletailer");
  mNdbConnection->dropEventOperation(ndbOp);
  delete mNdbConnection;
}

