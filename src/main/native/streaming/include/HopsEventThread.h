/*
 * EventThread.h
 *
 *  Created on: Feb 6, 2015
 *      Author: sri
 */

#ifndef HOPSEVENTTHREAD_H_
#define HOPSEVENTTHREAD_H_

#include <iostream>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <vector>
#include <map>
#include "NdbApi.hpp"
#include "HopsUtils.h"
#include "HopsConfigReader.h"
using namespace hops::utl::que;
using namespace hops;
using namespace hops::utl;
using namespace cnf;

#define PRINT_ERROR(code,msg) \
  std::cout << "Error in " << __FILE__ << ", line: " << __LINE__ \
            << ", code: " << code \
            << ", msg: " << msg << "." << std::endl

#define APIERROR(error) { \
  PRINT_ERROR(error.code,error.message); \
  exit(-1); }

typedef struct {
	NdbRecAttr* m_ptrNdbRecAttr;
} NdbRecAttribute;

class HopsEventThread {
public:
	HopsEventThread();
	virtual ~HopsEventThread();
	void InintEventThread(Ndb_cluster_connection *_ptrClusterConnection,
			HopsEventQueueFrame **_ptrQHolder, int _iMaxEventBufferMemory,
			int _iEventType, QueueSizeCondition ** _ptrCondtionLock,
			int _iTotalProcessingThread, int _iThreadSingleContainerSize);
	void StartThread();
	static void * Run(void * _pProcessor);
	static pthread_t StartEventThread(HopsEventThread *_ptrEventThread);

	void DropEvents();
	int EventSubscription(Ndb* myNdb, const char *eventName,
			const char *eventTableName, const char **eventColumnNames,
			const int noEventColumnNames, bool merge_events);

	void setDatabaseNameAndNoOfTable(const char *_pDatabaseName,
			int _iTotalNoOfTable);
	void setEventColArray(int *_pEventArray, char **_pEventTableNameArray,
			char **_pEventColNames);
	void setMergeEvents(bool _bMergeEvent) {
		m_bMergeEvent = _bMergeEvent;
	}
	inline bool getIsMergeEvents() {
		return m_bMergeEvent;
	}

	void subscribeEvents();
	inline bool getIsEventDetailsSet() {
		return m_bIsEventDetailsSet;
	}

	void StopEventThread() {
		m_bIsIinterrupt = true;
	}
	void CancelEventThread();
private:

	void populateArray();
	void PushDataToOtherThread(NdbEventOperation *_pNdbOperation);
	static pthread_t m_threadid;
	HopsEventQueueFrame ** m_pQHolder;
	int m_iInternalGCI;
	bool m_isThisFirstTime;
	Uint64 m_iGlobalGCIIndexValue;

	struct timespec m_nanoTimeSpec;
	Ndb_cluster_connection *m_pClusterConnection;
	std::vector<NdbEventOperation*> m_VectorEventOperationArray;
	Ndb* m_Ndb;
	int m_iEventType;

	int m_iTotalNoOfTable;
	int m_iSingleContainerSize;
	int *m_pIntEventArray;
	bool m_bMergeEvent;
	bool m_bIsInitialized;
	bool m_bIsEventDetailsSet;
	const char *m_pZDatabseName;
	char **m_pzEventTableNameArray;
	char **m_pzEventColNamesArray;
	char **m_pzEventNameArray;
	NdbRecAttribute ** m_pNdbRecAttrCurrent;
	NdbRecAttribute ** m_pNdbRecAttrPrevious;
	int m_iMaxEventBufferMemory;
	std::map<std::string, int> m_mapTableNameIndex;
	std::map<std::string, int> m_mapTablenNoOfEvents;

	HopsEventStreamingTimer *m_ptrnanoSleepTimer;
	volatile bool m_bIsIinterrupt;

	QueueSizeCondition ** m_ptrQueueSizeCondition;

	int m_iTotalProcessingThreads;
	vector<char *> m_eventNames;

};

#endif /* HOPSEVENTTHREAD_H_ */
