//============================================================================
// Name        : HopsEventThread.cpp
// Created on  : Feb 6, 2015
// Author      : sri(skug@kth.se)
// Version     : 1.0
// Copyright   : SICS
// Description : Event listener thread, listening the event and converted to c++ primitive types
//============================================================================

#include "../include/HopsEventThread.h"
#include "../include/HopsReturnObject.h"
#include "../include/HopsEventThreadData.h"
#include <unistd.h>
#include <fstream>
#include <sys/time.h>
#include <fstream>
#include <sstream>
#include <sys/utsname.h>
#define MAX_EVENT_LENGTH 40
#define MAX_NUMBER_OF_COLUMN_SUBSCRIPTION 50
#define MAX_TABLE_NANE_LENGTH 50
#define UINT_MAX32 0xFFFFFFFFL

pthread_t HopsEventThread::m_threadid;

HopsEventThread::HopsEventThread() {
	m_bMergeEvent = false;
	m_isThisFirstTime = false;
	m_bIsInitialized = false;
	m_bIsEventDetailsSet = false;
	m_bIsIinterrupt = false;
	m_iTotalNoOfTable = 0;
	m_iInternalGCI = 0;
	m_iMaxEventBufferMemory = 0;
	m_iGlobalGCIIndexValue = 0;
	m_iTotalProcessingThreads = 0;
	m_iSingleContainerSize = 0;
	m_iEventType = 0;
	m_Ndb = NULL;
	m_pZDatabseName = NULL;
	m_pzEventTableNameArray = NULL;
	m_pIntEventArray = NULL;
	m_pzEventNameArray = NULL;
	m_pzEventColNamesArray = NULL;
	m_pNdbRecAttrCurrent = NULL;
	m_pNdbRecAttrPrevious = NULL;
	m_pClusterConnection = NULL;
	m_pQHolder = NULL;
	m_ptrQueueSizeCondition=NULL;
	m_ptrnanoSleepTimer=NULL;

}

void HopsEventThread::InintEventThread(
		Ndb_cluster_connection *_ptrClusterConnection,
		HopsEventQueueFrame **_ptrQHolder, int _iMaxEventBufferMemory,
		int _iEventType, QueueSizeCondition ** _ptrCondtionLock,
		int _iTotalProcessingThread,int _iThreadSingleContainerSize) {
	m_iEventType = _iEventType;
	m_pClusterConnection = _ptrClusterConnection;
	m_pQHolder = _ptrQHolder;
	m_iMaxEventBufferMemory = _iMaxEventBufferMemory;
	m_ptrQueueSizeCondition = _ptrCondtionLock;
	m_iTotalProcessingThreads = _iTotalProcessingThread;
	m_iSingleContainerSize=_iThreadSingleContainerSize;
}

HopsEventThread::~HopsEventThread() {

	std::cout << "[EventThread] destructor is calling now " << std::endl;
	// don't want to listen to events anymore
	if (m_Ndb != NULL) {
		for (int i = 0; i < m_iTotalNoOfTable; ++i) {
			if (m_Ndb->dropEventOperation(m_VectorEventOperationArray[i])) {
				APIERROR(m_Ndb->getNdbError());
			}
		}

		if (m_pIntEventArray)
			delete[] m_pIntEventArray;
		for (int i = 0; i < m_iTotalNoOfTable; ++i) {
			delete m_pzEventTableNameArray[i];
			delete m_pzEventColNamesArray[i];
			delete m_pNdbRecAttrCurrent[i];
			delete m_pNdbRecAttrPrevious[i];
			delete m_pzEventNameArray[i];

		}

		delete[] m_pzEventTableNameArray;
		delete[] m_pzEventColNamesArray;
		delete[] m_pNdbRecAttrCurrent;
		delete[] m_pNdbRecAttrPrevious;
		delete[] m_pzEventNameArray;
		delete m_Ndb;
	}

}
void HopsEventThread::populateArray() {
	m_pIntEventArray = new int[m_iTotalNoOfTable];
	m_pzEventTableNameArray = new char *[m_iTotalNoOfTable];
	m_pzEventColNamesArray = new char *[MAX_NUMBER_OF_COLUMN_SUBSCRIPTION];
	for (int i = 0; i < m_iTotalNoOfTable; ++i) {
		m_pzEventTableNameArray[i] = new char[MAX_TABLE_NANE_LENGTH];
	}
	for (int i = 0; i < MAX_NUMBER_OF_COLUMN_SUBSCRIPTION; ++i) {
		m_pzEventColNamesArray[i] = new char[MAX_TABLE_NANE_LENGTH];
	}

	m_pNdbRecAttrCurrent = new NdbRecAttribute*[m_iTotalNoOfTable];
	m_pNdbRecAttrPrevious = new NdbRecAttribute*[m_iTotalNoOfTable];

}

void HopsEventThread::setDatabaseNameAndNoOfTable(const char *_pDatabaseName,
		int _iTotalNoOfTable) {
	m_pZDatabseName = _pDatabaseName;
	m_iTotalNoOfTable = _iTotalNoOfTable;
	if (!m_bIsInitialized)
		populateArray();
}

void HopsEventThread::setEventColArray(int *_pEventArray,
		char **_pEventTableNameArray, char **_pEventColNames) {
	memcpy(m_pIntEventArray, _pEventArray, sizeof(int) * m_iTotalNoOfTable);
	int l_iOffSetVal = 0;

	for (int i = 0; i < m_iTotalNoOfTable; ++i) {
		if (i > 0)
			l_iOffSetVal += m_pIntEventArray[i - 1];
		strcpy(m_pzEventTableNameArray[i], _pEventTableNameArray[i]);
		for (int j = 0; j < m_pIntEventArray[i]; ++j) {
			strcpy(m_pzEventColNamesArray[l_iOffSetVal + j],
					_pEventColNames[l_iOffSetVal + j]);
		}
	}
}

void HopsEventThread::subscribeEvents() {
	m_Ndb = new Ndb(m_pClusterConnection, m_pZDatabseName); // Object representing the database
	m_Ndb->set_eventbuf_max_alloc(UINT_MAX32); // this is in bytes
	HopsEventStreamingTimer *l_EventStreamingTimer =
			new HopsEventStreamingTimer();
	//UINT_MAX32

//	Ndb::EventBufferMemoryUsage ttl;
//	m_Ndb->get_event_buffer_memory_usage(ttl);
//	std::cout << "allocated_bytes  " << ttl.allocated_bytes
//			<< "| usage_percent : " << ttl.usage_percent << "|used_bytes  : "
//			<< ttl.used_bytes << std::endl;

	if (m_Ndb->init() == -1)
		APIERROR(m_Ndb->getNdbError());

	m_pzEventNameArray = new char*[m_iTotalNoOfTable];
	for (int i = 0; i < m_iTotalNoOfTable; ++i) {
		m_pzEventNameArray[i] = new char[MAX_EVENT_LENGTH];
		memset(m_pzEventNameArray[i], 0, MAX_EVENT_LENGTH);
	}
	int l_iOffSetVal = 0;
	for (int j = 0; j < m_iTotalNoOfTable; ++j) {

		char *l_ptrTimeStamp = l_EventStreamingTimer->GetUniqString(j);
		strcpy(m_pzEventNameArray[j], l_ptrTimeStamp);
		m_eventNames.push_back(l_ptrTimeStamp);
    
		int l_iNumberOfCol = m_pIntEventArray[j];
		char *l_zTableName = m_pzEventTableNameArray[j];
		char *l_zEventColumnName[l_iNumberOfCol];
		//copy all the event into char array
		if (j > 0)
			l_iOffSetVal += m_pIntEventArray[j - 1];

		for (int h = 0; h < l_iNumberOfCol; ++h) {
			memcpy(l_zEventColumnName + h,
					(m_pzEventColNamesArray + h + l_iOffSetVal),
					sizeof(char *));
		}
		EventSubscription(m_Ndb, (const char *) m_pzEventNameArray[j],
				(const char *) l_zTableName, (const char **) l_zEventColumnName,
				l_iNumberOfCol, getIsMergeEvents());
		std::string l_sTablename(m_pzEventTableNameArray[j]);
		m_mapTablenNoOfEvents.insert(
				std::pair<std::string, int>(l_sTablename, l_iNumberOfCol));

		// Start "transaction" for handling events
		NdbEventOperation *NdbEventOperation;
    std::cout << "create event operation " << m_pzEventNameArray[j] << std::endl;
		if ((NdbEventOperation = m_Ndb->createEventOperation(
				m_pzEventNameArray[j])) == NULL) {
			APIERROR(m_Ndb->getNdbError());
		} else {
			NdbEventOperation->mergeEvents(getIsMergeEvents());
			m_VectorEventOperationArray.push_back(NdbEventOperation);
		}

		// primary keys should always be a part of the result
		int numberOfEventColumn = m_mapTablenNoOfEvents[l_sTablename];

		m_pNdbRecAttrCurrent[j] = new NdbRecAttribute[numberOfEventColumn];
		m_pNdbRecAttrPrevious[j] = new NdbRecAttribute[numberOfEventColumn];

		m_mapTableNameIndex.insert(
				std::make_pair<std::string, int>(l_sTablename, j));
		for (int i = 0; i < numberOfEventColumn; i++) {
			m_pNdbRecAttrCurrent[j][i].m_ptrNdbRecAttr =
					m_VectorEventOperationArray[j]->getValue(
							l_zEventColumnName[i]);
			m_pNdbRecAttrPrevious[j][i].m_ptrNdbRecAttr =
					m_VectorEventOperationArray[j]->getPreValue(
							l_zEventColumnName[i]);
		}

		if (m_VectorEventOperationArray[j]->execute())
			APIERROR(m_VectorEventOperationArray[j]->getNdbError());

	}

	m_bIsEventDetailsSet = true;
	delete l_EventStreamingTimer;
	printf(
			"[EventThread]################## Successfully subscribed to an event listening  ###################### \n");
}

pthread_t HopsEventThread::StartEventThread(HopsEventThread *_ptrEventThread) {
	pthread_attr_t l_pthreadAttr;
	size_t l_pthreadStackSize;
	pthread_attr_init(&l_pthreadAttr);
	pthread_attr_getstacksize(&l_pthreadAttr, &l_pthreadStackSize);
	struct utsname l_utsName;
	uname(&l_utsName);
	printf(
			"[HopsJNIDispatcher][INFO] ########### Starting event processor thread ################# \n");
	printf("######## System architecture      : %s\n", l_utsName.machine);
	printf("######## Node name 		  : %s\n", l_utsName.nodename);
	printf("######## System name              : %s\n", l_utsName.sysname);
	printf("######## Kernel release           : %s\n", l_utsName.release);
	printf("######## Version                  : %s\n", l_utsName.version);
	printf("######## System stack size        : %li\n", l_pthreadStackSize);
	pthread_create(&m_threadid, NULL, (void*(*)(void*)) HopsEventThread::Run, (void*)_ptrEventThread);
	cout
			<< "[EventThread] ################### Event processor starting on this thread id  : "
			<< m_threadid << endl;

	pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);
	return m_threadid;
}

void *HopsEventThread::Run(void * _pLHandler) {
	((HopsEventThread*) _pLHandler)->StartThread();
	return NULL;
}

void HopsEventThread::PushDataToOtherThread(NdbEventOperation *_pNdbOperation) {
	Uint64 tempTransId = _pNdbOperation->getTransId();
	Uint64 transactionId = Uint64((Uint32) tempTransId) << 32
			| tempTransId >> 32;
	std::string stablename(_pNdbOperation->getTable()->getName());
	int index = m_mapTableNameIndex[stablename];
	int numberOfEvents = m_mapTablenNoOfEvents[stablename];
	HopsReturnObject *pReturnObject = new HopsReturnObject(
			(char *) _pNdbOperation->getTable()->getName());
	for (int l = 0; l < numberOfEvents; l++) {
		NdbRecAttr* ra = m_pNdbRecAttrCurrent[index][l].m_ptrNdbRecAttr;
		if (ra->isNULL() >= 0) { // we have a value , this is including null values

			pReturnObject->processNdbRecAttr(ra);

		}
	}
	if (!m_isThisFirstTime) {
		m_iGlobalGCIIndexValue = _pNdbOperation->getGCI();
		m_isThisFirstTime = true;
		m_iInternalGCI = 0;
	} else if (m_iGlobalGCIIndexValue != _pNdbOperation->getGCI()) {
		m_iGlobalGCIIndexValue = _pNdbOperation->getGCI();
		++m_iInternalGCI;
	}
	int l_iThreaedIdOffSet = m_iInternalGCI % m_iTotalProcessingThreads;
	m_ptrQueueSizeCondition[l_iThreaedIdOffSet]->IncreaseQueueSize(m_iSingleContainerSize);

	EventThreadData *pEventData = new EventThreadData();
	pEventData->SetGCIIndexValue(m_iInternalGCI);
	pEventData->setReturnObjectPtr(pReturnObject);
	pEventData->setTransactionId(transactionId);
	HopsEventDataPacket *pContMNS = new HopsEventDataPacket(pEventData);
	m_pQHolder[l_iThreaedIdOffSet]->AddToProducerQueue(pContMNS);
	m_pQHolder[l_iThreaedIdOffSet]->PushToIntermediateQueue();

}

void HopsEventThread::CancelEventThread() {
	pthread_cancel(m_threadid);
}
void HopsEventThread::StartThread() {
	while (true) {
		if (getIsEventDetailsSet()) {
			while (true) {
				if (!m_bIsIinterrupt) {
					int l_iResult = m_Ndb->pollEvents(1); // wait for event or 1 ms
					if (l_iResult > 0) {
						NdbEventOperation *ptempOP;
						while ((ptempOP = m_Ndb->nextEvent())) {
							PushDataToOtherThread(ptempOP);
						}
					}
				}else{
					sleep(1);
				}
			}

		}
	}
}
void HopsEventThread::DropEvents() {
	// when api function called, drop all the events and start again
	if (m_Ndb != NULL) {
		// if component restart m_ndb is not null, so drop all the events and subscribe again
		NdbDictionary::Dictionary *myDict = m_Ndb->getDictionary();
		for (int i = 0; i < (int) m_eventNames.size(); ++i) {
			printf("Dropping the events from data base : %s \n",
					m_eventNames[i]);
			if (myDict->dropEvent(m_eventNames[i]))
				APIERROR(myDict->getNdbError());
		}
	}
  m_VectorEventOperationArray.clear();
	m_eventNames.clear();
	m_VectorEventOperationArray.clear();

}
int HopsEventThread::EventSubscription(Ndb* myNdb, const char *eventName,
		const char *eventTableName, const char **eventColumnNames,
		const int noEventColumnNames, bool merge_events) {
	NdbDictionary::Dictionary *myDict = myNdb->getDictionary();
	if (!myDict)
		APIERROR(myNdb->getNdbError());
	const NdbDictionary::Table *table = myDict->getTable(eventTableName);
	if (!table) 
  {
		printf("[NdbEventAPI] Table not found - name : %s\n",eventTableName);
		APIERROR(myDict->getNdbError());
	}

	NdbDictionary::Event l_hopEvents(eventName, *table);
	switch (m_iEventType) {
	case 0: //all events
	{
		l_hopEvents.addTableEvent(NdbDictionary::Event::TE_ALL);
	}
		break;
	case 1: {
		l_hopEvents.addTableEvent(NdbDictionary::Event::TE_INSERT);
	}
		break;
	case 2: {
		l_hopEvents.addTableEvent(NdbDictionary::Event::TE_UPDATE);
	}
		break;
	case 3: {
		l_hopEvents.addTableEvent(NdbDictionary::Event::TE_DELETE);
	}
		break;
	case 4: //update and insert
	{
		l_hopEvents.addTableEvent(NdbDictionary::Event::TE_INSERT);
		l_hopEvents.addTableEvent(NdbDictionary::Event::TE_UPDATE);
	}
		break;

	}

	l_hopEvents.addEventColumns(noEventColumnNames, eventColumnNames);
	l_hopEvents.mergeEvents(merge_events);

// Add event to database
	if (myDict->createEvent(l_hopEvents) == 0)
		l_hopEvents.print();
	else if (myDict->getNdbError().classification
			== NdbError::SchemaObjectExists) {
		printf(
				"\033[22;33m\a [NdbEnetAPI][WARNING]##########   Event is already created, dropping an event - %s -%s \033[0m\n",
				eventName, eventTableName);
		if (myDict->dropEvent(eventName))
			APIERROR(myDict->getNdbError());
		// try again
		// Add event to database
		if (myDict->createEvent(l_hopEvents))
			APIERROR(myDict->getNdbError());
	} else {
		printf("\033[1;31m [NdbEnetAPI][ERROR]##########  Schema doesn't exist - %s -%s \033[0m\n",
				eventName, eventTableName);
		APIERROR(myDict->getNdbError());
	}

	return 0;
}
