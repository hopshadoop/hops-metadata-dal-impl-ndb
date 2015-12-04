//============================================================================
// Name        : HopsEventAPI.cpp
// Created on  : Feb 9, 2015
// Author      : sri(skug@kth.se)
// Version     : 1.0
// Copyright   : SICS
// Description : API functions to call from JNI interface
//============================================================================

#include "../include/HopsEventAPI.h"
#include "../include/HopsLoadSimulation.h"
using namespace cnf;

#define MAX_TABLE_NAME_LENGTH 50
#define GB 1073741824

HopsEventAPI* HopsEventAPI::m_pInstance = NULL;

HopsEventAPI* HopsEventAPI::Instance() {
	if (!m_pInstance)   // Only allow one instance of class to be generated.
		m_pInstance = new HopsEventAPI();

	return m_pInstance;
}

HopsEventAPI::HopsEventAPI() {

	m_ptrThreadArray = NULL;
	m_ptrJavaObjectDispatcherQ = NULL;
	m_pConditionLock = NULL;
	m_ptrProcessingQ = NULL;
	m_iTotalThreads = 0;
	m_iSingleContainerSize = 0;
	m_iMaxEventBufferSize = 0;
	m_ptrCondtionLock = NULL;
	m_ptrJNIDispatcher = NULL;
	m_ptrEventThread = NULL;
	m_bAPIInitialized = false;
}

HopsEventAPI::~HopsEventAPI() {

	delete[] m_ptrThreadArray;
	delete m_ptrJavaObjectDispatcherQ;
	for (int i = 0; i < m_iTotalThreads; ++i) {
		delete m_ptrProcessingQ[i];
		delete m_ptrJNIDispatcher[i];
	}
	delete[] m_ptrJNIDispatcher;
	delete[] m_ptrProcessingQ;
	delete m_ptrEventThread;
}

pthread_t * HopsEventAPI::GetPthreadIdArray(int *_ptrSize) {
	*_ptrSize = m_iTotalThreads + 1;
	return m_ptrThreadArray;
}

void HopsEventAPI::dropEvents() {
	if (m_bAPIInitialized) {
		// first stop pooling from db, dont get any events from ndb buffer
		m_ptrEventThread->StopEventThread();
		sleep(2);
		// first drop all the events
		m_ptrEventThread->DropEvents();
		//cancel all the dispatcher threads
		for (int i = 0; i < m_iTotalThreads; ++i) {
			m_ptrJNIDispatcher[i]->StopDispatcher();
			delete m_ptrJNIDispatcher[i];
		}
		//finally cancel the event thread
		m_ptrEventThread->CancelEventThread();
		delete m_ptrEventThread;
	}
}
void HopsEventAPI::initAPI(JavaVM *_ptrJVM, HopsConfigFile *_ptrConf) {

	char **pEventTableNameArray;

	char l_zConfigReaderArray[1024];
	char l_zSetOfTables[1024];
	char l_zNdbDatabaseName[1024];
	char l_zSetOfCol[1024];
	char l_zNdbConnectionString[1024];

	memset(l_zConfigReaderArray, 0, sizeof(l_zConfigReaderArray));
	memset(l_zSetOfTables, 0, sizeof(l_zSetOfTables));

	m_iSingleContainerSize = (int) atoi(
			_ptrConf->GetValue("SINGLE_CONTAINER_SIZE"));
	int l_iMaxQCapacity = (int) atoi(_ptrConf->GetValue("QUEUE_MAX_CAPACITY"));
	m_iTotalThreads = (int) atoi(_ptrConf->GetValue("PROCESSING_THREADS"));
	m_iMaxEventBufferSize = (int) atoi(_ptrConf->GetValue("MAX_EVENT_BUFFER"));
	int l_iEventType = (int) atoi(_ptrConf->GetValue("EVENT_TYPE"));
	int l_iTotalNoOfColumn = (int) atoi(
			_ptrConf->GetValue("TOTAL_NO_OF_COLUMN"));

	// event receiving thread plus total number of thread
	m_ptrThreadArray = new pthread_t[m_iTotalThreads + 1];

	memset(l_zConfigReaderArray, 0, sizeof(l_zConfigReaderArray));
	memset(l_zSetOfTables, 0, sizeof(l_zSetOfTables));

	sprintf(l_zConfigReaderArray, "TABLE_NAMES");
	strcpy(l_zSetOfTables, _ptrConf->GetValue(l_zConfigReaderArray));

	HopsStringTokenizer l_oTablesSep(l_zSetOfTables, ',');

	pEventTableNameArray = new char *[l_oTablesSep.GetCount()];

	std::vector<std::string> l_vecTableNames;
	for (int i = 0; i < l_oTablesSep.GetCount(); ++i) {
		pEventTableNameArray[i] = new char[MAX_TABLE_NAME_LENGTH];
		strcpy(pEventTableNameArray[i], l_oTablesSep.GetTokenAt(i));
		std::string l_sTableName(pEventTableNameArray[i]);
		l_vecTableNames.push_back(l_sTableName);
	}

	char ** pEventArray = new char *[l_iTotalNoOfColumn];

	for (int i = 0; i < l_iTotalNoOfColumn; ++i) {
		pEventArray[i] = new char[MAX_TABLE_NAME_LENGTH];
	}

	memset(l_zConfigReaderArray, 0, sizeof(l_zConfigReaderArray));

	sprintf(l_zConfigReaderArray, "EVENT_COL_NAMES");
	strcpy(l_zSetOfCol, _ptrConf->GetValue(l_zConfigReaderArray));

	HopsStringTokenizer l_oColSep(l_zSetOfCol, '|');
	int l_iCounter = 0;
	int l_arrayEvent[l_oTablesSep.GetCount()];
	for (int i = 0; i < l_oColSep.GetCount(); ++i) {
		HopsStringTokenizer l_oInterCol(l_oColSep.GetTokenAt(i), ',');
		l_arrayEvent[i] = l_oInterCol.GetCount();
		for (int j = 0; j < l_oInterCol.GetCount(); ++j) {
			strcpy(pEventArray[l_iCounter], l_oInterCol.GetTokenAt(j));
			++l_iCounter;

		}
	}

	memset(l_zConfigReaderArray, 0, sizeof(l_zConfigReaderArray));
	sprintf(l_zConfigReaderArray, "NDB_CONNECT_STRING");
	strcpy(l_zNdbConnectionString, _ptrConf->GetValue(l_zConfigReaderArray));

	Ndb_cluster_connection *cluster_connection = new Ndb_cluster_connection(
			l_zNdbConnectionString); // Object representing the cluster

	int l_iNumberOfRetries = (int) atoi(
			_ptrConf->GetValue("NUMBER_OF_RETRIES_ATTEMPT"));
	int l_iDelayInSeconds = (int) atoi(
			_ptrConf->GetValue("RETRY_DELAY_IN_SECONDS"));

	int r = cluster_connection->connect(l_iNumberOfRetries, l_iDelayInSeconds,
			1);
	if (r > 0) {
		printf(
				"[EventAPI] ########### Cluster connect failed, possibly resolved with more retries.\n");
		exit(EXIT_FAILURE);
	} else if (r < 0) {
		printf("[EventAPI] ########## Cluster connect failed.\n");
		exit(EXIT_FAILURE);
	}

	int l_iTimeoutForFistAlive = (int) atoi(
			_ptrConf->GetValue("TIMEOUT_FOR_FIRST_ALIVE"));
	int l_iTimeoutAfterFirstAlive = (int) atoi(
			_ptrConf->GetValue("TIMEOUT_AFTER_FIRST_ALIVE"));

	if (cluster_connection->wait_until_ready(l_iTimeoutForFistAlive,
			l_iTimeoutAfterFirstAlive)) {
		printf("[EventAPI] ########### Cluster was not ready within %d secs.\n",
				l_iTimeoutForFistAlive);
		exit(EXIT_FAILURE);
	}

	memset(l_zConfigReaderArray, 0, sizeof(l_zConfigReaderArray));
	sprintf(l_zConfigReaderArray, "NDB_DATABASE_NAME");
	strcpy(l_zNdbDatabaseName, _ptrConf->GetValue(l_zConfigReaderArray));

	m_ptrProcessingQ = new HopsEventQueueFrame *[m_iTotalThreads];

	m_ptrJNIDispatcher = new HopsJNIDispatcher *[m_iTotalThreads];
	m_ptrCondtionLock = new QueueSizeCondition *[m_iTotalThreads];
	for (int i = 0; i < m_iTotalThreads; ++i) {
		m_ptrCondtionLock[i] = new QueueSizeCondition();
		m_ptrCondtionLock[i]->InitQueueSizeCondition(l_iMaxQCapacity * GB);
	}

	pthread_t l_pthreadId;
	for (int i = 0; i < m_iTotalThreads; ++i) {
		m_ptrProcessingQ[i] = new HopsEventQueueFrame();
		m_ptrJNIDispatcher[i] = new HopsJNIDispatcher();

		m_ptrJNIDispatcher[i]->InintJNIDispatcher(m_ptrProcessingQ[i], _ptrJVM,
				m_ptrCondtionLock[i], m_iSingleContainerSize);
		for (int j = 0; j < (int) l_vecTableNames.size(); ++j) {
			m_ptrJNIDispatcher[i]->InitializeTablePosition(l_vecTableNames[j]);
		}

	}

	if (m_iTotalThreads == 1) {
		m_ptrJNIDispatcher[0]->SetSingleThread(true);
		l_pthreadId = m_ptrJNIDispatcher[0]->StartEventProcessor(
				m_ptrJNIDispatcher[0], NULL, NULL, _ptrConf);
		m_ptrThreadArray[0] = l_pthreadId;
	} else {
		ThreadToken **l_ptrThreadToken = new ThreadToken *[m_iTotalThreads];
		for (int j = 0; j < m_iTotalThreads; ++j) {
			l_ptrThreadToken[j] = new ThreadToken();
			if (j == m_iTotalThreads - 1) {
				l_pthreadId = m_ptrJNIDispatcher[j]->StartEventProcessor(
						m_ptrJNIDispatcher[j], m_ptrJNIDispatcher[0],
						l_ptrThreadToken[j], _ptrConf);
			} else {
				l_pthreadId = m_ptrJNIDispatcher[j]->StartEventProcessor(
						m_ptrJNIDispatcher[j], m_ptrJNIDispatcher[j + 1],
						l_ptrThreadToken[j], _ptrConf);
			}
			m_ptrThreadArray[j] = l_pthreadId;
			// Give some time to attach previous thread and start next one
			sleep(2);
		}
	}

	//starting the program during the ongoing transaction may cause the lib crash. First start the processor and sleep 1sec.

	sleep(2);

	m_ptrEventThread = new HopsEventThread();
	m_ptrEventThread->InintEventThread(cluster_connection, m_ptrProcessingQ,
			m_iMaxEventBufferSize, l_iEventType, m_ptrCondtionLock,
			m_iTotalThreads, m_iSingleContainerSize);
	m_ptrEventThread->setDatabaseNameAndNoOfTable(l_zNdbDatabaseName,
			l_oTablesSep.GetCount());
	m_ptrEventThread->setEventColArray(l_arrayEvent, pEventTableNameArray,
			pEventArray);
	m_ptrEventThread->subscribeEvents();

	l_pthreadId = m_ptrEventThread->StartEventThread(m_ptrEventThread);
	m_ptrThreadArray[m_iTotalThreads] = l_pthreadId;
	m_bAPIInitialized = true;

}

