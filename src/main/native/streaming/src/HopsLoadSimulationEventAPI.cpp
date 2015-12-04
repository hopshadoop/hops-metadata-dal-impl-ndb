
//============================================================================
// Name        : HopsLoadSimulationEventAPI.cpp
// Created on  : Apr 7, 2015
// Author      : sri(skug@kth.se)
// Version     : 1.0
// Copyright   : SICS
// Description : API functions for load simulation, this will be called from JNI interface
//============================================================================

#include "../include/HopsLoadSimulationEventAPI.h"

#include "../include/HopsLoadSimulation.h"
using namespace cnf;

#define MAX_TABLE_NAME_LENGTH 30
#define GB 1073741824

HopsLoadSimulationEventAPI* HopsLoadSimulationEventAPI::m_pInstance = NULL;

HopsLoadSimulationEventAPI* HopsLoadSimulationEventAPI::Instance() {
	if (!m_pInstance)   // Only allow one instance of class to be generated.
		m_pInstance = new HopsLoadSimulationEventAPI();

	return m_pInstance;
}

HopsLoadSimulationEventAPI::HopsLoadSimulationEventAPI() {
	m_pthreadArray = NULL;
	m_ptrJavaObjectDispatcherQ = NULL;
	m_pConditionLock = NULL;
	m_ptrProcessingQ = NULL;
	m_ptrLoadSimulationJNIDispatcher=NULL;
	m_iTotalThreads = 0;
	m_iMaxEventBufferSize = 0;
	m_ptrCondtionLock = NULL;
}

HopsLoadSimulationEventAPI::~HopsLoadSimulationEventAPI() {

	delete[] m_pthreadArray;
	delete m_ptrJavaObjectDispatcherQ;
	for (int i = 0; i < m_iTotalThreads; ++i) {
		delete m_ptrProcessingQ[i];
		delete m_ptrLoadSimulationJNIDispatcher[i];
	}
	delete[] m_ptrProcessingQ;
	delete[] m_ptrLoadSimulationJNIDispatcher;
}

pthread_t * HopsLoadSimulationEventAPI::GetPthreadIdArray(int *_ptrSize) {
	*_ptrSize = m_iTotalThreads + 1;
	return m_pthreadArray;
}

//TODO are you sure that stopping thread return nothing?
void HopsLoadSimulationEventAPI::StopAllDispatchingThreads() {
	for (int i = 0; i < m_iTotalThreads; ++i) {
		m_ptrLoadSimulationJNIDispatcher[i]->StopProcessorThread();
	}
}

void HopsLoadSimulationEventAPI::LoadSimulationInitAPI(JavaVM *_ptrJVM,HopsConfigFile *_ptrConf) {
	char **pEventTableNameArray;

	char l_zConfigReaderArray[1024];
	char l_zSetOfTables[1024];

	memset(l_zConfigReaderArray, 0, sizeof(l_zConfigReaderArray));
	memset(l_zSetOfTables, 0, sizeof(l_zSetOfTables));

	m_iTotalThreads = (int) atoi(_ptrConf->GetValue("PROCESSING_THREADS"));
	double l_dMaxQCapacity = (double) atof(_ptrConf->GetValue("QUEUE_MAX_CAPACITY"));

	// event receiving thread plus total number of thread
	m_pthreadArray = new pthread_t[m_iTotalThreads + 1];

	memset(l_zConfigReaderArray, 0, sizeof(l_zConfigReaderArray));
	memset(l_zSetOfTables, 0, sizeof(l_zSetOfTables));
	sprintf(l_zConfigReaderArray, "SIMULATION_TABLE_NAMES");
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

	m_ptrProcessingQ = new HopsEventQueueFrame *[m_iTotalThreads];

	m_ptrLoadSimulationJNIDispatcher = new HopsLoadSimulationJNIDispatcher *[m_iTotalThreads];
	m_ptrCondtionLock = new QueueSizeCondition *[m_iTotalThreads];
	for (int i = 0; i < m_iTotalThreads; ++i) {
		m_ptrCondtionLock[i] = new QueueSizeCondition();
		m_ptrCondtionLock[i]->InitQueueSizeCondition(l_dMaxQCapacity*GB);
	}
	pthread_t l_pthreadId;
	for (int i = 0; i < m_iTotalThreads; ++i) {
		m_ptrProcessingQ[i] = new HopsEventQueueFrame();
		m_ptrLoadSimulationJNIDispatcher[i] = new HopsLoadSimulationJNIDispatcher();

		m_ptrLoadSimulationJNIDispatcher[i]->InintLoadSimulationJNIDispatcher(m_ptrProcessingQ[i], _ptrJVM,
				m_ptrCondtionLock[i]);
		for (int j = 0; j < (int) l_vecTableNames.size(); ++j) {
			m_ptrLoadSimulationJNIDispatcher[i]->InitializeTablePosition(l_vecTableNames[j]);
		}

	}

	if (m_iTotalThreads == 1) {
		m_ptrLoadSimulationJNIDispatcher[0]->SetSingleThread(true);
		l_pthreadId = m_ptrLoadSimulationJNIDispatcher[0]->StartEventProcessor(
				m_ptrLoadSimulationJNIDispatcher[0], NULL, NULL,_ptrConf);
		m_pthreadArray[0] = l_pthreadId;
	} else {
		ThreadToken **l_ptrThreadToken = new ThreadToken *[m_iTotalThreads];
		for (int j = 0; j < m_iTotalThreads; ++j) {
			l_ptrThreadToken[j] = new ThreadToken();
			if (j == m_iTotalThreads - 1) {
				l_pthreadId = m_ptrLoadSimulationJNIDispatcher[j]->StartEventProcessor(
						m_ptrLoadSimulationJNIDispatcher[j], m_ptrLoadSimulationJNIDispatcher[0],
						l_ptrThreadToken[j],_ptrConf);
			} else {
				l_pthreadId = m_ptrLoadSimulationJNIDispatcher[j]->StartEventProcessor(
						m_ptrLoadSimulationJNIDispatcher[j], m_ptrLoadSimulationJNIDispatcher[j + 1],
						l_ptrThreadToken[j],_ptrConf);
			}
			m_pthreadArray[j] = l_pthreadId;
			sleep(2);
		}
	}

	//starting the program during the ongoing transaction may cause the lib crash. First start the processor and sleep 1sec.

	printf(
			"########### Load simulation is starting now #######################  \n");
	sleep(3);
	HopsLoadSimulation *ptrSimulation = new HopsLoadSimulation();
	l_pthreadId = ptrSimulation->InitializeHopsSimulationThread(ptrSimulation,
			m_ptrProcessingQ,m_ptrCondtionLock,_ptrConf);

	m_pthreadArray[m_iTotalThreads] = l_pthreadId;

}

