//============================================================================
// Name        : HopsJNIDispatcher.cpp
// Created on  : Feb 2, 2015
// Author      : sri(skug@kth.se)
// Version     : 1.0
// Copyright   : SICS
// Description : Dispatcher thread is processing and dispatching the objects to java
//============================================================================

#include "../include/HopsJNIDispatcher.h"

#include <jni_md.h>
#include <stdlib.h>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <map>
#include <string>
#include <utility>
#include <algorithm>

#define SECONDS      1000000000
#define MILLISECONDS 1000000
#define MICROSECONDS 1000
#define NANOSECONDS  1

using namespace hops::utl;
using namespace cnf;

int globalTransCounter = 0;
HopsJNIDispatcher::HopsJNIDispatcher() {
	m_ptrJavaObjectDispatcherQ = NULL;
	m_jvm = NULL;
	m_newCallBackObj = NULL;
	m_ptrNeighbourDispatcher = NULL;
	m_mdMultiThreadCallBackMethod = NULL;
	m_mdSingleThreadCallBackMethod = NULL;
	m_mdBuildCompositeMethod = NULL;
	m_mdResetMethod = NULL;
	m_ptrThreadToken = NULL;
	m_ptrJNI = NULL;
	m_ptrConf = NULL;
	m_ptrCondtionLock = NULL;
	m_ptrHopsObjects = NULL;
	m_ptrSleepTimer = NULL;
	m_jniClassGlobalRef = NULL;

	m_bIsIinterrupt = false;
	m_bIsThisFirstTime = false;
	m_bIsPrintEnabled = false;
	m_isSingleThread = false;
	m_bIsReferenceTableProvided = false;

	m_iTablePositionOffset = 0;
	m_iSingleContainerSize = 0;
	m_ullPreviousDispatchTime = 0;
	m_iInternalGCIIndex = 0;
	m_threadid = 0;

}

HopsJNIDispatcher::~HopsJNIDispatcher() {
	std::cout << "[HopsJNIDispatcher] Deallocating the memory now "
			<< std::endl;
	delete m_ptrSleepTimer;
}
void HopsJNIDispatcher::InintJNIDispatcher(
		HopsEventQueueFrame *_ptrJavaObjectDispatcher, JavaVM *_ptrJVM,
		QueueSizeCondition *_ptrCondtionLock, int _iThreadSingleContainerSize) {

	m_ptrJavaObjectDispatcherQ = _ptrJavaObjectDispatcher;
	m_jvm = _ptrJVM;
	m_iSingleContainerSize = _iThreadSingleContainerSize;
	m_ptrSleepTimer = new HopsEventStreamingTimer(1, false);
	m_ptrCondtionLock = _ptrCondtionLock;

}
void *HopsJNIDispatcher::Run(void * _pLHandler) {
	((HopsJNIDispatcher*) _pLHandler)->StartProcesser();
	return NULL;
}

pthread_t HopsJNIDispatcher::StartEventProcessor(
		HopsJNIDispatcher *_ptrHopsJNIDispatcher,
		HopsJNIDispatcher *_ptrFriendDispatcher, ThreadToken *_ptrThreadToken,
		HopsConfigFile *_ptrConf) {

	m_ptrConf = _ptrConf;
	m_ptrThreadToken = _ptrThreadToken;
	pthread_create(&m_threadid, NULL, (void*(*)(void*)) HopsJNIDispatcher::Run, (void*) _ptrHopsJNIDispatcher);
	printf(
			"[HopsJNIDispatcher][INFO] ############  Event processor thread id -  %li \n",
			m_threadid);

	m_ptrNeighbourDispatcher = _ptrFriendDispatcher;
	pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);
	return m_threadid;
}

void HopsJNIDispatcher::InitializeTablePosition(std::string _sTableName) {
	m_TablePositions.insert(
			std::make_pair<std::string, int>(_sTableName,
					m_iTablePositionOffset));
	++m_iTablePositionOffset;
}

void HopsJNIDispatcher::PrintJNIPlainMessage(int _iCategory,
		const char *_pzMessage) {
	char l_zTemp[400];
	memset(l_zTemp, 0, sizeof(l_zTemp));
	const char *l_ptrTrailer = "\033[0m\n";
	switch (_iCategory) {
	case 0: // info
	{

		sprintf(l_zTemp,
				"\033[22;33m\a [HopsLoadSimulationJNIDispatcher][INFO] ### %s ",
				_pzMessage);

	}
		break;
	case 1: // warning
	{
		sprintf(l_zTemp,
				"\033[22;33m\a [HopsLoadSimulationJNIDispatcher][WARNING] ### %s ",
				_pzMessage);

	}
		break;
	case 2: // error
	{

		sprintf(l_zTemp,
				"\033[22;33m\a [HopsLoadSimulationJNIDispatcher][ERROR] ### %s ",
				_pzMessage);
	}
		break;

	}
	sprintf(l_zTemp + (int) strlen(l_zTemp), " %s ", l_ptrTrailer);
	printf("%s", l_zTemp);
	if (m_ptrJNI->ExceptionCheck()) {
		m_ptrJNI->ExceptionDescribe();
		m_ptrJNI->ExceptionClear();
	}
	exit(EXIT_FAILURE);

}
void HopsJNIDispatcher::WarmUpJavaObjectConfiguration() {
	char l_zConfigReaderArray[1024];
	char l_zValues[1024];
	memset(l_zValues, 0, sizeof(l_zValues));
	// table used to order the incoming events from streaming

	sprintf(l_zConfigReaderArray, "REFERENCE_TABLE_NAME");
	strcpy(m_zReferenceTable, m_ptrConf->GetValue(l_zConfigReaderArray));
	if (!(strcmp(m_zReferenceTable, "X") == 0)) {
		m_bIsReferenceTableProvided = true;
	}

	m_bIsPrintEnabled =
			(int) atoi(m_ptrConf->GetValue("PRINT_ENABLED")) == 1 ?
					true : false;

	int l_iTotalNumberOfClasses = (int) atoi(
			m_ptrConf->GetValue("TOTAL_NUMBER_OF_CLASSES"));

	if (l_iTotalNumberOfClasses == 0) {
		printf(
				"[HopsJNIDispatcher][FAILED] ########### Number of classes can not be null");
		exit(EXIT_FAILURE);
	}

	memset(l_zConfigReaderArray, 0, sizeof(l_zConfigReaderArray));
	memset(m_zCallBackClassName, 0, sizeof(m_zCallBackClassName));
	sprintf(l_zConfigReaderArray, "CALLBACK_CLASS_NAME");

	strcpy(m_zCallBackClassName, m_ptrConf->GetValue(l_zConfigReaderArray));

	if (m_bIsPrintEnabled) {
		printf(
				"[HopsJNIDispatcher][INFO] ########### Callback class-name 		   : %s\n",
				m_zCallBackClassName);
	}

	memset(l_zConfigReaderArray, 0, sizeof(l_zConfigReaderArray));
	sprintf(l_zConfigReaderArray, "SINGLE_THREAD_CALLBACK_METHOD");
	strcpy(l_zValues, m_ptrConf->GetValue(l_zConfigReaderArray));
	HopsStringTokenizer l_oListSepSingleThreadCallBack(l_zValues, '|'); // this separator helps to extract the load deviation

	memset(m_zSingleThreadCallBackMethod, 0,
			sizeof(m_zSingleThreadCallBackMethod));
	memset(m_zSingleThreadCallBackMethodSig, 0,
			sizeof(m_zSingleThreadCallBackMethodSig));

	strcpy(m_zSingleThreadCallBackMethod,
			l_oListSepSingleThreadCallBack.GetTokenAt(0));
	strcpy(m_zSingleThreadCallBackMethodSig,
			l_oListSepSingleThreadCallBack.GetTokenAt(1));
	if (m_bIsPrintEnabled) {
		printf(
				"[HopsJNIDispatcher][INFO] ########### Callback method 		   : %s\n",
				m_zSingleThreadCallBackMethod);
		printf(
				"[HopsJNIDispatcher][INFO] ########### Callback method signature : %s\n",
				m_zSingleThreadCallBackMethodSig);
	}

	memset(l_zConfigReaderArray, 0, sizeof(l_zConfigReaderArray));
	sprintf(l_zConfigReaderArray, "RESET_METHOD");
	strcpy(l_zValues, m_ptrConf->GetValue(l_zConfigReaderArray));
	HopsStringTokenizer l_oListSepResetMethod(l_zValues, '|'); // this separator helps to extract the load deviation

	memset(m_zResetMethod, 0, sizeof(m_zResetMethod));
	memset(m_zResetMethodSig, 0, sizeof(m_zResetMethodSig));

	strcpy(m_zResetMethod, l_oListSepResetMethod.GetTokenAt(0));
	strcpy(m_zResetMethodSig, l_oListSepResetMethod.GetTokenAt(1));

	jclass l_callbackClass = m_ptrJNI->FindClass(m_zCallBackClassName);

	if (l_callbackClass == NULL) {
		printf(
				"[HopsJNIDispatcher][FAILED] ########### Callback class not found");
		if (m_ptrJNI->ExceptionCheck()) {
			m_ptrJNI->ExceptionDescribe();
			m_ptrJNI->ExceptionClear();
		}
		exit(EXIT_FAILURE);
	}
	m_jniClassGlobalRef = (jclass) m_ptrJNI->NewGlobalRef(l_callbackClass);
	jmethodID m_methdHopClassMethod = m_ptrJNI->GetMethodID(m_jniClassGlobalRef,
			"<init>", "()V");

	m_newCallBackObj = m_ptrJNI->NewObject(m_jniClassGlobalRef,
			m_methdHopClassMethod);
	memset(l_zConfigReaderArray, 0, sizeof(l_zConfigReaderArray));
	memset(l_zValues, 0, sizeof(l_zValues));

	sprintf(l_zConfigReaderArray, "JAVA_CLASS_NAME_LISTS");
	strcpy(l_zValues, m_ptrConf->GetValue(l_zConfigReaderArray));
	HopsStringTokenizer l_oListClass(l_zValues, ','); // this separator helps to extract the load deviation
	for (int i = 0; i < l_oListClass.GetCount(); ++i) {
		jmethodID l_mdCallBackMethod = m_ptrJNI->GetMethodID(
				m_jniClassGlobalRef, l_oListClass.GetTokenAt(i), "()V");
		vecListJavaMethod.push_back(l_mdCallBackMethod);
		if (m_bIsPrintEnabled) {
			printf(
					"[HopsJNIDispatcher][INFO] ############## List creation name : %s \n",
					l_oListClass.GetTokenAt(i));
		}
	}
	m_ptrHopsObjects = new HopObject *[l_iTotalNumberOfClasses];
	for (int m = 0; m < l_iTotalNumberOfClasses; ++m) {
		char l_zBuff[40]; //this is fixed and it won't make any problem
		memset(l_zBuff, 0, sizeof(l_zBuff));

		sprintf(l_zBuff, "JAVA_CLASS_NAME_%d", m + 1);
		//TODO Sometime class length is bigger than this limit, we should change to dynamic memory allocation
		char l_zSetOfSignatures[2048];
		strcpy(l_zSetOfSignatures, m_ptrConf->GetValue(l_zBuff));

		HopsStringTokenizer l_oClassSigSep(l_zSetOfSignatures, '|');
		const char *l_pzNewBuildFunctionName = l_oClassSigSep.GetTokenAt(0);

		std::map<const char*, const char*> mapDbColNameToJavaFunctions;
		std::map<const char *, const char *> mapDbColNameToSignatures;
		m_ptrHopsObjects[m] = new HopObject(m_ptrJNI);
		m_ptrHopsObjects[m]->SetCallBackClassAndObject(m_jniClassGlobalRef,
				m_newCallBackObj);
		for (int j = 1; j < l_oClassSigSep.GetCount(); ++j) {
			HopsStringTokenizer l_oInterCol(l_oClassSigSep.GetTokenAt(j), ',');
			mapDbColNameToJavaFunctions.insert(
					std::make_pair<const char *, const char *>(
							l_oInterCol.GetTokenAt(0),
							l_oInterCol.GetTokenAt(1)));
			mapDbColNameToSignatures.insert(
					std::make_pair<const char *, const char *>(
							l_oInterCol.GetTokenAt(0),
							l_oInterCol.GetTokenAt(2)));
		}
		m_ptrHopsObjects[m]->PrepareHopJavaObjects(l_pzNewBuildFunctionName,
				mapDbColNameToJavaFunctions, mapDbColNameToSignatures);

	}

	memset(l_zConfigReaderArray, 0, sizeof(l_zConfigReaderArray));
	memset(l_zValues, 0, sizeof(l_zValues));

	sprintf(l_zConfigReaderArray, "MULTI_THREAD_CLASS_BUILDER_NAME");
	strcpy(l_zValues, m_ptrConf->GetValue(l_zConfigReaderArray));

	HopsStringTokenizer l_oListSepMTBuildCallBack(l_zValues, '|');

	memset(m_zMultiThreadBuildCallBackMethod, 0,
			sizeof(m_zMultiThreadBuildCallBackMethod));
	memset(m_zMultiThreadBuildClassBackMethodSig, 0,
			sizeof(m_zMultiThreadBuildClassBackMethodSig));

	strcpy(m_zMultiThreadBuildCallBackMethod,
			l_oListSepMTBuildCallBack.GetTokenAt(0));
	strcpy(m_zMultiThreadBuildClassBackMethodSig,
			l_oListSepMTBuildCallBack.GetTokenAt(1));

	memset(l_zConfigReaderArray, 0, sizeof(l_zConfigReaderArray));

	sprintf(l_zConfigReaderArray, "MULTI_THREAD_CALLBACK_METHOD");
	strcpy(l_zValues, m_ptrConf->GetValue(l_zConfigReaderArray));

	HopsStringTokenizer l_oListSepMTCallBack(l_zValues, '|'); // this separator helps to extract the load deviation

	memset(m_zMultiThreadCallBackMethodName, 0,
			sizeof(m_zMultiThreadCallBackMethodName));
	memset(m_zMultiThreadCallBackMethodSig, 0,
			sizeof(m_zMultiThreadCallBackMethodSig));

	strcpy(m_zMultiThreadCallBackMethodName,
			l_oListSepMTCallBack.GetTokenAt(0));
	strcpy(m_zMultiThreadCallBackMethodSig, l_oListSepMTCallBack.GetTokenAt(1));

	m_mdMultiThreadCallBackMethod = m_ptrJNI->GetMethodID(m_jniClassGlobalRef,
			m_zMultiThreadCallBackMethodName, m_zMultiThreadCallBackMethodSig);

	if (m_mdMultiThreadCallBackMethod == NULL) {
		PrintJNIPlainMessage(2,
				"Exception occurred in finding multi-threaded callback method");
	}
	m_mdBuildCompositeMethod = m_ptrJNI->GetMethodID(m_jniClassGlobalRef,
			m_zMultiThreadBuildCallBackMethod,
			m_zMultiThreadBuildClassBackMethodSig);

	if (m_mdBuildCompositeMethod == NULL) {
		PrintJNIPlainMessage(2,
				"Exception occurred in finding in build composite Java method ");
	}
	m_mdSingleThreadCallBackMethod = m_ptrJNI->GetMethodID(m_jniClassGlobalRef,
			m_zSingleThreadCallBackMethod, m_zSingleThreadCallBackMethodSig);

	m_mdResetMethod = m_ptrJNI->GetMethodID(m_jniClassGlobalRef, m_zResetMethod,
			m_zResetMethodSig);

	if (m_mdSingleThreadCallBackMethod == NULL) {
		PrintJNIPlainMessage(2,
				"Exception occurred in finding Single thread java method");
	}
	if (m_mdResetMethod == NULL) {
		PrintJNIPlainMessage(2,
				"Exception occurred in finding reset java method");
	}
	if (m_bIsPrintEnabled) {
		printf(
				"[HopsJNIDispatcher][INFO] ########### builder class method 		   : %s\n",
				m_zMultiThreadBuildCallBackMethod);
		printf(
				"[HopsJNIDispatcher][INFO] ########### builder class method sig : %s\n",
				m_zMultiThreadBuildClassBackMethodSig);
		printf(
				"[HopsJNIDispatcher][INFO] ########### Callback method 		   : %s\n",
				m_zMultiThreadCallBackMethodName);
		printf(
				"[HopsJNIDispatcher][INFO] ########### Callback method signature : %s\n",
				m_zMultiThreadCallBackMethodSig);
	}
}

void HopsJNIDispatcher::PrepareAllJNIVariables() {
	jint l_jResult;

	l_jResult = m_jvm->AttachCurrentThread((void **) &m_ptrJNI, NULL);
	if (l_jResult != JNI_OK) {
		printf(
				"[HopsJNIDispatcher][FAILED] ########### Failed to attach with Java thread - %li\n",
				pthread_self());
		exit(EXIT_FAILURE);
	} else {
		if (m_bIsPrintEnabled) {
			printf(
					"[HopsJNIDispatcher][INFO] ########### Successfully attached native thread - %li\n",
					pthread_self());
		}
		WarmUpJavaObjectConfiguration();
	}

}

void HopsJNIDispatcher::StopDispatcher() {
	jint l_jResult = m_jvm->DetachCurrentThread();
	if (l_jResult != JNI_OK) {
		printf(
				"[HopsJNIDispatcher][FAILED] ########### Failed to detach native thread  - %li\n",
				pthread_self());
	} else {
		printf(
				"[HopsJNIDispatcher][INFO] ########### Successfully  detach native thread  - %li\n",
				pthread_self());
	}
	pthread_cancel(m_threadid);
}
void HopsJNIDispatcher::StartProcesser() {

	PrepareAllJNIVariables();
	while (true) {
		int l_iPrcoeedSize = processQ();
		m_ptrCondtionLock->DecreaseQueueSize(l_iPrcoeedSize);
	}
}

int HopsJNIDispatcher::processQ() {
	HopsEventDataPacket * pCont = NULL;
	int l_iProcessedMsg = 0;

	m_ptrJavaObjectDispatcherQ->PollFromIntermediateQueue();
	while ((pCont = m_ptrJavaObjectDispatcherQ->PollFromConsumerQueue())) {
		EventThreadData * _pMsg = (EventThreadData*) pCont->m_ptrData;
		++l_iProcessedMsg;
		if (!m_bIsThisFirstTime) {
			m_iInternalGCIIndex = _pMsg->GetGCIIndexValue();
			m_bIsThisFirstTime = true;
			m_ullPreviousDispatchTime = m_ptrSleepTimer->GetEpochTime();
		} else if (m_iInternalGCIIndex != _pMsg->GetGCIIndexValue()) {
			if (!isThisSingleThread()) {
				vector<jobject> l_vecJavaTempObject;
				if (m_bIsReferenceTableProvided) {
					PreprocessJavaObjects(l_vecJavaTempObject);
				} else {
					PreprocessJavaObjectsWithoutReferenceTable(
							l_vecJavaTempObject);
				}

				if (m_iInternalGCIIndex > 0) {
					m_ptrThreadToken->WaitForSignal();
				}
				MultiThreadedDispatch(
						l_vecJavaTempObject);
				//unsigned long long l_FinishTime =
				//		m_ptrSleepTimer->GetEpochTime();
				//printf("%d,%lld\n", l_iTransactionCount, l_FinishTime);
				m_ptrNeighbourDispatcher->m_ptrThreadToken->SendSignal();

			} else {
				unsigned long long l_FinishTime =
						m_ptrSleepTimer->GetEpochTime();
				int l_iTransactionCount = 0;
				if (m_bIsReferenceTableProvided) {
					l_iTransactionCount = SingleThreadBDWithRefTable();
				} else {
					l_iTransactionCount = SingleThreadBDWithOutRefTable();
				}
//				unsigned long long duration = l_FinishTime
//						- m_ullPreviousDispatchTime;
				++globalTransCounter;
				m_ullPreviousDispatchTime = l_FinishTime;
			}
			ClearDataStructures();
			m_iInternalGCIIndex = _pMsg->GetGCIIndexValue();
		}

		ProcessAndFillBatchData(_pMsg->GetReturnObject(),
				_pMsg->getTransactionId());

		delete _pMsg;
		delete pCont;
		pCont = NULL;
	}
	return l_iProcessedMsg * m_iSingleContainerSize;

}
// every gci , we should call this , otherwise , duplicated event dispatch will happen
void HopsJNIDispatcher::ClearDataStructures() {
	m_mapOfReturnObject.clear();
	m_mapOfSortingObjects.clear();
}
void HopsJNIDispatcher::CleanUpUnwantedObjectMemory() {
	//we need to clean up the memory , there is no element to dispatch
	for (m_mapOfReturnObjectItr = m_mapOfReturnObject.begin();
			m_mapOfReturnObjectItr != m_mapOfReturnObject.end();
			++m_mapOfReturnObjectItr) {
		if ((int) m_mapOfReturnObjectItr->second.size() != 0) {
			for (int i = 0; i < (int) m_mapOfReturnObjectItr->second.size();
					++i) {
				delete m_mapOfReturnObjectItr->second[i];
			}
			m_mapOfReturnObjectItr->second.clear();
		}
	}
}

void HopsJNIDispatcher::ProcessAndFillBatchData(
		HopsReturnObject *_ptrReturnObject, Uint64 _uTransactionId) {

	// we need to make sure first value of each streaming object is a pending event id and second value is rmnodeid.
	// this can be done through the configuration files we are passing
	int l_iPendingEventId =
			_ptrReturnObject->m_listOfNdbValues[0].getInt32Value();
	std::string l_sRMNodeId =
			_ptrReturnObject->m_listOfNdbValues[1].getCharValue();

	m_mapOfBTObjectsItr = m_mapOfBatchTransactionObjects.find(l_sRMNodeId);

	if (m_mapOfBTObjectsItr != m_mapOfBatchTransactionObjects.end()) {
		// so we found the existing rm node, check the pending id and update corresponding object pointers
		// first check whether we have any existing pending event id
		map<int, std::vector<HopsReturnObject*> >::iterator innerItr =
				m_mapOfBTObjectsItr->second.find(l_iPendingEventId);
		if (innerItr != m_mapOfBTObjectsItr->second.end()) {
			// so we already have pending event, just update it
			innerItr->second.push_back(_ptrReturnObject);
		} else {
			std::vector<HopsReturnObject*> arrayOfReturnObject;
			arrayOfReturnObject.push_back(_ptrReturnObject);
			m_mapOfBTObjectsItr->second.insert(
					std::make_pair<int, std::vector<HopsReturnObject *> >(
							l_iPendingEventId, arrayOfReturnObject));
		}
	} else {
		//create new one
		std::map<int, std::vector<HopsReturnObject*> > l_mapInnerOrderMap;
		std::vector<HopsReturnObject*> arrayOfReturnObject;
		arrayOfReturnObject.push_back(_ptrReturnObject);
		l_mapInnerOrderMap.insert(
				std::make_pair<int, std::vector<HopsReturnObject *> >(
						l_iPendingEventId, arrayOfReturnObject));
		m_mapOfBatchTransactionObjects.insert(
				std::make_pair<std::string,
						std::map<int, std::vector<HopsReturnObject*> > >(
						l_sRMNodeId, l_mapInnerOrderMap));
	}

	if (m_bIsReferenceTableProvided) {
		if (strcmp(_ptrReturnObject->GetTableName(), m_zReferenceTable) == 0) {
			m_itrPendingEvent = m_mapOfPendingEvents.find(l_sRMNodeId);
			if (m_itrPendingEvent != m_mapOfPendingEvents.end()) {
				m_itrPendingEvent->second.push_back(l_iPendingEventId);
			} else {
				std::vector<int> arrayOfPendingEvents;
				arrayOfPendingEvents.push_back(l_iPendingEventId);
				m_mapOfPendingEvents.insert(
						std::make_pair<std::string, std::vector<int> >(
								l_sRMNodeId, arrayOfPendingEvents));
			}
		}
	}
}
void HopsJNIDispatcher::ProcessAndFillTheData(
		HopsReturnObject *_ptrReturnObject, Uint64 _uTransactionId) {
	m_mapOfReturnObjectItr = m_mapOfReturnObject.begin();
	m_mapOfReturnObjectItr = m_mapOfReturnObject.find(_uTransactionId);
	if (m_mapOfReturnObjectItr != m_mapOfReturnObject.end()) { // We have the value in the map , so update it
		m_mapOfReturnObjectItr->second.push_back(_ptrReturnObject);
	} else {
		std::vector<HopsReturnObject*> arrayOfReturnObject;
		arrayOfReturnObject.push_back(_ptrReturnObject);

		m_mapOfReturnObject.insert(
				std::make_pair<Uint64, std::vector<HopsReturnObject *> >(
						_uTransactionId, arrayOfReturnObject));
	}
	if (m_bIsReferenceTableProvided) {
		if (strcmp(_ptrReturnObject->GetTableName(), m_zReferenceTable) == 0) {
			std::string l_sRMnodeId(
					_ptrReturnObject->m_listOfNdbValues[1].getCharValue());
			int l_iOriginalId =
					_ptrReturnObject->m_listOfNdbValues[0].getInt32Value();

			mapOfSortingItr = m_mapOfSortingObjects.find(l_sRMnodeId);
			if (mapOfSortingItr != m_mapOfSortingObjects.end()) {
				mapOfSortingItr->second.insert(
						std::make_pair<int, Uint64>(l_iOriginalId,
								_uTransactionId));
			} else {
				std::map<int, Uint64> l_mapInnerOrderMap;
				l_mapInnerOrderMap.insert(
						std::make_pair<int, Uint64>(l_iOriginalId,
								_uTransactionId));
				m_mapOfSortingObjects.insert(
						std::make_pair<std::string, std::map<int, Uint64> >(
								l_sRMnodeId, l_mapInnerOrderMap));
			}

		}
	}
}

// this case , we don't need to cleanup unwanted memory , because , there is not reference table
int HopsJNIDispatcher::SingleThreadDispatchWithoutReferenceTable() {

	int GlobalTotalTransactionCount = 0;

	m_mapOfReturnObjectItr = m_mapOfReturnObject.begin();
	for (; m_mapOfReturnObjectItr != m_mapOfReturnObject.end();
			++m_mapOfReturnObjectItr) {

		++GlobalTotalTransactionCount;

		for (int j = 0; j < (int) vecListJavaMethod.size(); ++j) {
			m_ptrJNI->CallVoidMethod(m_newCallBackObj, vecListJavaMethod[j]);
		}
		for (int i = 0; i < (int) m_mapOfReturnObjectItr->second.size(); ++i) {
			string sTablanme(m_mapOfReturnObjectItr->second[i]->GetTableName());
			int l_iPos = m_TablePositions[sTablanme];
			m_ptrHopsObjects[l_iPos]->BuildHopJavaObject(
					m_mapOfReturnObjectItr->second[i]->m_listOfNdbValues);
			m_ptrHopsObjects[l_iPos]->FireNewClassMethod();
			delete m_mapOfReturnObjectItr->second[i];
		}

		m_ptrJNI->CallVoidMethod(m_newCallBackObj,
				m_mdSingleThreadCallBackMethod);
		if (m_ptrJNI->ExceptionCheck()) {
			m_ptrJNI->ExceptionDescribe();
			m_ptrJNI->ExceptionClear();
		}

		m_mapOfReturnObjectItr->second.clear();
	}

	return GlobalTotalTransactionCount;

}

void HopsJNIDispatcher::ClearBatchMemory() {

	m_mapOfBTObjectsItr = m_mapOfBatchTransactionObjects.begin();
	for (; m_mapOfBTObjectsItr != m_mapOfBatchTransactionObjects.end();
			++m_mapOfBTObjectsItr) {
		std::map<int, std::vector<HopsReturnObject*> >::iterator l_innermapItr =
				m_mapOfBTObjectsItr->second.begin();
		// check whether we have any objects inside the vector array.
		for (; l_innermapItr != m_mapOfBTObjectsItr->second.end();
				++l_innermapItr) {
			if (l_innermapItr->second.size()) {
				//unwanted objects. remove them
				for (int i = 0; i < (int) l_innermapItr->second.size(); ++i) {
					delete l_innermapItr->second[i];
				}
			}
			l_innermapItr->second.clear();
		}
	}
	// objects are successfully dispatcher, clear the map for next fresh batch of events.
	m_mapOfBatchTransactionObjects.clear();
}
int HopsJNIDispatcher::SingleThreadBDWithOutRefTable() {
	int GlobalTotalTransactionCount = 0;

	m_mapOfBTObjectsItr = m_mapOfBatchTransactionObjects.begin();
	for (; m_mapOfBTObjectsItr != m_mapOfBatchTransactionObjects.end();
			++m_mapOfBTObjectsItr) {
		std::map<int, std::vector<HopsReturnObject*> >::iterator l_innermapItr =
				m_mapOfBTObjectsItr->second.begin();

		for (; l_innermapItr != m_mapOfBTObjectsItr->second.end();
				++l_innermapItr) {
			// go through all the pending event and dispatch , this will avoid unnecessary dispatch
			++GlobalTotalTransactionCount;
			///int l_iPendingEventId = l_innermapItr->first;
			for (int j = 0; j < (int) vecListJavaMethod.size(); ++j) {
				m_ptrJNI->CallVoidMethod(m_newCallBackObj,
						vecListJavaMethod[j]);
			}
			for (int i = 0; i < (int) l_innermapItr->second.size(); ++i) {
				string l_sTableName(l_innermapItr->second[i]->GetTableName());
				//	printf("table name - %s - pending id - %d \n",l_sTableName.c_str(), l_iPendingEventId);
				int l_iPos = m_TablePositions[l_sTableName];

				m_ptrHopsObjects[l_iPos]->BuildHopJavaObject(
						l_innermapItr->second[i]->m_listOfNdbValues);
				m_ptrHopsObjects[l_iPos]->FireNewClassMethod();
				delete l_innermapItr->second[i];
			}

			m_ptrJNI->CallVoidMethod(m_newCallBackObj,
					m_mdSingleThreadCallBackMethod);

			l_innermapItr->second.clear();
		}

		m_mapOfBTObjectsItr->second.clear();

	}
	ClearBatchMemory();
	return GlobalTotalTransactionCount;
}
int HopsJNIDispatcher::SingleThreadBDWithRefTable() {

	int GlobalTotalTransactionCount = 0;

	m_mapOfBTObjectsItr = m_mapOfBatchTransactionObjects.begin();
	for (; m_mapOfBTObjectsItr != m_mapOfBatchTransactionObjects.end();
			++m_mapOfBTObjectsItr) {
		std::map<int, std::vector<HopsReturnObject*> >::iterator l_innermapItr =
				m_mapOfBTObjectsItr->second.begin();

		m_itrPendingEvent = m_mapOfPendingEvents.find(
				m_mapOfBTObjectsItr->first);
		if (m_itrPendingEvent != m_mapOfPendingEvents.end()) {
			// so we found the rm node , lets dispatch
			// now sort the pending events
			std::sort(m_itrPendingEvent->second.begin(),
					m_itrPendingEvent->second.end());
			for (int i = 0; i < (int) m_itrPendingEvent->second.size(); ++i) {
				// go through all the pending event and dispatch , this will avoid unnecessary dispatch
				int l_iPendingEventId = m_itrPendingEvent->second[i];
				// now lets search the pending event id in the main map,

				l_innermapItr = m_mapOfBTObjectsItr->second.find(
						l_iPendingEventId);
				if (l_innermapItr != m_mapOfBTObjectsItr->second.end()) {
					++GlobalTotalTransactionCount;
					//int l_iPendingEventId = l_innermapItr->first;
					for (int j = 0; j < (int) vecListJavaMethod.size(); ++j) {
						m_ptrJNI->CallVoidMethod(m_newCallBackObj,
								vecListJavaMethod[j]);
					}
					for (int i = 0; i < (int) l_innermapItr->second.size();
							++i) {
						string l_sTableName(
								l_innermapItr->second[i]->GetTableName());
						int l_iPos = m_TablePositions[l_sTableName];
						m_ptrHopsObjects[l_iPos]->BuildHopJavaObject(
								l_innermapItr->second[i]->m_listOfNdbValues);
						m_ptrHopsObjects[l_iPos]->FireNewClassMethod();
						delete l_innermapItr->second[i];
					}

					m_ptrJNI->CallVoidMethod(m_newCallBackObj,
							m_mdSingleThreadCallBackMethod);
					//lets call the reset method here to clear the objects, so we can prepare the objects for next round
					m_ptrJNI->CallVoidMethod(m_newCallBackObj, m_mdResetMethod);

					l_innermapItr->second.clear();
				}
			}
		}

		m_mapOfBTObjectsItr->second.clear();
	}
	ClearBatchMemory();
	return GlobalTotalTransactionCount;
}

int HopsJNIDispatcher::SingleThreadDispatch() {

	int GlobalTotalTransactionCount = 0;

	mapOfSortingItr = m_mapOfSortingObjects.begin();
	for (; mapOfSortingItr != m_mapOfSortingObjects.end(); ++mapOfSortingItr) {
		std::map<int, Uint64>::iterator l_innermapItr =
				mapOfSortingItr->second.begin();

		for (; l_innermapItr != mapOfSortingItr->second.end();
				++l_innermapItr) {
			++GlobalTotalTransactionCount;
			Uint64 l_transactionId = l_innermapItr->second;
			m_mapOfReturnObjectItr = m_mapOfReturnObject.begin();
			m_mapOfReturnObjectItr = m_mapOfReturnObject.find(l_transactionId);

			for (int j = 0; j < (int) vecListJavaMethod.size(); ++j) {
				m_ptrJNI->CallVoidMethod(m_newCallBackObj,
						vecListJavaMethod[j]);
			}
			for (int i = 0; i < (int) m_mapOfReturnObjectItr->second.size();
					++i) {
				string sTablanme(
						m_mapOfReturnObjectItr->second[i]->GetTableName());
				int l_iPos = m_TablePositions[sTablanme];

				m_ptrHopsObjects[l_iPos]->BuildHopJavaObject(
						m_mapOfReturnObjectItr->second[i]->m_listOfNdbValues);
				m_ptrHopsObjects[l_iPos]->FireNewClassMethod();
				delete m_mapOfReturnObjectItr->second[i];
			}

			m_ptrJNI->CallVoidMethod(m_newCallBackObj,
					m_mdSingleThreadCallBackMethod);

			m_mapOfReturnObjectItr->second.clear();
		}
		mapOfSortingItr->second.clear();

	}
	CleanUpUnwantedObjectMemory();
	return GlobalTotalTransactionCount;

}
int HopsJNIDispatcher::PreprocessJavaObjectsWithoutReferenceTable(
		vector<jobject> &_vecJObject) {
	int GlobalTotalTransactionCount = 0;

	m_mapOfReturnObjectItr = m_mapOfReturnObject.begin();
	for (; m_mapOfReturnObjectItr != m_mapOfReturnObject.end();
			++m_mapOfReturnObjectItr) {

		++GlobalTotalTransactionCount;

		for (int j = 0; j < (int) vecListJavaMethod.size(); ++j) {
			m_ptrJNI->CallVoidMethod(m_newCallBackObj, vecListJavaMethod[j]);
		}
		for (int i = 0; i < (int) m_mapOfReturnObjectItr->second.size(); ++i) {
			string sTablanme(m_mapOfReturnObjectItr->second[i]->GetTableName());
			int l_iPos = m_TablePositions[sTablanme];
			m_ptrHopsObjects[l_iPos]->BuildHopJavaObject(
					m_mapOfReturnObjectItr->second[i]->m_listOfNdbValues);
			m_ptrHopsObjects[l_iPos]->FireNewClassMethod();
			delete m_mapOfReturnObjectItr->second[i];
		}

		jobject jobj = m_ptrJNI->CallObjectMethod(m_newCallBackObj,
				m_mdBuildCompositeMethod);
		if (m_ptrJNI->ExceptionCheck()) {
			m_ptrJNI->ExceptionDescribe();
			m_ptrJNI->ExceptionClear();
		}
		_vecJObject.push_back(jobj);

		m_mapOfReturnObjectItr->second.clear();
	}

	return GlobalTotalTransactionCount;

}
int HopsJNIDispatcher::PreprocessJavaObjects(vector<jobject> & _vecJObject) {

	int GlobalTotalTransactionCount = 0;

	mapOfSortingItr = m_mapOfSortingObjects.begin();
	for (; mapOfSortingItr != m_mapOfSortingObjects.end(); ++mapOfSortingItr) {
		std::map<int, Uint64>::iterator l_innermapItr =
				mapOfSortingItr->second.begin();

		for (; l_innermapItr != mapOfSortingItr->second.end();
				++l_innermapItr) {
			++GlobalTotalTransactionCount;
			Uint64 l_transactionId = l_innermapItr->second;
			m_mapOfReturnObjectItr = m_mapOfReturnObject.begin();
			m_mapOfReturnObjectItr = m_mapOfReturnObject.find(l_transactionId);

			for (int j = 0; j < (int) vecListJavaMethod.size(); ++j) {
				m_ptrJNI->CallVoidMethod(m_newCallBackObj,
						vecListJavaMethod[j]);
			}
			for (int i = 0; i < (int) m_mapOfReturnObjectItr->second.size();
					++i) {
				string sTablanme(
						m_mapOfReturnObjectItr->second[i]->GetTableName());
				int l_iPos = m_TablePositions[sTablanme];

				m_ptrHopsObjects[l_iPos]->BuildHopJavaObject(
						m_mapOfReturnObjectItr->second[i]->m_listOfNdbValues);
				m_ptrHopsObjects[l_iPos]->FireNewClassMethod();
				delete m_mapOfReturnObjectItr->second[i];
			}

			jobject jobj = m_ptrJNI->CallObjectMethod(m_newCallBackObj,
					m_mdBuildCompositeMethod);
			_vecJObject.push_back(jobj);

			m_mapOfReturnObjectItr->second.clear();
		}
		mapOfSortingItr->second.clear();

	}
	CleanUpUnwantedObjectMemory();
	return GlobalTotalTransactionCount;

}

int HopsJNIDispatcher::MultiThreadedDispatch(vector<jobject> &classObjects) {

	int l_iClassObjects = (int) classObjects.size();
	for (int i = 0; i < l_iClassObjects; ++i) {

		if (m_mdMultiThreadCallBackMethod != NULL) {
			m_ptrJNI->CallVoidMethod(m_newCallBackObj,
					m_mdMultiThreadCallBackMethod, classObjects[i]);
			m_ptrJNI->DeleteLocalRef(classObjects[i]);
		} else {
			printf(
					"[HopsJNIDispatcher][WARING] ### Exception occurred during callback ##### \n");
			if (m_ptrJNI->ExceptionCheck()) {
				m_ptrJNI->ExceptionDescribe();
				m_ptrJNI->ExceptionClear();
			}
		}
	}
	classObjects.clear();
	return l_iClassObjects;

}

