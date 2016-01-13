//============================================================================
// Name        : HopsLoadSimulationJNIDispatcher.cpp
// Created on  : FApr 7, 2015
// Author      : sri(skug@kth.se)
// Version     : 1.0
// Copyright   : SICS
// Description : Event processor thread is processing and dispatching the objects to java
//============================================================================

#include <sys/time.h>
#include <exception>
#include <pthread.h>
#include <limits.h>
#include <stdlib.h>
#include <sys/utsname.h>
#include "../include/HopsEventThreadData.h"
#include "../include/HopsLoadSimulationJNIDispatcher.h"


#define SECONDS      1000000000
#define MILLISECONDS 1000000
#define MICROSECONDS 1000
#define NANOSECONDS  1

using namespace hops::utl;
using namespace cnf;
HopsLoadSimulationJNIDispatcher::HopsLoadSimulationJNIDispatcher() {

	m_jvm = NULL;
	m_newCallBackObj = NULL;
	m_ptrNeighbourDispatcher = NULL;
	m_mdBuildCompositeMethod = NULL;
	m_ptrThreadToken = NULL;
	m_ptrJNI = NULL;
	m_ptrCondtionLock = NULL;
	m_ptrHopsObjects = NULL;
	m_ptrSleepTimer = NULL;
	m_jniClassGlobalRef = NULL;
	m_ptrJavaObjectDispatcherQ = NULL;
	m_mdSingleThreadJavaMethod = NULL;
	m_mdMultiThreadCallBackMethod = NULL;
	m_ptrConf=NULL;

	m_bIsIinterrupt = false;
	m_bIsThisFirstTime = false;
	m_isSingleThread = false;
	m_bIsPrintEnabled = false;

	m_iTablePositionOffset = 0;
	m_iSingleContainerSize = 0;
	m_ullPreviousDispatchTime = 0;
	m_iInternalGCIIndex = 0;
	m_threadid = 0;

}
HopsLoadSimulationJNIDispatcher::~HopsLoadSimulationJNIDispatcher() {
	std::cout
			<< "[HopsLoadSimulationJNIDispatcher] Deallocating the memory now "
			<< std::endl;
	delete m_ptrSleepTimer;
}
void HopsLoadSimulationJNIDispatcher::InintLoadSimulationJNIDispatcher(
		HopsEventQueueFrame *_ptrJavaObjectDispatcher, JavaVM *_ptrJVM,
		QueueSizeCondition *_ptrCondtionLock) {

	m_ptrJavaObjectDispatcherQ = _ptrJavaObjectDispatcher;
	m_jvm = _ptrJVM;
	m_ptrSleepTimer = new HopsEventStreamingTimer(1, false);
	m_ptrCondtionLock = _ptrCondtionLock;

}
void *HopsLoadSimulationJNIDispatcher::Run(void * _pLHandler) {
	((HopsLoadSimulationJNIDispatcher*) _pLHandler)->StartProcesser();
	return NULL;
}

pthread_t HopsLoadSimulationJNIDispatcher::StartEventProcessor(
		HopsLoadSimulationJNIDispatcher *_ptrHopsJNIDispatcher,
		HopsLoadSimulationJNIDispatcher *_ptrFriendDispatcher,
		ThreadToken *_ptrThreadToken,HopsConfigFile *_ptrConf) {

	m_ptrConf=_ptrConf;
	m_ptrThreadToken = _ptrThreadToken;

	pthread_create(&m_threadid, NULL,
			(void*(*)(void*)) HopsLoadSimulationJNIDispatcher::Run, (void*) _ptrHopsJNIDispatcher);
	printf(
			"[HopsLoadSimulationJNIDispatcher][INFO] ############  Event processor thread id -  %li \n",
			m_threadid);

	m_ptrNeighbourDispatcher = _ptrFriendDispatcher;
	return m_threadid;
}

void HopsLoadSimulationJNIDispatcher::InitializeTablePosition(
		std::string _sTableName) {
	m_TablePositions.insert(
			std::make_pair<std::string, int>(_sTableName,
					m_iTablePositionOffset));
	++m_iTablePositionOffset;
}
void HopsLoadSimulationJNIDispatcher::PrintJNIPlainMessage(int _iCategory,
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
void HopsLoadSimulationJNIDispatcher::WarmUpJavaObjectConfiguration() {

	m_iSingleContainerSize = (int) atoi(
			m_ptrConf->GetValue("SINGLE_CONTAINER_SIZE"));
	m_bIsPrintEnabled =
			(int) atoi(m_ptrConf->GetValue("PRINT_ENABLED")) == 1 ? true : false;
	char l_zConfigReaderArray[1024];
	char l_zCallBackReaderArray[1024];
	sprintf(l_zConfigReaderArray, "SIMULATION_REFERENCE_TABLE_NAME");
	strcpy(m_zReferenceTable, m_ptrConf->GetValue(l_zConfigReaderArray));

	int l_iTotalNumberOfClasses = (int) atoi(
			m_ptrConf->GetValue("SIMULATION_TOTAL_NUMBER_OF_CLASSES"));

	if (l_iTotalNumberOfClasses == 0) {
		printf(
				"[HopsLoadSimulationJNIDispatcher][FAILED] ########### Number of classes can not be null");
		exit(EXIT_FAILURE);
	}

	memset(l_zConfigReaderArray, 0, sizeof(l_zConfigReaderArray));
	memset(m_zCallBackClassName, 0, sizeof(m_zCallBackClassName));
	sprintf(l_zConfigReaderArray, "SIMULATION_CALLBACK_CLASS_NAME");
	strcpy(m_zCallBackClassName, m_ptrConf->GetValue(l_zConfigReaderArray));

	if (m_bIsPrintEnabled) {
		printf(
				"[HopsLoadSimulationJNIDispatcher][INFO] ########### Callback class-name 		   : %s\n",
				m_zCallBackClassName);
	}
	memset(l_zConfigReaderArray, 0, sizeof(l_zConfigReaderArray));
	sprintf(l_zConfigReaderArray, "SIMULATION_SINGLE_THREAD_CALLBACK_METHOD");
	strcpy(l_zCallBackReaderArray, m_ptrConf->GetValue(l_zConfigReaderArray));
	HopsStringTokenizer l_oListSepSingleThreadCallBack(l_zCallBackReaderArray,
			'|');

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
				"[HopsLoadSimulationJNIDispatcher][INFO] ########### Callback method 		   : %s\n",
				m_zSingleThreadCallBackMethod);
		printf(
				"[HopsLoadSimulationJNIDispatcher][INFO] ########### Callback method signature : %s\n",
				m_zSingleThreadCallBackMethodSig);
	}

	jclass l_callbackClass = m_ptrJNI->FindClass(m_zCallBackClassName);

	if (l_callbackClass == NULL) {
		printf(
				"[HopsLoadSimulationJNIDispatcher][FAILED] ########### Callback class not found");
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
	m_ptrHopsObjects = new HopObject *[l_iTotalNumberOfClasses];
	for (int m = 0; m < l_iTotalNumberOfClasses; ++m) {
		char l_zBuff[40]; //this is fixed and it won't make any problem
		memset(l_zBuff, 0, sizeof(l_zBuff));
		sprintf(l_zBuff, "SIMULATION_JAVA_CLASS_NAME_%d", m + 1);
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
	memset(l_zCallBackReaderArray, 0, sizeof(l_zCallBackReaderArray));
	sprintf(l_zConfigReaderArray, "SIMULATION_MULTI_THREAD_CLASS_BUILDER_NAME");
	strcpy(l_zCallBackReaderArray, m_ptrConf->GetValue(l_zConfigReaderArray));

	HopsStringTokenizer l_oListSepMTBuildCallBack(l_zCallBackReaderArray, '|'); // this separator helps to extract the load deviation

	memset(m_zMultiThreadBuildCallBackMethod, 0,
			sizeof(m_zSingleThreadCallBackMethod));
	memset(m_zMultiThreadBuildClassBackMethodSig, 0,
			sizeof(m_zSingleThreadCallBackMethodSig));

	strcpy(m_zMultiThreadBuildCallBackMethod,
			l_oListSepMTBuildCallBack.GetTokenAt(0));
	strcpy(m_zMultiThreadBuildClassBackMethodSig,
			l_oListSepMTBuildCallBack.GetTokenAt(1));

	memset(l_zConfigReaderArray, 0, sizeof(l_zConfigReaderArray));
	sprintf(l_zConfigReaderArray, "SIMULATION_MULTI_THREAD_CALLBACK_METHOD");
	strcpy(l_zCallBackReaderArray, m_ptrConf->GetValue(l_zConfigReaderArray));

	HopsStringTokenizer l_oListSepMTCallBack(l_zCallBackReaderArray, '|'); // this separator helps to extract the load deviation

	memset(m_zMultiThreadCallBackMethodName, 0,
			sizeof(m_zSingleThreadCallBackMethod));
	memset(m_zMultiThreadCallBackMethodSig, 0,
			sizeof(m_zSingleThreadCallBackMethodSig));

	strcpy(m_zMultiThreadCallBackMethodName,
			l_oListSepMTCallBack.GetTokenAt(0));
	strcpy(m_zMultiThreadCallBackMethodSig, l_oListSepMTCallBack.GetTokenAt(1));

	m_mdMultiThreadCallBackMethod = m_ptrJNI->GetMethodID(m_jniClassGlobalRef,
			m_zMultiThreadCallBackMethodName, m_zMultiThreadCallBackMethodSig);

	if (m_mdMultiThreadCallBackMethod == NULL) {
		PrintJNIPlainMessage(2,
				"Exception occurred in finding load simulation callback method");

	}

	m_mdBuildCompositeMethod = m_ptrJNI->GetMethodID(m_jniClassGlobalRef,
			m_zMultiThreadBuildCallBackMethod,
			m_zMultiThreadBuildClassBackMethodSig);

	if (m_mdBuildCompositeMethod == NULL) {
		PrintJNIPlainMessage(2,
				"Exception occurred in finding in build composite Java method");
	}

	m_mdSingleThreadJavaMethod = m_ptrJNI->GetMethodID(m_jniClassGlobalRef,
			m_zSingleThreadCallBackMethod, m_zSingleThreadCallBackMethodSig);

	if (m_mdSingleThreadJavaMethod == NULL) {
		PrintJNIPlainMessage(2,
				"Exception occurred in finding Single thread java method");
	}

	if (m_bIsPrintEnabled) {
		printf(
				"[HopsLoadSimulationJNIDispatcher][INFO] ########### builder class method 		   : %s\n",
				m_zMultiThreadBuildCallBackMethod);
		printf(
				"[HopsLoadSimulationJNIDispatcher][INFO] ########### builder class method sig : %s\n",
				m_zMultiThreadBuildClassBackMethodSig);

		printf(
				"[HopsLoadSimulationJNIDispatcher][INFO] ########### Callback method 		   : %s\n",
				m_zMultiThreadCallBackMethodName);
		printf(
				"[HopsLoadSimulationJNIDispatcher][INFO] ########### Callback method signature : %s\n",
				m_zMultiThreadCallBackMethodSig);

	}

}

void HopsLoadSimulationJNIDispatcher::PrepareAllJNIVariables() {
	jint l_jResult;

	l_jResult = m_jvm->AttachCurrentThread((void **) &m_ptrJNI, NULL);
	if (l_jResult != JNI_OK) {
		printf(
				"[HopsLoadSimulationJNIDispatcher][FAILED] ########### Failed to attach with Java thread - %li\n",
				pthread_self());
		exit(EXIT_FAILURE);
	} else {
		printf(
				"[HopsLoadSimulationJNIDispatcher][INFO] ########### Successfully attached native thread - %li\n",
				pthread_self());
		WarmUpJavaObjectConfiguration();
	}

}

void HopsLoadSimulationJNIDispatcher::StartProcesser() {

	PrepareAllJNIVariables();

	while (true) {
		int l_iPrcoeedSize = ProcessQ();
		m_ptrCondtionLock->DecreaseQueueSize(l_iPrcoeedSize);
		if (m_bIsIinterrupt) {
			jint l_jResult = m_jvm->DetachCurrentThread();
			if (l_jResult != JNI_OK) {
				printf(
						"[HopsLoadSimulationJNIDispatcher][FAILED] ########### Failed to detach native thread  - %li\n",
						pthread_self());
			} else {
				printf(
						"[HopsLoadSimulationJNIDispatcher][INFO] ########### Successfully  detach native thread  - %li\n",
						pthread_self());
			}
			pthread_exit(NULL);
		}
	}
}

int HopsLoadSimulationJNIDispatcher::ProcessQ() {
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
			//time to dispatch
			if (CleanUpUnwantedObjectMemory()) {
				ClearDataStructures();
			} else {
				if (!isThisSingleThread()) {
					vector<jobject> l_vecJavaTempObject;

					PreprocessJavaObjects(l_vecJavaTempObject);

					if (m_iInternalGCIIndex > 0) {
						m_ptrThreadToken->WaitForSignal();
					}

					MultiThreadedDispatch(l_vecJavaTempObject);
					unsigned long long l_FinishTime =
							m_ptrSleepTimer->GetEpochTime();
					printf("%lld\n", l_FinishTime);

					m_ptrNeighbourDispatcher->m_ptrThreadToken->SendSignal();

				} else {
					SingleThreadDispatch();
					unsigned long long l_FinishTime =
							m_ptrSleepTimer->GetEpochTime();
					unsigned long long duration = l_FinishTime
							- m_ullPreviousDispatchTime;
					printf("%lld\n", duration);
					m_ullPreviousDispatchTime = l_FinishTime;
				}

				ClearDataStructures();

			}
			m_iInternalGCIIndex = _pMsg->GetGCIIndexValue();
		}

		ProcessAndFillTheData(_pMsg->GetSimulationReturnObject(),
				_pMsg->getTransactionId());

		delete _pMsg;
		delete pCont;
		pCont = NULL;
	}
	return l_iProcessedMsg * m_iSingleContainerSize;
}

void HopsLoadSimulationJNIDispatcher::ClearDataStructures() {
	m_mapOfReturnObject.clear();
	mapOfSortingObjects.clear();
}
bool HopsLoadSimulationJNIDispatcher::CleanUpUnwantedObjectMemory() {
	if (m_mapOfReturnObject.size() == 0 && m_mapOfReturnObject.size() != 0) {
		//we need to clean up the memory , there is no element to dispatch
		printf(
				"[HopsLoadSimulationJNIDispatcher][INFO] ########### Can't dispatch objects, because, we have an objects from other tables but not from reference table ############# \n");
		for (m_mapOfReturnObjectItr = m_mapOfReturnObject.begin();
				m_mapOfReturnObjectItr != m_mapOfReturnObject.end();
				++m_mapOfReturnObjectItr) {
			for (int i = 0; i < (int) m_mapOfReturnObjectItr->second.size();
					++i) {
				delete m_mapOfReturnObjectItr->second[i];
			}
			m_mapOfReturnObjectItr->second.clear();
			m_mapOfReturnObject.erase(m_mapOfReturnObjectItr);
		}
		printf(
				"[HopsLoadSimulationJNIDispatcher][INFO] ########### Check the size, it should be zero - %d ############# \n",
				(int) m_mapOfReturnObject.size());
		return true;
	} else
		return false;

}
void HopsLoadSimulationJNIDispatcher::ProcessAndFillTheData(
		HopsSimulationReturnObject *_ptrSimulationReturnObject,
		Uint64 _uTransactionId) {
	m_mapOfReturnObjectItr = m_mapOfReturnObject.begin();
	m_mapOfReturnObjectItr = m_mapOfReturnObject.find(_uTransactionId);
	if (m_mapOfReturnObjectItr != m_mapOfReturnObject.end()) { // We have the value in the map , so update it
		m_mapOfReturnObjectItr->second.push_back(_ptrSimulationReturnObject);
	} else {
		std::vector<HopsSimulationReturnObject*> arrayOfReturnObject;
		arrayOfReturnObject.push_back(_ptrSimulationReturnObject);

		m_mapOfReturnObject.insert(
				std::make_pair<Uint64, std::vector<HopsSimulationReturnObject *> >(
						_uTransactionId, arrayOfReturnObject));
	}
	if (strcmp(_ptrSimulationReturnObject->GetTableName(), m_zReferenceTable)
			== 0) {

		std::string l_sRMnodeId(
				_ptrSimulationReturnObject->m_listOfNdbValues[1].getCharValue());
		int l_iOriginalId =
				_ptrSimulationReturnObject->m_listOfNdbValues[0].getInt32Value();

		mapOfSortingItr = mapOfSortingObjects.find(l_sRMnodeId);
		if (mapOfSortingItr != mapOfSortingObjects.end()) {
			mapOfSortingItr->second.insert(
					std::make_pair<int, Uint64>(l_iOriginalId,
							_uTransactionId));
		} else {
			std::map<int, Uint64> l_mapInnerOrderMap;
			l_mapInnerOrderMap.insert(
					std::make_pair<int, Uint64>(l_iOriginalId,
							_uTransactionId));
			mapOfSortingObjects.insert(
					std::make_pair<std::string, std::map<int, Uint64> >(
							l_sRMnodeId, l_mapInnerOrderMap));
		}

	}
}

int HopsLoadSimulationJNIDispatcher::SingleThreadDispatch() {

	int GlobalTotalTransactionCount = 0;

	mapOfSortingItr = mapOfSortingObjects.begin();
	for (; mapOfSortingItr != mapOfSortingObjects.end(); ++mapOfSortingItr) {
		std::map<int, Uint64>::iterator l_innermapItr =
				mapOfSortingItr->second.begin();

		for (; l_innermapItr != mapOfSortingItr->second.end();
				++l_innermapItr) {
			++GlobalTotalTransactionCount;
			Uint64 l_transactionId = l_innermapItr->second;
			m_mapOfReturnObjectItr = m_mapOfReturnObject.begin();
			m_mapOfReturnObjectItr = m_mapOfReturnObject.find(l_transactionId);

			for (int i = 0; i < (int) m_mapOfReturnObjectItr->second.size();
					++i) {
				string sTablanme(
						m_mapOfReturnObjectItr->second[i]->GetTableName());
				int l_iPos = m_TablePositions[sTablanme];

				m_ptrHopsObjects[l_iPos]->SimulationBuildHopJavaObject(
						m_mapOfReturnObjectItr->second[i]->m_listOfNdbValues);
				m_ptrHopsObjects[l_iPos]->FireNewClassMethod();
				delete m_mapOfReturnObjectItr->second[i];
			}

			m_ptrJNI->CallVoidMethod(m_newCallBackObj,
					m_mdSingleThreadJavaMethod);

			m_mapOfReturnObjectItr->second.clear();
		}
		mapOfSortingItr->second.clear();
		mapOfSortingObjects.erase(mapOfSortingItr);

	}

	mapOfSortingObjects.clear();
	return GlobalTotalTransactionCount;

}

int HopsLoadSimulationJNIDispatcher::PreprocessJavaObjects(
		vector<jobject> & _vecJObject) {

	int GlobalTotalTransactionCount = 0;

	mapOfSortingItr = mapOfSortingObjects.begin();
	for (; mapOfSortingItr != mapOfSortingObjects.end(); ++mapOfSortingItr) {
		std::map<int, Uint64>::iterator l_innermapItr =
				mapOfSortingItr->second.begin();

		for (; l_innermapItr != mapOfSortingItr->second.end();
				++l_innermapItr) {
			++GlobalTotalTransactionCount;
			Uint64 l_transactionId = l_innermapItr->second;
			m_mapOfReturnObjectItr = m_mapOfReturnObject.begin();
			m_mapOfReturnObjectItr = m_mapOfReturnObject.find(l_transactionId);

			for (int i = 0; i < (int) m_mapOfReturnObjectItr->second.size();
					++i) {
				string sTablanme(
						m_mapOfReturnObjectItr->second[i]->GetTableName());
				int l_iPos = m_TablePositions[sTablanme];

				m_ptrHopsObjects[l_iPos]->SimulationBuildHopJavaObject(
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
		mapOfSortingObjects.erase(mapOfSortingItr);

	}

	mapOfSortingObjects.clear();
	return GlobalTotalTransactionCount;

}

void HopsLoadSimulationJNIDispatcher::MultiThreadedDispatch(
		vector<jobject> &classObjects) {

	for (int i = 0; i < (int) classObjects.size(); ++i) {

		if (m_mdMultiThreadCallBackMethod != NULL) {
			m_ptrJNI->CallVoidMethod(m_newCallBackObj,
					m_mdMultiThreadCallBackMethod, classObjects[i]);
			m_ptrJNI->DeleteLocalRef(classObjects[i]);
		} else {
			printf(
					"[HopsLoadSimulationJNIDispatcher][WARING] ### Exception occurred during callback ##### \n");
			if (m_ptrJNI->ExceptionCheck()) {
				m_ptrJNI->ExceptionDescribe();
				m_ptrJNI->ExceptionClear();
			}
		}

	}

	classObjects.clear();

}

