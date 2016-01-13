/*
 * HopsLoadSimulationJNIDispatcher.h
 *
 *  Created on: Apr 7, 2015
 *      Author: sri
 */

#ifndef HOPSLOADSIMULATIONJNIDISPATCHER_H_
#define HOPSLOADSIMULATIONJNIDISPATCHER_H_

#include <pthread.h>
#include <vector>
#include <jni.h>
#include <sstream>
#include <fstream>

#include "HopsUtils.h"
#include "HopsJNICallBackObject.h"
#include "HopsReturnObject.h"
#include "HopsEventThreadData.h"
#include "NdbApi.hpp"
#include "HopsConfigReader.h"

using namespace cnf;
using namespace hops::utl::que;
using namespace hops::utl;
using namespace hopsjni;

class HopsLoadSimulationJNIDispatcher {
public:
	HopsLoadSimulationJNIDispatcher();  // Private so that it can  not be called
	virtual ~HopsLoadSimulationJNIDispatcher();
	void InintLoadSimulationJNIDispatcher(
			HopsEventQueueFrame *_ptrJavaObjectDispatcher, JavaVM *_ptrJVM,
			QueueSizeCondition *_ptrCondtionLock);
	static void * Run(void * _pProcessor);
	pthread_t StartEventProcessor(
			HopsLoadSimulationJNIDispatcher *_ptrHopsJNIDispatcher,
			HopsLoadSimulationJNIDispatcher *_ptrFriendDispatcher,
			ThreadToken *_ptrThreadToken,HopsConfigFile *_ptrConf);
	void StopProcessorThread() {
		m_bIsIinterrupt = true;
	}
	void InitializeTablePosition(std::string _sTableName);

	inline void SetSingleThread(bool _isSingleThread) {
		m_isSingleThread = _isSingleThread;
	}
	inline bool isThisSingleThread() {
		return m_isSingleThread;
	}
private:

	void PrintJNIPlainMessage(int _iCategory,
			const char *_pzMessage);
	int processQ();
	int dispatch();
	void StartProcesser();
	void AttachAgain();
	bool CleanUpUnwantedObjectMemory();
	void PrepareAllJNIVariables();

	int SingleThreadDispatch();
	int ProcessQ();
	void WarmUpJavaObjectConfiguration();
	void MultiThreadedDispatch(vector<jobject> &classObjects);
	int PreprocessJavaObjects(vector<jobject> & _vecJObject);
	void ProcessAndFillTheData(
			HopsSimulationReturnObject *_ptrSimulationReturnObject,
			Uint64 _uTransactionId);
	void ClearDataStructures();

	pthread_t m_threadid;

	bool m_isSingleThread;
	bool m_bIsThisFirstTime;
	bool m_bIsPrintEnabled;
	volatile bool m_bIsIinterrupt;

	JavaVM *m_jvm;
	JNIEnv *m_ptrJNI;

	jmethodID m_mdBuildCompositeMethod;
	jmethodID m_mdMultiThreadCallBackMethod;
	jclass m_jniClassGlobalRef;
	jmethodID m_mdSingleThreadJavaMethod;
	jobject m_newCallBackObj;

	HopObject **m_ptrHopsObjects;
	QueueSizeCondition *m_ptrCondtionLock;
	HopsEventStreamingTimer *m_ptrSleepTimer;
	ThreadToken *m_ptrThreadToken;
	HopsConfigFile *m_ptrConf;
	HopsLoadSimulationJNIDispatcher *m_ptrNeighbourDispatcher;
	HopsEventQueueFrame *m_ptrJavaObjectDispatcherQ;

	int m_iInternalGCIIndex;
	int m_iTablePositionOffset;
	int m_iSingleContainerSize;

	unsigned long long m_ullPreviousDispatchTime;

	struct timeval m_tPerformanceTime;

	std::map<Uint64, std::vector<HopsSimulationReturnObject *> > m_mapOfReturnObject;
	std::map<Uint64, std::vector<HopsSimulationReturnObject *> >::iterator m_mapOfReturnObjectItr;

	std::map<std::string, std::map<int, Uint64> > mapOfSortingObjects;
	std::map<std::string, std::map<int, Uint64> >::iterator mapOfSortingItr;

	std::map<std::string, const char *> mapTableNameToFunctionSig;
	std::map<std::string, std::string> mapTableNameFunctionName;
	std::map<std::string, std::string> mapJavaContainerBuild;
	std::map<std::string, int> m_TablePositions;
	std::map<int, int> m_mapOfJavaClassAndPos;

	//TODO how do we know the size , change to this pointer
	char m_zReferenceTable[100];
	char m_zCallBackClassName[400];

	char m_zSingleThreadCallBackMethod[400];
	char m_zSingleThreadCallBackMethodSig[400];

	char m_zMultiThreadCallBackMethodName[400];
	char m_zMultiThreadCallBackMethodSig[400];

	char m_zMultiThreadBuildCallBackMethod[400];
	char m_zMultiThreadBuildClassBackMethodSig[400];

};
#endif /* HOPSLOADSIMULATIONJNIDISPATCHER_H_ */
