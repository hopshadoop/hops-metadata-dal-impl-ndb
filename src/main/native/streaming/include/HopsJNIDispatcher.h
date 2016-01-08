/*
 * EventProcessor.h
 *
 *  Created on: Feb 2, 2015
 *      Author: sri
 */

#ifndef HOPSEVENTPROCESSOR_H_
#define HOPSEVENTPROCESSOR_H_
#include <pthread.h>
#include <vector>
#include <jni.h>
#include <sstream>
#include <fstream>

#include "NdbApi.hpp"
#include "HopsConfigReader.h"
#include "HopsUtils.h"
#include "HopsJNICallBackObject.h"
#include "HopsReturnObject.h"
#include "HopsEventThreadData.h"

using namespace cnf;
using namespace hops::utl::que;
using namespace hops::utl;
using namespace hopsjni;

class HopsJNIDispatcher {
public:
	HopsJNIDispatcher();  // Private so that it can  not be called
	virtual ~HopsJNIDispatcher();
	void InintJNIDispatcher(HopsEventQueueFrame *_ptrJavaObjectDispatcher,
			JavaVM *_ptrJVM, QueueSizeCondition *_ptrCondtionLock,int _iThreadSingleContainerSize);
	static void * Run(void * _pProcessor);
	pthread_t StartEventProcessor(HopsJNIDispatcher *_ptrHopsJNIDispatcher,
			HopsJNIDispatcher *_ptrFriendDispatcher,
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
	void StopDispatcher();
private:

	bool IsMyPredecessorProcessed();
	int processQ();
	int dispatch();
	void StartProcesser();
	void AttachAgain();
	void CleanUpUnwantedObjectMemory();
	void PrepareAllJNIVariables();
	int SingleThreadDispatch();
	int SingleThreadDispatchWithoutReferenceTable();
	void WarmUpJavaObjectConfiguration();
	int MultiThreadedDispatch(vector<jobject> &classObjects);
	int PreprocessJavaObjects(vector<jobject> & _vecJObject);
	int PreprocessJavaObjectsWithoutReferenceTable(vector<jobject> & _vecJObject);
	void ProcessAndFillTheData(HopsReturnObject *_ptrReturnObject,
			Uint64 _uTransactionId);
	void ProcessAndFillBatchData(HopsReturnObject *_ptrReturnObject,Uint64 _uTransactionId);
	void ClearDataStructures();
	void PrintJNIPlainMessage(int _iCategory, const char *_pzMessage);
    int SingleThreadBDWithRefTable();
    void ClearBatchMemory();
    int SingleThreadBDWithOutRefTable();
	ThreadToken *m_ptrThreadToken;
	HopsConfigFile *m_ptrConf;
	HopsJNIDispatcher *m_ptrNeighbourDispatcher;
	HopsEventQueueFrame *m_ptrJavaObjectDispatcherQ;
	HopObject **m_ptrHopsObjects;
	HopsEventStreamingTimer *m_ptrSleepTimer;
	QueueSizeCondition *m_ptrCondtionLock;

	JavaVM *m_jvm;
	JNIEnv *m_ptrJNI;
	jmethodID m_mdMultiThreadCallBackMethod;
	jmethodID m_mdBuildCompositeMethod;
	jmethodID m_mdSingleThreadCallBackMethod;
	jmethodID m_mdResetMethod;	
	jclass m_jniClassGlobalRef;
	jobject m_newCallBackObj;

	pthread_t m_threadid;

	int m_iSingleContainerSize;
	int m_iTablePositionOffset;
	int m_iInternalGCIIndex;

	unsigned long long m_ullPreviousDispatchTime;

	bool m_isSingleThread;
	bool m_bIsThisFirstTime;
	bool m_bIsPrintEnabled;
	bool m_bIsReferenceTableProvided;
	volatile bool m_bIsIinterrupt;

	struct timeval m_tPerformanceTime;

	std::map<Uint64, std::vector<HopsReturnObject *> > m_mapOfReturnObject;
	std::map<Uint64, std::vector<HopsReturnObject *> >::iterator m_mapOfReturnObjectItr;

	std::map<std::string, std::map<int, Uint64> > m_mapOfSortingObjects;
	std::map<std::string, std::map<int, Uint64> >::iterator mapOfSortingItr;

	std::map<std::string,std::vector<int> > m_mapOfPendingEvents;
	std::map<std::string,std::vector<int> >::iterator m_itrPendingEvent;


	std::map<std::string, std::map <int,std::vector<HopsReturnObject* > > > m_mapOfBatchTransactionObjects;
	std::map<std::string, std::map <int,std::vector<HopsReturnObject* > > >::iterator m_mapOfBTObjectsItr;


	std::map<std::string, const char *> mapTableNameToFunctionSig;
	std::map<std::string, std::string> mapTableNameFunctionName;
	std::map<std::string, std::string> mapJavaContainerBuild;
	std::map<std::string, int> m_TablePositions;
	std::map<int, int> m_mapOfJavaClassAndPos;
	std::vector<jmethodID> vecListJavaMethod;

	//TODO how do we know the size , change to this pointer
	char m_zCallBackClassName[400];
	char m_zReferenceTable[100];

	char m_zSingleThreadCallBackMethod[400];
	char m_zSingleThreadCallBackMethodSig[400];
	
    char m_zResetMethod[400];
	char m_zResetMethodSig[400];

	char m_zMultiThreadCallBackMethodName[400];
	char m_zMultiThreadCallBackMethodSig[400];

	char m_zMultiThreadBuildCallBackMethod[400];
	char m_zMultiThreadBuildClassBackMethodSig[400];

};

#endif /* HOPSEVENTPROCESSOR_H_ */


