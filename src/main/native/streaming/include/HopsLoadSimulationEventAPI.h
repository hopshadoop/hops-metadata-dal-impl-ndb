/*
 * HopsLoadSimulationEventAPI.h
 *
 *  Created on: Apr 7, 2015
 *      Author: sri
 */

#ifndef HOPSLOADSIMULATIONEVENTAPI_H_
#define HOPSLOADSIMULATIONEVENTAPI_H_
#include <map>
#include <set>
#include <sstream>
#include <iostream>
#include <jni.h>
#include <time.h>
#include "NdbApi.hpp"
#include "HopsLoadSimulationJNIDispatcher.h"
#include "HopsEventThreadData.h"
#include "HopsReturnObject.h"
#include "HopsEventThread.h"
#include "HopsConfigReader.h"

class HopsLoadSimulationEventAPI {
public:
	static HopsLoadSimulationEventAPI* Instance();
	~HopsLoadSimulationEventAPI();
	void initAPI(JavaVM *_ptrJVM);
	pthread_t * GetPthreadIdArray(int *_ptrSize);
	void StopAllDispatchingThreads();
	void LoadSimulationInitAPI(JavaVM *_ptrJVM,HopsConfigFile *_ptrConf);
private:
	HopsLoadSimulationEventAPI();  // Private so that it can  not be called
	static HopsLoadSimulationEventAPI* m_pInstance;
	pthread_t *m_pthreadArray;
	HopsEventQueueFrame **m_ptrProcessingQ;
	HopsEventQueueFrame *m_ptrJavaObjectDispatcherQ;
	QueueSizeCondition *m_pConditionLock;
	int m_iTotalThreads;
	int m_iMaxEventBufferSize;
	QueueSizeCondition **m_ptrCondtionLock;
	HopsLoadSimulationJNIDispatcher **m_ptrLoadSimulationJNIDispatcher;

};

#endif /* HOPSLOADSIMULATIONEVENTAPI_H_ */
