/*
 * EventAPI.h
 *
 *  Created on: Feb 10, 2015
 *      Author: sri
 */

#ifndef HOPSEVENTAPI_H_
#define HOPSEVENTAPI_H_
#include <map>
#include <set>
#include <sstream>
#include <iostream>
#include <jni.h>
#include <time.h>
#include "NdbApi.hpp"
#include "HopsJNIDispatcher.h"
#include "HopsEventThreadData.h"
#include "HopsReturnObject.h"
#include "HopsEventThread.h"
#include "HopsConfigReader.h"

class HopsEventAPI {
public:
	static HopsEventAPI* Instance();
	~HopsEventAPI();
	void initAPI(JavaVM *_ptrJVM,HopsConfigFile *_ptrConf);
	pthread_t * GetPthreadIdArray(int *_ptrSize);
	void dropEvents();
private:
	HopsEventAPI();  // Private so that it can  not be called
	static HopsEventAPI* m_pInstance;
	pthread_t *m_ptrThreadArray;
	HopsEventQueueFrame **m_ptrProcessingQ;
	HopsEventQueueFrame *m_ptrJavaObjectDispatcherQ;
	QueueSizeCondition *m_pConditionLock;
	int m_iTotalThreads;
	int m_iMaxEventBufferSize;
	QueueSizeCondition **m_ptrCondtionLock;
	HopsJNIDispatcher **m_ptrJNIDispatcher;
	HopsEventThread *m_ptrEventThread;
	int m_iSingleContainerSize;
	bool m_bAPIInitialized;
};

#endif /* HOPSEVENTAPI_H_ */
