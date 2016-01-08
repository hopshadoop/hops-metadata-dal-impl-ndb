/*
 * EventJNICallBack.cpp
 *
 *  Created on: Feb 10, 2015
 *      Author: sri
 */

#include <jni.h>
#include <stdio.h>
#include <ctime>
#include "../include/HopsEventJNICallBack.h"
#include "../include/HopsEventAPI.h"
#include "../include/HopsLoadSimulationEventAPI.h"

using namespace cnf;
#define RT_EVENT_API_CONFIG "/etc/hadoop/RT_EventAPIConfig.ini"
#define RM_EVENT_API_CONFIG "/etc/hadoop/RM_EventAPIConfig.ini"
static JavaVM *g_ptrGlobalJVM;
static JNIEnv *g_ptrGlobalJniEnv;
HopsEventStreamingTimer *g_EventStreamingTimer;

// close event api session is still in testing
JNIEXPORT void JNICALL Java_io_hops_metadata_ndb_JniNdbEventStreaming_closeEventAPISession(
		JNIEnv *env, jobject thisObj) {
	int l_iThreadArraySize = 0;
	pthread_t *l_pThradArray = HopsEventAPI::Instance()->GetPthreadIdArray(
			&l_iThreadArraySize);
	for (int i = 0; i < l_iThreadArraySize; ++i) {
		printf("[EventAPI] ########## Stopping thread ids - %ld\n",
				l_pThradArray[i]);
	}


}

JNIEXPORT void JNICALL Java_io_hops_metadata_ndb_JniNdbEventStreaming_startEventAPISession(
		JNIEnv *env, jobject thisObj, jstring jpath) {

  const char * path = env->GetStringUTFChars(jpath,0);

	HopsConfigFile *l_ptrCFile=NULL;
  printf("starting event api session with config file: %s\n", path);
    // load the rt configuration file
		l_ptrCFile = new HopsConfigFile(path);
		HopsEventAPI::Instance()->dropEvents();
	
	int status = env->GetJavaVM(&g_ptrGlobalJVM);
	char *l_ptrTimeStamp = g_EventStreamingTimer->GetCurrentTimeStamp();
	if (status != 0) {
		printf("[FAILED][%s] JNI layer, Getting the JVM pointer failed \n",
				l_ptrTimeStamp);
		delete[] l_ptrTimeStamp;
		exit(EXIT_FAILURE);
	}

	bool l_bIsLoadSimulationEnbled =
			((int) atoi(l_ptrCFile->GetValue("LOAD_SIMULATION_ENABLED"))) == 1 ?
					true : false;
	if (l_bIsLoadSimulationEnbled) {
		printf(
				"[JNI][%s] ###################### Starting the Load simulation EventAPI and native methods ####################\n",
				l_ptrTimeStamp);
		HopsLoadSimulationEventAPI::Instance()->LoadSimulationInitAPI(
				g_ptrGlobalJVM, l_ptrCFile);
	} else {
		printf(
				"[JNI][%s] ###################### Starting the  EventAPI and native methods ####################\n",
				l_ptrTimeStamp);
		HopsEventAPI::Instance()->initAPI(g_ptrGlobalJVM, l_ptrCFile);
	}
	printf(
			"[JNI][%s] ################## Initialization done , now starting callback ##################\n",
			l_ptrTimeStamp);
	delete[] l_ptrTimeStamp;
  
}

JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM *vm, void *pvt) {
	jint l_jResult = vm->GetEnv((void **) &g_ptrGlobalJniEnv, JNI_VERSION_1_6);
	g_EventStreamingTimer = new HopsEventStreamingTimer();
	char *l_ptrTimeStamp = g_EventStreamingTimer->GetCurrentTimeStamp();
	if (l_jResult == JNI_OK) {
		l_jResult = g_ptrGlobalJniEnv->EnsureLocalCapacity(1024);
		printf(
				"[JNI][%s]###################  JVM initialize the JNI <version - JNI_VERSION_1_6> ########### \n",
				l_ptrTimeStamp);
		ndb_init();
		delete[] l_ptrTimeStamp;
		return JNI_VERSION_1_6;
	} else {
		delete[] l_ptrTimeStamp;
		return JNI_ERR;
	}

}

JNIEXPORT void JNICALL JNI_OnUnload(JavaVM *vm, void *pvt) {
	char *l_ptrTimeStamp = g_EventStreamingTimer->GetCurrentTimeStamp();
	printf(
			"[JNI][%s] ################### JVM is unloading now ###############\n",
			l_ptrTimeStamp);
	printf(
			"[JNI][%s] ################### Releasing NDBAPI resources ###############\n",
			l_ptrTimeStamp);
	ndb_end(0);
	printf(
			"[JNI][%s] ################### Released NDBAPI resources ###############\n",
			l_ptrTimeStamp);

	delete[] l_ptrTimeStamp;
}

