/*
 * HopsLoadSimulation.h
 *
 *  Created on: Mar 30, 2015
 *      Author: sri
 */

#ifndef HOPSLOADSIMULATION_H_
#define HOPSLOADSIMULATION_H_

#include <iostream>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <vector>
#include <sstream>

#include "HopsUtils.h"
#include "HopsConfigReader.h"
using namespace hops::utl::que;
using namespace hops;
using namespace hops::utl;
using namespace cnf;
class HopsLoadSimulation {
public:
	HopsLoadSimulation();
	virtual ~HopsLoadSimulation();
	void LoadSimulation();
	static void * Run(void * _ptrHandler);
	pthread_t InitializeHopsSimulationThread(
			HopsLoadSimulation * _ptrLoadSimulation,
			HopsEventQueueFrame **_ptrLoadQ,
			QueueSizeCondition ** _ptrQueueSizeCondition,HopsConfigFile *_ptrConf);
	void StartSimulationThread();
private:
	void InitSimulation();
	HopsEventStreamingTimer *m_ptrnanoSleepTimer;
	pthread_t m_threadid;
	HopsEventQueueFrame **m_ptrLoadQ;
	HopsConfigFile *m_ptrConf;
	QueueSizeCondition **m_ptrQueueSizeCondtion;
	HopsEventStreamerMemoryUsage *m_ptrHopsMemoryUsage;

	int m_iTotalNoOfGCI;
	int m_iTotalThreads;
	int m_iDelayms;
	int m_iTransactionPerGCI;
	int m_iNumberOfJavaClass;
	int m_iNumberOfAttributePerClass;
	int m_iSingleContainerSize;
	int m_iPhysicalThresholdMemory;
	int m_iMaxProgramAllocationMemory;
	int m_iVirtualThresholdMemory;
	int m_iMaxProgramAllocationVirtualMemory;
	int m_iTransactionId;

	float m_fTotalPhysicalMemoryMB;
	float m_fVirtualPhysicalMemoryMB;

};

#endif /* HOPSLOADSIMULATION_H_ */
