/*
 * HopsLoadSimulation.cpp
 *
 *  Created on: Mar 30, 2015
 *      Author: sri
 */

#include "../include/HopsLoadSimulation.h"
#include "../include/HopsReturnObject.h"
#include "../include/HopsEventThreadData.h"
#include <sys/utsname.h>

#define GB 1073741824
#define MB 1048576

HopsLoadSimulation::HopsLoadSimulation() {
	m_ptrnanoSleepTimer = NULL;
	m_ptrLoadQ = NULL;
	m_ptrHopsMemoryUsage = NULL;
	m_ptrQueueSizeCondtion = NULL;

	m_fTotalPhysicalMemoryMB = 0.0f;
	m_fVirtualPhysicalMemoryMB = 0.0f;

	m_threadid = 0;
	m_iTotalNoOfGCI = 0;
	m_iTotalThreads = 0;
	m_iDelayms = 0;
	m_iTransactionPerGCI = 0;
	m_iNumberOfJavaClass = 0;
	m_iNumberOfAttributePerClass = 0;
	m_iSingleContainerSize = 0;
	m_iPhysicalThresholdMemory = 0;
	m_iMaxProgramAllocationMemory = 0;
	m_iVirtualThresholdMemory = 0;
	m_iMaxProgramAllocationVirtualMemory = 0;
	m_iTransactionId = 0;
}

HopsLoadSimulation::~HopsLoadSimulation() {
	delete m_ptrnanoSleepTimer;
	delete m_ptrHopsMemoryUsage;
}
pthread_t HopsLoadSimulation::InitializeHopsSimulationThread(
		HopsLoadSimulation * _ptrLoadSimulation,
		HopsEventQueueFrame **_ptrLoadQ,
		QueueSizeCondition ** _ptrQueueSizeCondition, HopsConfigFile *_ptrConf) {
	m_ptrLoadQ = _ptrLoadQ;
	m_ptrConf=_ptrConf;
	m_ptrQueueSizeCondtion = _ptrQueueSizeCondition;
	pthread_attr_t l_pthreadAttr;
	size_t l_pthreadStackSize;
	pthread_attr_init(&l_pthreadAttr);
	pthread_attr_getstacksize(&l_pthreadAttr, &l_pthreadStackSize);
	struct utsname l_utsName;
	uname(&l_utsName);
	printf(
			"[HopsLoadSimulationJNIDispatcher][INFO] ########### Starting event processor thread ################# \n");
	printf("######## System architecture      : %s\n", l_utsName.machine);
	printf("######## Node name 		  : %s\n", l_utsName.nodename);
	printf("######## System name              : %s\n", l_utsName.sysname);
	printf("######## Kernel release           : %s\n", l_utsName.release);
	printf("######## Version                  : %s\n", l_utsName.version);
	printf("######## System stack size        : %li\n", l_pthreadStackSize);
	pthread_create(&m_threadid, NULL, (void*(*)(void*)) HopsLoadSimulation::Run, (void*) (void*) _ptrLoadSimulation);
	cout
			<< "[HopsLoadSimulation] ################### LoadSimulation starting on this thread id  : "
			<< m_threadid << endl;

	return m_threadid;
}

void HopsLoadSimulation::InitSimulation() {
	m_ptrnanoSleepTimer = new HopsEventStreamingTimer(m_iDelayms, true);
	m_ptrHopsMemoryUsage = new HopsEventStreamerMemoryUsage();

	m_iTransactionId = 64000000;

	m_iTotalNoOfGCI = (int) atoi(m_ptrConf->GetValue("TOTAL_GCI"));
	m_iTotalThreads = (int) atoi(m_ptrConf->GetValue("PROCESSING_THREADS"));
	m_iDelayms = (int) atoi(m_ptrConf->GetValue("DELAY_BETWEN_GCI"));
	m_iTransactionPerGCI = (int) atoi(m_ptrConf->GetValue("TRANSACTION_PER_GCI"));
	m_iNumberOfJavaClass = (int) atoi(m_ptrConf->GetValue("NUMBER_OF_JAVA_CLASS"));
	m_iNumberOfAttributePerClass = (int) atoi(
			m_ptrConf->GetValue("NUMBER_OF_ATTRIBUTE_PER_CLASS"));

	m_iSingleContainerSize = (int) atoi(
			m_ptrConf->GetValue("SINGLE_CONTAINER_SIZE"));

	m_iPhysicalThresholdMemory = (int) atoi(
			m_ptrConf->GetValue("PHYSICAL_THRESHOLD_PERCENTAGE"));
	m_fTotalPhysicalMemoryMB =
			(float) m_ptrHopsMemoryUsage->GetTotalSystemMemory() / (float) GB;
	m_iMaxProgramAllocationMemory = ((int) m_fTotalPhysicalMemoryMB
			* m_iPhysicalThresholdMemory) / 100;

	m_iVirtualThresholdMemory = (int) atoi(
			m_ptrConf->GetValue("VIRTUAL_MEMORY_THRESHOLD_PERCENTAGE"));
	m_fVirtualPhysicalMemoryMB =
			(float) m_ptrHopsMemoryUsage->GetTotalSystemMemory() / (float) GB;
	m_iMaxProgramAllocationVirtualMemory = ((int) m_fVirtualPhysicalMemoryMB
			* m_iVirtualThresholdMemory) / 100;

}
void *HopsLoadSimulation::Run(void *_ptrHandler) {
	((HopsLoadSimulation*) _ptrHandler)->StartSimulationThread();
	return NULL;
}

void HopsLoadSimulation::StartSimulationThread() {
	while (true) {
		LoadSimulation();
		sleep(5);
		//TODO
		//first kill all jvm attached threads and then kill this one, this is wrong
		pthread_exit(NULL);
	}
}

void HopsLoadSimulation::LoadSimulation() {

	InitSimulation();
	std::vector<std::string> l_vecTableNames;
	std::map<int, std::vector<std::string> > l_mapOfClassAttributes;

	for (int i = 0; i < m_iNumberOfJavaClass; ++i) {
		stringstream l_sStream;
		l_sStream << "table_";
		l_sStream << i + 1;
		l_vecTableNames.push_back(l_sStream.str());
		std::vector<std::string> l_vecAttributeNames;
		for (int j = 0; j < m_iNumberOfAttributePerClass; ++j) {
			stringstream l_sStream;
			l_sStream << "class";
			l_sStream << i + 1;
			l_sStream << "Attr";
			l_sStream << j + 1;
			l_vecAttributeNames.push_back(l_sStream.str());
		}
		l_mapOfClassAttributes.insert(
				std::make_pair<int, std::vector<std::string> >(i,
						l_vecAttributeNames));
	}
	int l_orderid = 0;
	unsigned long long l_llStartTime, l_llEndTime;
	char l_zDummyvalue[10];
	bool l_bIsPhysicalExceed = false;

	for (int gci = 0; gci < m_iTotalNoOfGCI + m_iTotalThreads; ++gci) {

		int l_iThreaedIdOffSet = gci % m_iTotalThreads;
		for (int tid = 0; tid < m_iTransactionPerGCI; ++tid) {
			++l_orderid;
			for (int nt = 0; nt < m_iNumberOfJavaClass; ++nt) {
				HopsSimulationReturnObject *pSimulationReturnObject =
						new HopsSimulationReturnObject(
								(char*) l_vecTableNames[nt].c_str());

				for (int nc = 0; nc < m_iNumberOfAttributePerClass; ++nc) {
					//first table is the reference table
					if (nt == 0 && nc == 0) {
						pSimulationReturnObject->LSAddNdbRecAttrOrderVal(
								l_mapOfClassAttributes[nt][nc].c_str(),
								l_orderid);
					} else {
						sprintf(l_zDummyvalue, "d_%d_%d", nt, nc);
						pSimulationReturnObject->LSAddNdbRecAttr(
								l_mapOfClassAttributes[nt][nc].c_str(),
								l_zDummyvalue);
						memset(l_zDummyvalue, 0, sizeof(l_zDummyvalue));
					}
				}

				EventThreadData *pEventData = new EventThreadData();
				pEventData->SetGCIIndexValue(gci);
				pEventData->setSimulationReturnObjectPtr(
						pSimulationReturnObject);
				pEventData->setTransactionId(m_iTransactionId + tid);
				HopsEventDataPacket *pContMNS = new HopsEventDataPacket(
						pEventData);

				m_ptrLoadQ[l_iThreaedIdOffSet]->AddToProducerQueue(pContMNS);

			}

		}

		int l_iCurrentPhysicalMemoryGB =
				m_ptrHopsMemoryUsage->GetCurrentRSS() / GB;
		if (l_iCurrentPhysicalMemoryGB > m_iMaxProgramAllocationMemory) {
			if (!l_bIsPhysicalExceed) {
				printf(
						"[LoadSimulation][WARNING] Max physical memory     - %d GB \n",
						m_iMaxProgramAllocationMemory);
				printf(
						"[LoadSimulation][WARNING] Current physical memory - %d GB\n",
						l_iCurrentPhysicalMemoryGB);
				printf(
						"[LoadSimulation][WARNING] Physical memory is exceed the configured value. But less than configured virtual memory..\n");
				l_bIsPhysicalExceed = true;
			}
			int l_iCurrentVMGB =
					m_ptrHopsMemoryUsage->GetCurrentVirtualMemory() / MB;
			if (l_iCurrentVMGB > m_iMaxProgramAllocationVirtualMemory) {
				printf("[LoadSimulation][ERROR] Max virtual memory     - %d \n",
						m_iMaxProgramAllocationVirtualMemory);
				printf("[LoadSimulation][ERROR] Current virtual memory - %d \n",
						l_iCurrentVMGB);
				printf(
						"[LoadSimulation][ERROR] Machine virtual memory is exceed the configured value. Exit now.");
				exit(EXIT_FAILURE);
			}
		}

	}

	l_llStartTime = m_ptrnanoSleepTimer->GetMonotonicTime();
	for (int gci = 0; gci < m_iTotalNoOfGCI + m_iTotalThreads; ++gci) {
		int l_iThreaedIdOffSet = gci % m_iTotalThreads;
		m_ptrQueueSizeCondtion[l_iThreaedIdOffSet]->IncreaseQueueSize(
				m_iSingleContainerSize * m_iNumberOfJavaClass
						* m_iTransactionPerGCI);
		m_ptrLoadQ[l_iThreaedIdOffSet]->PushToIntermediateQueue();
		m_ptrnanoSleepTimer->NanoSleep();
	}

	l_llEndTime = m_ptrnanoSleepTimer->GetMonotonicTime();
	unsigned long long l_llDuration = (l_llEndTime - l_llStartTime);
	float fduration = (float) l_llDuration / (float) 1000000;
	printf("Load duration - %.3f ms \n", fduration);

	printf(
			"#################### Experiment is done #############################");

}
