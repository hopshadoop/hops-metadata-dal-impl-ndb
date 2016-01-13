//============================================================================
// Name        : HopsUtils.cpp
// Created on  : Feb 1, 2015
// Author      : sri(skug@kth.se)
// Version     : 1.0
// Copyright   : SICS
// Description : Queue communication mechanism between thread, several methods are implemented to support
//============================================================================
#include "../include/HopsUtils.h"
#include <stdio.h>
#include <sys/time.h>
using namespace hops::utl::que;
using namespace hops::utl;

#define SECONDS      1000000000
#define MILLISECONDS 1000000
#define MICROSECONDS 1000
#define NANOSECONDS  1
HopsEventDataPacket::HopsEventDataPacket() {
	m_ptrData = NULL;
	m_ptrNext = NULL;
}
HopsEventDataPacket::HopsEventDataPacket(void * _pData) {
	m_ptrData = _pData;
	m_ptrNext = NULL;
}

HopsQueue::HopsQueue() {
	m_ptrHead = NULL;
	m_ptrTail = NULL;
}
void HopsQueue::PushToQueue(HopsEventDataPacket * _pDataContainer) {
	if (m_ptrHead) {
		m_ptrTail->m_ptrNext = _pDataContainer;
	} else {
		m_ptrHead = _pDataContainer;
	}
	m_ptrTail = _pDataContainer;
}

HopsEventQueueFrame::HopsEventQueueFrame() {
	m_ptrProducerQueue = new HopsQueue();
	m_ptrIntermediateQueue = new HopsQueue();
	m_ptrConsumerQueue = new HopsQueue();

	pthread_mutex_init(&m_mutexQueueHolder, NULL);

}
HopsEventQueueFrame::~HopsEventQueueFrame() {
	delete m_ptrProducerQueue;
	delete m_ptrIntermediateQueue;
	delete m_ptrConsumerQueue;
}
void HopsEventQueueFrame::AddToProducerQueue(
		HopsEventDataPacket * _pDataContainer) {
	m_ptrProducerQueue->PushToQueue(_pDataContainer);
}
void HopsEventQueueFrame::AddToProducerQueueWithLock(
		HopsEventDataPacket * _pDataContainer) {
	pthread_mutex_lock(&m_mutexQueueHolder);
	m_ptrProducerQueue->PushToQueue(_pDataContainer);
	pthread_mutex_unlock(&m_mutexQueueHolder);

}
void HopsEventQueueFrame::PushToIntermediateQueue() {
	pthread_mutex_lock(&m_mutexQueueHolder);
	if (m_ptrProducerQueue->m_ptrHead) {
		if (m_ptrIntermediateQueue->m_ptrHead) {
			m_ptrIntermediateQueue->m_ptrTail->m_ptrNext =
					m_ptrProducerQueue->m_ptrHead;
		} else {
			m_ptrIntermediateQueue->m_ptrHead = m_ptrProducerQueue->m_ptrHead;
		}
		m_ptrIntermediateQueue->m_ptrTail = m_ptrProducerQueue->m_ptrTail;
		m_ptrProducerQueue->m_ptrHead = NULL;
		m_ptrProducerQueue->m_ptrTail = NULL;
	}

	pthread_mutex_unlock(&m_mutexQueueHolder);
}
void HopsEventQueueFrame::PollFromIntermediateQueue() {
	pthread_mutex_lock(&m_mutexQueueHolder);
	if (m_ptrIntermediateQueue->m_ptrHead) {
		if (m_ptrConsumerQueue->m_ptrHead) {
			m_ptrConsumerQueue->m_ptrTail->m_ptrNext =
					m_ptrIntermediateQueue->m_ptrHead;
		} else {
			m_ptrConsumerQueue->m_ptrHead = m_ptrIntermediateQueue->m_ptrHead;
		}
		m_ptrConsumerQueue->m_ptrTail = m_ptrIntermediateQueue->m_ptrTail;
		m_ptrIntermediateQueue->m_ptrHead = NULL;
		m_ptrIntermediateQueue->m_ptrTail = NULL;
	}
	pthread_mutex_unlock(&m_mutexQueueHolder);
}
HopsEventDataPacket* HopsEventQueueFrame::PollFromConsumerQueue() {
	if (m_ptrConsumerQueue->m_ptrHead) {
		HopsEventDataPacket* pCont = m_ptrConsumerQueue->m_ptrHead;
		m_ptrConsumerQueue->m_ptrHead =
				m_ptrConsumerQueue->m_ptrHead->m_ptrNext;
		pCont->m_ptrNext = NULL;
		return pCont;
	} else
		return NULL;
}

ThreadToken::ThreadToken() {
	pthread_mutex_init(&m_mutexThreadToken, NULL);
	pthread_cond_init(&m_condThreadToken, NULL);
	m_isPreviousPrcessed = false;
}
ThreadToken::~ThreadToken() {
	pthread_mutex_destroy(&m_mutexThreadToken);
	pthread_cond_destroy(&m_condThreadToken);
}
void ThreadToken::WaitForSignal() {
	pthread_mutex_lock(&m_mutexThreadToken);
	while (!m_isPreviousPrcessed) {
		pthread_cond_wait(&m_condThreadToken, &m_mutexThreadToken);
	}
	m_isPreviousPrcessed = false;
	pthread_mutex_unlock(&m_mutexThreadToken);
}
void ThreadToken::SendSignal() {
	pthread_mutex_lock(&m_mutexThreadToken);
	m_isPreviousPrcessed = true;
	pthread_cond_signal(&m_condThreadToken);
	pthread_mutex_unlock(&m_mutexThreadToken);
}

QueueSizeCondition::QueueSizeCondition() {
	pthread_mutex_init(&m_mutexQueueSize, NULL);
	pthread_cond_init(&m_condQueueSize, NULL);
	m_iObectSize = 0;
	m_iMaxQueueCapacity = 0;
}
QueueSizeCondition::~QueueSizeCondition() {
	pthread_cond_destroy(&m_condQueueSize);
	pthread_mutex_destroy(&m_mutexQueueSize);
}

void QueueSizeCondition::InitQueueSizeCondition(
		unsigned long long _iMaxCapcity) {
	m_iMaxQueueCapacity = _iMaxCapcity;
}

void QueueSizeCondition::IncreaseQueueSize(unsigned int _iValue) {
	pthread_mutex_lock(&m_mutexQueueSize);
	while (m_iObectSize >= m_iMaxQueueCapacity) {
		pthread_cond_wait(&m_condQueueSize, &m_mutexQueueSize);
	}
	m_iObectSize += _iValue;
	/* A woken thread must acquire the lock, so it will also have to wait until we call unlock*/
	pthread_cond_signal(&m_condQueueSize);
	pthread_mutex_unlock(&m_mutexQueueSize);

}
void QueueSizeCondition::DecreaseQueueSize(unsigned int _iValue) {
	pthread_mutex_lock(&m_mutexQueueSize);
	while (m_iObectSize == 0) {
		pthread_cond_wait(&m_condQueueSize, &m_mutexQueueSize);
	}
	m_iObectSize -= _iValue;
	pthread_cond_signal(&m_condQueueSize);
	pthread_mutex_unlock(&m_mutexQueueSize);

}

HopsEventStreamingTimer::HopsEventStreamingTimer() {
	m_iSleepInterval = 0;
}
HopsEventStreamingTimer::HopsEventStreamingTimer(int _iThreadSleepInterval,
		bool _isThisMilliSleep) {
	m_iSleepInterval = _iThreadSleepInterval;
	memset(&m_stNanoTimeSpec, 0, sizeof(m_stNanoTimeSpec));
	m_stNanoTimeSpec.tv_sec = 0;
	if (_isThisMilliSleep)
		m_stNanoTimeSpec.tv_nsec = m_iSleepInterval * 1000000L;
	else
		m_stNanoTimeSpec.tv_nsec = m_iSleepInterval;
}
HopsEventStreamingTimer::~HopsEventStreamingTimer() {

}
// calling function has to delete the buffer, otherwise, this would be memory leak
char * HopsEventStreamingTimer::GetCurrentTimeStamp() {
	time_t l_rawTime;
	struct tm * l_ptTimeInfo;
	char *l_ptrBuffer = new char[80];

	time(&l_rawTime);
	l_ptTimeInfo = localtime(&l_rawTime);

	strftime(l_ptrBuffer, 80, "%d-%m-%Y %I:%M:%S", l_ptTimeInfo);

	return l_ptrBuffer;

}

unsigned long long HopsEventStreamingTimer::GetEpochTime() {
	struct timeval l_stTimeVal;
	gettimeofday(&l_stTimeVal, NULL);
	unsigned long long l_llMillisecondsSinceEpoch =
			(unsigned long long) (l_stTimeVal.tv_sec) * 1000
					+ (unsigned long long) (l_stTimeVal.tv_usec) / 1000;
	return l_llMillisecondsSinceEpoch;
}
// calling function has to delete the buffer, otherwise, this would be a memory leak
// we can use this string for event name
char * HopsEventStreamingTimer::GetUniqString(int _iSuffix) {
	char *l_ptrBuffer = new char[80];
	struct timeval te;
	gettimeofday(&te, NULL); // get current time
	long long milliseconds = te.tv_sec * 1000LL + te.tv_usec / 1000; //
	sprintf(l_ptrBuffer, "%lld_%s_%d", milliseconds, "SICS", _iSuffix);
	return l_ptrBuffer;
}
unsigned long long HopsEventStreamingTimer::GetMonotonicTime() {
	struct timespec l_stTimeSpec;
	int l_iReturnValue;
	l_iReturnValue = clock_gettime(CLOCK_MONOTONIC, &l_stTimeSpec);
	if (l_iReturnValue < 0) {
		printf(
				"[StreamingProcessor][ERROR] ############ Error in getting timestamp - %i\n",
				l_iReturnValue);
		exit(EXIT_FAILURE);
	}
	return (unsigned long long) l_stTimeSpec.tv_sec * SECONDS
			+ l_stTimeSpec.tv_nsec * NANOSECONDS;
}

void HopsEventStreamingTimer::NanoSleep() {
	nanosleep(&m_stNanoTimeSpec, (struct timespec *) NULL);
}

HopsEventStreamerMemoryUsage::HopsEventStreamerMemoryUsage() {
}

HopsEventStreamerMemoryUsage::~HopsEventStreamerMemoryUsage() {

}
size_t HopsEventStreamerMemoryUsage::GetPeakRSS() {
	struct rusage l_stRusage;
	getrusage(RUSAGE_SELF, &l_stRusage);
	return (size_t) (l_stRusage.ru_maxrss * 1024L);
}

size_t HopsEventStreamerMemoryUsage::GetCurrentRSS() {
	long l_lRSS = 0L;
	FILE* fp = NULL;
	if ((fp = fopen("/proc/self/statm", "r")) == NULL)
		return (size_t) 0L;
	if (fscanf(fp, "%*s%ld", &l_lRSS) != 1) {
		fclose(fp);
		return (size_t) 0L;
	}
	fclose(fp);
	return (size_t) l_lRSS * (size_t) sysconf( _SC_PAGESIZE);
}

//return total memory in bytes.
size_t HopsEventStreamerMemoryUsage::GetTotalSystemMemory() {
	long l_lPages = sysconf(_SC_PHYS_PAGES);
	long l_lPageSize = sysconf(_SC_PAGE_SIZE);
	return l_lPages * l_lPageSize;
}

int HopsEventStreamerMemoryUsage::ParseLine(char* _ptrzLine) {
	int l_iLength = strlen(_ptrzLine);
	while (*_ptrzLine < '0' || *_ptrzLine > '9')
		_ptrzLine++;
	_ptrzLine[l_iLength - 3] = '\0';
	l_iLength = atoi(_ptrzLine);
	return l_iLength;
}

int HopsEventStreamerMemoryUsage::GetCurrentVirtualMemory() {
	FILE* l_ptrFile = fopen("/proc/self/status", "r");
	int l_iResult = -1;
	char l_zLine[128];

	while (fgets(l_zLine, 128, l_ptrFile) != NULL) {
		if (strncmp(l_zLine, "VmSize:", 7) == 0) {
			l_iResult = ParseLine(l_zLine);
			break;
		}
	}
	fclose(l_ptrFile);
	return l_iResult;
}

