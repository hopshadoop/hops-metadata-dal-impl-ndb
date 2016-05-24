/*
 * HopsUtils.h
 *
 *  Created on: Feb 1, 2015
 *      Author: sri
 *      Email : skug@kth.se
 *      Notes : Queue communication mechanism implemented in several ways, currently I am using one method. Please read the comments
 *              before use the specific method.
 */

#ifndef HOPSUTILS_H_
#define HOPSUTILS_H_
#include <iostream>
#include <list>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/resource.h>

namespace hops
{
	namespace utl
	{
		namespace que
		{
			class HopsEventDataPacket
			{
			public:
				HopsEventDataPacket( void *);
				HopsEventDataPacket();
				virtual ~HopsEventDataPacket() {}
				void * m_ptrData;
				HopsEventDataPacket* m_ptrNext;
			};
			class HopsQueue
			{
			public:
				HopsEventDataPacket * m_ptrHead, * m_ptrTail;
				HopsQueue();
				void PushToQueue( HopsEventDataPacket *);
			};
			class HopsEventQueueFrame
			{
			public:
				HopsEventQueueFrame();
				virtual ~HopsEventQueueFrame();
				void AddToProducerQueue( HopsEventDataPacket* );
				void AddToProducerQueueWithLock( HopsEventDataPacket* );
				HopsEventDataPacket* PollFromConsumerQueue();
				void PushToIntermediateQueue();
				void PollFromIntermediateQueue();
			public:
				HopsQueue * m_ptrProducerQueue;
				HopsQueue * m_ptrIntermediateQueue;
				HopsQueue *m_ptrConsumerQueue;
				pthread_mutex_t m_mutexQueueHolder;


			};

			class QueueSizeCondition
			{
			public:
				QueueSizeCondition();
				~QueueSizeCondition();
				int getQueueTotalSize(){return m_iObectSize;}
				void IncreaseQueueSize(unsigned int _iValue);
				void DecreaseQueueSize(unsigned int _iValue);
				void InitQueueSizeCondition(unsigned long long _iMaxCapcity);
			private:
				pthread_mutex_t m_mutexQueueSize;
				pthread_cond_t m_condQueueSize;
				unsigned long long m_iObectSize;
				unsigned long long m_iMaxQueueCapacity;

			};
			class ThreadToken
			{
			public:
				ThreadToken();
				~ThreadToken();
				void SendSignal();
				void WaitForSignal();
				void TerminateIgnition();
			private:
				bool m_isPreviousPrcessed;
				pthread_mutex_t m_mutexThreadToken;
				pthread_cond_t m_condThreadToken;

			};

		}
		class HopsEventStreamingTimer
		{
		public:
			HopsEventStreamingTimer();
			HopsEventStreamingTimer(int _iThreadSleepInterval,bool _isThisMilliSleep);
			unsigned long long GetMonotonicTime();
			char * GetUniqString(int _iSuffix);
			unsigned long long GetEpochTime();
			char * GetCurrentTimeStamp();
			~HopsEventStreamingTimer();
			void NanoSleep();
		private:
			int m_iSleepInterval;
			struct timespec m_stNanoTimeSpec;
		};

		class HopsEventStreamerMemoryUsage
		{
		public:
			HopsEventStreamerMemoryUsage();
			~HopsEventStreamerMemoryUsage();
			size_t GetTotalSystemMemory();
			size_t GetCurrentRSS();
			size_t GetPeakRSS();
			int ParseLine(char* _ptrzLine);
			int GetCurrentVirtualMemory();

		};
	}
}

#endif /* HOPSUTILS_H_ */
