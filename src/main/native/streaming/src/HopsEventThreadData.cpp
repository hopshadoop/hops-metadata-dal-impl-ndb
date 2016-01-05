//============================================================================
// Name        : HopsEventThreadData.cpp
// Created on  : Feb 3, 2015
// Author      : sri(skug@kth.se)
// Version     : 1.0
// Copyright   : SICS
// Description : Thread object within  threads.
//============================================================================

#include "../include/HopsEventThreadData.h"

EventThreadData::EventThreadData() {
	m_TransactionId = 0;
	m_TableEvent = NdbDictionary::Event::TE_ALL;
	m_isCurrent = false;

	m_ptrReturnObject = NULL;
	m_GCIIndex = 0;

}

EventThreadData::~EventThreadData() {
}

void EventThreadData::setTransactionId(Uint64 _transactionId) {
	m_TransactionId = _transactionId;
}
Uint64 EventThreadData::getTransactionId() {
	return m_TransactionId;
}

