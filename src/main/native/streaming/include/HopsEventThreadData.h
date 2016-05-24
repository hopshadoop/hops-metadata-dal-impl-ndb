/*
 * EventThreadData.h
 *
 *  Created on: Feb 2, 2015
 *      Author: sri
 */

#ifndef HOPSEVENTTHREADDATA_H_
#define HOPSEVENTTHREADDATA_H_
#include <iostream>
#include <stdio.h>
#include <string.h>

#include "NdbApi.hpp"
#include "HopsReturnObject.h"
#include "HopsSimulationReturnObject.h"

class EventThreadData {
public:
	EventThreadData();
	virtual ~EventThreadData();
	Uint64 getTransactionId();
	void setTransactionId(Uint64 _transactionId);
	inline int getNoOfEventColumn() {
		return m_noOfEventsColumn;
	}
	inline bool isCurrent() {
		return m_isCurrent;
	}
	inline std::string getTableName() {
		return m_sTableName;
	}
	void setReturnObjectPtr(HopsReturnObject *_ptrReturnObject) {
		m_ptrReturnObject = _ptrReturnObject;
	}
	void setSimulationReturnObjectPtr(
			HopsSimulationReturnObject *_ptrSimulationReturnObject) {
		m_ptrSimulationReturnObject = _ptrSimulationReturnObject;
	}
	HopsSimulationReturnObject * GetSimulationReturnObject()
	{
		return m_ptrSimulationReturnObject;
	}
	HopsReturnObject * GetReturnObject() {
		return m_ptrReturnObject;
	}
	void SetGCIIndexValue(int _iValue) {
		m_GCIIndex = _iValue;
	}
	int GetGCIIndexValue() {
		return m_GCIIndex;
	}

private:
	int m_GCIIndex;
	Uint64 m_TransactionId;
	int m_noOfEventsColumn;
	NdbDictionary::Event::TableEvent m_TableEvent;
	bool m_isCurrent;
	std::string m_sTableName;
	HopsReturnObject *m_ptrReturnObject;
	HopsSimulationReturnObject *m_ptrSimulationReturnObject;

};

#endif /* HOPSEVENTTHREADDATA_H_ */
