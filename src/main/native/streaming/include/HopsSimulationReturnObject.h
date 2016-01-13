/*
 * HopsSimulationReturnObject.h
 *
 *  Created on: Apr 7, 2015
 *      Author: sri
 */

#ifndef HOPSSIMULATIONRETURNOBJECT_H_
#define HOPSSIMULATIONRETURNOBJECT_H_


#include <iostream>
#include <map>
#include <list>
#include <vector>
#include <string.h>
#include "NdbApi.hpp"
using namespace std;
struct SimulationNdbValues
{

	SimulationNdbValues() {
		m_dataType = NdbDictionary::Column::Undefined;
		m_isThisNull = false;
		m_int32Value = 0;
		memset(m_zColName,0,sizeof(m_zColName));
	}
	~SimulationNdbValues() {

	}
	char * getColName()
	{
		return m_zColName;
	}
	NdbDictionary::Column::Type getDataType() {
		return m_dataType;
	}
	char* getCharValue() {
		if (m_isThisNull)
			return NULL;
		else {
			return m_zValue;
		}
	}

	Int32 getInt32Value() {
		if (m_isThisNull)
			return -1;
		else {
			return m_int32Value;
		}
	}


	char m_zValue[100];
	char m_zColName[100];
	Int32 m_int32Value;
	bool m_isThisNull;

	NdbDictionary::Column::Type m_dataType;

};
class HopsSimulationReturnObject {
public:
	HopsSimulationReturnObject(char *_pzTableName);
	virtual ~HopsSimulationReturnObject();
	void print();
	void setTotalColumnSize(int _iColumnSize);
	void processNdbRecAttr(NdbRecAttr * _pNdbRectAttr);
	void setObject(NdbRecAttr * _pNdbRectAttr,std::string sTableName,Uint64 _transactionid);
	void LSAddNdbRecAttr(const char *_pzColName,const char *_pzActualData);
	void LSAddNdbRecAttrOrderVal(const char *_pzColName,
			int _iValue);

	char*  GetTableName(){ return m_zTableName;}
	std::vector<SimulationNdbValues > m_listOfNdbValues;

private:
	int GetString(const NdbRecAttr* attr, string& str);
	int GetByteArray(const NdbRecAttr* attr,
	                          const char*& first_byte,
	                          size_t& bytes);

	//TO-DO need to fix with configuration variable, otherwise there will be a segmentation fault
	char m_zTableName[10];




};

#endif /* HOPSSIMULATIONRETURNOBJECT_H_ */
