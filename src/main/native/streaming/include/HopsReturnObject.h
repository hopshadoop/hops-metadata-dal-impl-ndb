/*
 * ReturnObject.h
 *
 *  Created on: Feb 3, 2015
 *      Author: sri
 */

#ifndef HOPSRETURNOBJECT_H_
#define HOPSRETURNOBJECT_H_

#include <iostream>
#include <map>
#include <list>
#include <vector>
#include <string.h>
#include "NdbApi.hpp"
using namespace std;
struct NdbValues {

	NdbValues() {
		m_dataType = NdbDictionary::Column::Undefined;
		m_isThisNull = false;
		m_u32Value = 0;
		m_int64Value = 0;
		m_int32Value = 0;
		m_byteLength = 0;
		m_zSignedChar = 0;
		memset(m_zbyteArray, 0, sizeof(m_zbyteArray));
		memset(m_zColName, 0, sizeof(m_zColName));
	}
	~NdbValues() {

	}
	Int8 getSignedChar() {
		return m_zSignedChar;
	}
	char * getColName() {
		return m_zColName;
	}
	NdbDictionary::Column::Type getDataType() {
		return m_dataType;
	}
	Uint32 getUint32Value() {
		if (m_isThisNull)
			return 0;
		else {
			return m_u32Value;
		}

	}
	std::string getStringValue() {
		if (m_isThisNull)
			return "";
		else {
			return m_stringValue;
		}
	}
	char* getCharValue() {
		if (m_isThisNull)
			return NULL;
		else {
			return m_zValue;
		}
	}

	Int64 getInt64Value() {
		if (m_isThisNull)
			return -1;
		else {
			return m_int64Value;
		}
	}
	Int32 getInt32Value() {
		if (m_isThisNull)
			return -1;
		else {
			return m_int32Value;
		}
	}

	const char * getByteArray() {
		return m_zbyteArray;
	}

	char m_zValue[100];
	char m_zColName[100];
	std::string m_stringValue;
	Uint32 m_u32Value;
	Int64 m_int64Value;
	Int32 m_int32Value;
	char m_zbyteArray[1300]; // this we need to test
	int m_byteLength;
	bool m_isThisNull;
	Int8 m_zSignedChar;

	NdbDictionary::Column::Type m_dataType;

};
class HopsReturnObject {
public:
	HopsReturnObject(char *_pzTableName);
	virtual ~HopsReturnObject();
	void print();
	void setTotalColumnSize(int _iColumnSize);
	void processNdbRecAttr(NdbRecAttr * _pNdbRectAttr);
	void setObject(NdbRecAttr * _pNdbRectAttr, std::string sTableName,
			Uint64 _transactionid);
	void LSAddNdbRecAttr(const char *_pzColName, const char *_pzActualData);
	void LSAddNdbRecAttrOrderVal(const char *_pzColName, int _iValue);

	char* GetTableName() {
		return m_zTableName;
	}
	std::vector<NdbValues> m_listOfNdbValues;

private:
	int GetString(const NdbRecAttr* attr, string& str);
	int GetByteArray(const NdbRecAttr* attr, const char*& first_byte,
			size_t& bytes);

	//TO-DO need to fix with configuration variable, otherwise there will be a segmentation fault
	char m_zTableName[40];

};

#endif /* HOPSRETURNOBJECT_H_ */
