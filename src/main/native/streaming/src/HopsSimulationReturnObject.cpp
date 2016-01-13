//============================================================================
// Name        : HopsSimulationReturnObject.cpp
// Created on  : Apr 7, 2015
// Author      : sri(skug@kth.se)
// Version     : 1.0
// Copyright   : SICS
// Description : Every events from NDB will be converted using this class
//============================================================================
#include "../include/HopsSimulationReturnObject.h"

HopsSimulationReturnObject::HopsSimulationReturnObject(char *_pzTableName) {
	strcpy(m_zTableName, _pzTableName);
}

HopsSimulationReturnObject::~HopsSimulationReturnObject() {
}

int HopsSimulationReturnObject::GetByteArray(const NdbRecAttr* attr,
		const char*& first_byte, size_t& bytes) {
	const NdbDictionary::Column::ArrayType array_type =
			attr->getColumn()->getArrayType();
	const size_t attr_bytes = attr->get_size_in_bytes();
	const char* aRef = attr->aRef();
	string result;

	switch (array_type) {
	case NdbDictionary::Column::ArrayTypeFixed:
		first_byte = aRef;
		bytes = attr_bytes;
		return 0;
	case NdbDictionary::Column::ArrayTypeShortVar:
		first_byte = aRef + 1;
		bytes = (size_t) (aRef[0]);
		return 0;
	case NdbDictionary::Column::ArrayTypeMediumVar:
		first_byte = aRef + 2;
		bytes = (size_t) (aRef[1]) * 256 + (size_t) (aRef[0]);
		return 0;
	default:
		first_byte = NULL;
		bytes = 0;
		return -1;
	}
}
int HopsSimulationReturnObject::GetString(const NdbRecAttr* attr, string& str) {
	size_t attr_bytes;
	const char* data_start_ptr = NULL;

	if (GetByteArray(attr, data_start_ptr, attr_bytes) == 0) {
		str = string(data_start_ptr, attr_bytes);
		if (attr->getType() == NdbDictionary::Column::Char) {
			size_t endpos = str.find_last_not_of(" ");
			if (string::npos != endpos) {
				str = str.substr(0, endpos + 1);
			}
		}
	}
	return 0;
}
void HopsSimulationReturnObject::LSAddNdbRecAttr(const char *_pzColName,
		const char *_pzActualData) {
	SimulationNdbValues l_stNdbValues;

	strcpy(l_stNdbValues.m_zColName, _pzColName);
	l_stNdbValues.m_dataType = NdbDictionary::Column::Varchar;
	strcpy(l_stNdbValues.m_zValue, _pzActualData);
	m_listOfNdbValues.push_back(l_stNdbValues);

}

void HopsSimulationReturnObject::LSAddNdbRecAttrOrderVal(const char *_pzColName,
		int _iValue) {
	SimulationNdbValues l_stNdbValues;

	strcpy(l_stNdbValues.m_zColName, _pzColName);
	l_stNdbValues.m_dataType = NdbDictionary::Column::Int;
	l_stNdbValues.m_int32Value = _iValue;
	m_listOfNdbValues.push_back(l_stNdbValues);

}

