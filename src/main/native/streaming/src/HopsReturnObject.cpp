//============================================================================
// Name        : HopsReturnObject.cpp
// Created on  : Feb 3, 2015
// Author      : sri(skug@kth.se)
// Version     : 1.0
// Copyright   : SICS
// Description : Every events from NDB will be converted using this class
//============================================================================
#include "../include/HopsReturnObject.h"
#include <stdio.h>
HopsReturnObject::HopsReturnObject(char *_pzTableName) {
	strcpy(m_zTableName, _pzTableName);
}

HopsReturnObject::~HopsReturnObject() {
}

int HopsReturnObject::GetByteArray(const NdbRecAttr* attr,
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
int HopsReturnObject::GetString(const NdbRecAttr* attr, string& str) {
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
void HopsReturnObject::processNdbRecAttr(NdbRecAttr * _pNdbRectAttr) {
	NdbValues l_stNdbValues;

	strcpy(l_stNdbValues.m_zColName, _pNdbRectAttr->getColumn()->getName());
	l_stNdbValues.m_dataType = _pNdbRectAttr->getType();

	if (_pNdbRectAttr->isNULL() == 0) {
		// we have a non-null value
		NdbDictionary::Column::Type column_type = _pNdbRectAttr->getType();
		switch (column_type) {
		case NdbDictionary::Column::Char:
		case NdbDictionary::Column::Varchar:
		case NdbDictionary::Column::Longvarchar: {
			std::string l_sDatabaseValue;
			GetString(_pNdbRectAttr, l_sDatabaseValue);
			strcpy(l_stNdbValues.m_zValue, l_sDatabaseValue.c_str());
		}
			break;
		case NdbDictionary::Column::Unsigned: {
			l_stNdbValues.m_u32Value = _pNdbRectAttr->u_32_value();
		}
			break;
		case NdbDictionary::Column::Bigint: {
			l_stNdbValues.m_int64Value = _pNdbRectAttr->int64_value();
		}
			break;
		case NdbDictionary::Column::Int: {
			l_stNdbValues.m_int32Value = _pNdbRectAttr->int32_value();
		}
			break;
		case NdbDictionary::Column::Binary:
		case NdbDictionary::Column::Varbinary:
		case NdbDictionary::Column::Longvarbinary: {
			const char* first;
			size_t l_byteLength;
			GetByteArray(_pNdbRectAttr, first, l_byteLength);
			int sum = 0;
			for (const char* byte = first; byte < first + l_byteLength;
					byte++) {
				l_stNdbValues.m_zbyteArray[sum] = *byte;
				++sum;
			}
			l_stNdbValues.m_byteLength = sum;

		}
			break;
		default: {
			std::cout << "[ReturnObject][FATAL_ERROR] Col name : "
					<< _pNdbRectAttr->getColumn()->getName()
					<< "Unsupported format received from db type :"
					<< _pNdbRectAttr->getColumn()->getType() << std::endl;
		}
			break;
		}

	} else //value is NULL
	{
	  //		std::cout << "[ReturnObject][WARNING] <"
	  //			<< _pNdbRectAttr->getColumn()->getName()
	  //			<< "> has null value , we should pack it java null object. !! Not yet tested "
	  //			<< std::endl;
		l_stNdbValues.m_isThisNull = true;
		m_listOfNdbValues.push_back(l_stNdbValues);
	}

	m_listOfNdbValues.push_back(l_stNdbValues);

}

