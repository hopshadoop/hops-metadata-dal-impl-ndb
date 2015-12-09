//============================================================================
// Name        : HopsJNICallBackObject.cpp
// Created on  : Feb 23, 2015
// Author      : sri(skug@kth.se)
// Version     : 1.0
// Copyright   : SICS
// Description : Construct all the specified java class in the configuration
//============================================================================
#include "../include/HopsJNICallBackObject.h"
#include <stdlib.h>
#include <unistd.h>
using namespace hopsjni;

HopsNdbJNIContainersObject::HopsNdbJNIContainersObject(JNIEnv *_pJNIPtr) {
	m_jniPtr = _pJNIPtr;
	m_bIsKeyCreated = false;
	mArrayListmap = NULL;
	mObjArrayList = NULL;
	mListClass = NULL;
	mListConstructer = NULL;
	mMethodOfArrayListAdd = NULL;
	mIntclass = NULL;
	mStringClass = NULL;
	mIntMethod = NULL;
	mStringMethod = NULL;
	mMapClass = NULL;
	mMethodOfConstructMap = NULL;
	mArrayListmap = NULL;
	mKeyOfMap = NULL;
	mArraymapPut = NULL;
}

HopsNdbJNIContainersObject::~HopsNdbJNIContainersObject() {
	if (mListClass != NULL) {
		m_jniPtr->DeleteGlobalRef(mListClass);
		mListClass = NULL;
	}
	if (mMapClass != NULL) {
		m_jniPtr->DeleteGlobalRef(mMapClass);
		mMapClass = NULL;
	}
}

void HopsNdbJNIContainersObject::PrepareJNIList() {

	jclass l_listClass = m_jniPtr->FindClass("java/util/ArrayList");
	if (l_listClass == NULL) {
		return; // FindClass already threw an exception such as NoClassDefFoundError.
	}
	mListClass = (jclass) m_jniPtr->NewGlobalRef(l_listClass);

}
void HopsNdbJNIContainersObject::CreateJNIList() {
	jmethodID l_mListConstructer = m_jniPtr->GetMethodID(mListClass, "<init>",
			"()V");

	mObjArrayList = NULL;
	mObjArrayList = m_jniPtr->NewObject(mListClass, l_mListConstructer);
	if (mObjArrayList == NULL) {
		if (m_jniPtr->ExceptionCheck()) {
			m_jniPtr->ExceptionDescribe();
			m_jniPtr->ExceptionClear();
			printf(
					"[HopObject] Reset threw an exception at HopObject creation\n");
			return;
		}
	}

}

void HopsNdbJNIContainersObject::AddListObject(jobject _jObject) {
	jmethodID l_mMethodOfArrayListAdd = m_jniPtr->GetMethodID(mListClass, "add",
			"(Ljava/lang/Object;)Z");
	m_jniPtr->CallObjectMethod(mObjArrayList, l_mMethodOfArrayListAdd,
			_jObject);
	if (m_jniPtr->ExceptionCheck()) {
		m_jniPtr->ExceptionDescribe();
		m_jniPtr->ExceptionClear();
		printf("[HopObject] Reset threw an exception at HopObject creation\n");
		return;
	}

}
void HopsNdbJNIContainersObject::PrepareJNIMapAndList(bool _bIsKeyInt) {

	PrepareJNIList();
	PrepareJNIMap(_bIsKeyInt);
}

void HopsNdbJNIContainersObject::PrepareJNIMap(bool _bIsKeyInt) {
	jclass l_mapClass = m_jniPtr->FindClass("java/util/HashMap");
	if (l_mapClass == NULL) {
		return; // FindClass already threw an exception such as NoClassDefFoundError.
	}
	mMapClass = (jclass) m_jniPtr->NewGlobalRef(l_mapClass);

	mMethodOfConstructMap = m_jniPtr->GetMethodID(mMapClass, "<init>", "()V");
	mArraymapPut = m_jniPtr->GetMethodID(mMapClass, "put",
			"(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");

	//later these methods have to change to template, this will only support int and string
	if (_bIsKeyInt) {
		jclass l_mIntclass = m_jniPtr->FindClass("java/lang/Integer");
		mIntclass = (jclass) m_jniPtr->NewGlobalRef(l_mIntclass);
		m_jniPtr->DeleteLocalRef(l_mIntclass);

		mIntMethod = m_jniPtr->GetMethodID(mIntclass, "<init>", "(I)V");
	} else {
		jclass l_mStringClass = m_jniPtr->FindClass("java/lang/String");
		mStringClass = (jclass) m_jniPtr->NewGlobalRef(l_mStringClass);
		m_jniPtr->DeleteLocalRef(l_mStringClass);

		mStringMethod = m_jniPtr->GetMethodID(mStringClass, "<init>",
				"(Ljava/lang/String;)V");
	}

}

/* these are the methods to use to store java map list objects*/
void HopsNdbJNIContainersObject::CreateJNIMapAndList() {
	CreateJNIList();
	jobject l_mArrayListmap = m_jniPtr->NewObject(mMapClass,
			mMethodOfConstructMap);
	mArrayListmap = m_jniPtr->NewGlobalRef(l_mArrayListmap);
	m_jniPtr->DeleteLocalRef(l_mArrayListmap);

}
void HopsNdbJNIContainersObject::AddingToMapList(int _iKey, jobject _oValue) {
	std::map<int, jobject>::iterator l_mapItr = m_cplusplusmapformaplist.find(
			_iKey);
	if (l_mapItr == m_cplusplusmapformaplist.end()) {

		jobject l_jObjArrayList = m_jniPtr->NewObject(mListClass,
				mListConstructer);
		m_jniPtr->CallObjectMethod(l_jObjArrayList, mMethodOfArrayListAdd,
				_oValue);
		m_cplusplusmapformaplist.insert(
				std::make_pair<int, jobject>(_iKey, l_jObjArrayList));
		std::cout << "creating key : " << _iKey << " | object refefnce : "
				<< l_jObjArrayList << std::endl;
	} else {
		m_jniPtr->CallObjectMethod(m_cplusplusmapformaplist[_iKey],
				mMethodOfArrayListAdd, _oValue);
	}

}

jobject HopsNdbJNIContainersObject::GetMapListObjects() {

	std::map<int, jobject>::iterator l_mapItr =
			m_cplusplusmapformaplist.begin();
	for (; l_mapItr != m_cplusplusmapformaplist.end(); ++l_mapItr) {
		jobject l_jIntKey = m_jniPtr->NewObject(mIntclass, mIntMethod,
				l_mapItr->first);
		m_jniPtr->CallObjectMethod(mArrayListmap, mArraymapPut, l_jIntKey,
				l_mapItr->second);

		m_jniPtr->DeleteLocalRef(l_jIntKey);
	}

	return mArrayListmap;

}

void HopsNdbJNIContainersObject::DeleteMapListReference() {
	std::map<int, jobject>::iterator l_mapItr =
			m_cplusplusmapformaplist.begin();
	for (; l_mapItr != m_cplusplusmapformaplist.end(); ++l_mapItr) {
		m_jniPtr->DeleteGlobalRef(l_mapItr->second);
		std::cout << "deleting the key and reference : " << l_mapItr->first
				<< "|vale:" << l_mapItr->second << std::endl;
		m_cplusplusmapformaplist.erase(l_mapItr);
	}
	m_cplusplusmapformaplist.clear();
}
void HopsNdbJNIContainersObject::CreateJNIMap() {
	jobject l_mArrayListmap = m_jniPtr->NewObject(mMapClass,
			mMethodOfConstructMap);
	mArrayListmap = m_jniPtr->NewGlobalRef(l_mArrayListmap);
	m_jniPtr->DeleteLocalRef(l_mArrayListmap);

}

void HopsNdbJNIContainersObject::DeleteLocalMapListReferences() {
	m_jniPtr->DeleteLocalRef(mObjArrayList);
}

void HopsNdbJNIContainersObject::DeleteLocalMapReferences() {
	m_jniPtr->DeleteGlobalRef(mKeyOfMap);
	m_jniPtr->DeleteGlobalRef(mArrayListmap);

}

void HopsNdbJNIContainersObject::AddMapObjects(const char *_pzKey,
		jobject _jMapObjects) {

	mKeyOfMap = m_jniPtr->NewStringUTF(_pzKey);
	m_jniPtr->CallObjectMethod(mArrayListmap, mArraymapPut, mKeyOfMap,
			_jMapObjects);

}

HopObject::HopObject(JNIEnv *_pJNIPtr) {
	m_jniPtr = _pJNIPtr;
	m_objHopCreatedHopObj = NULL;
	m_callbackClass = NULL;
	m_methdHopClassMethod = NULL;
	m_NewObjectFunction = NULL;
}

HopObject::~HopObject() {
	m_jniPtr->DeleteLocalRef(m_callbackClass);
}

void HopObject::SetUpHopObject(const char *_pzClassPath,
		const char *_pzConstructerSignature,
		std::map<const char*, const char*> &_mapMemberVariableSignature,
		std::map<const char*, const char*> &_mapDatabaseToJavaFields) {
	jclass l_classHopClass = m_jniPtr->FindClass(_pzClassPath);

	if (l_classHopClass == NULL) {
		cout << "[EventProcessor] Java object " << _pzClassPath
				<< " construction failed. " << std::endl;
		exit(EXIT_FAILURE); // FindClass already threw an exception such as NoClassDefFoundError.
	}
	m_callbackClass = (jclass) m_jniPtr->NewGlobalRef(l_classHopClass);
	m_methdHopClassMethod = m_jniPtr->GetMethodID(m_callbackClass, "<init>",
			_pzConstructerSignature);

	std::map<const char*, const char *>::iterator l_mapItr =
			_mapMemberVariableSignature.begin();
	for (; l_mapItr != _mapMemberVariableSignature.end(); ++l_mapItr) {
		jfieldID l_sJavaField = NULL;
		l_sJavaField = m_jniPtr->GetFieldID(m_callbackClass, l_mapItr->first,
				l_mapItr->second);
		if (l_sJavaField == NULL) {
			cout << "[EventProcessor] Java field is not found  "
					<< l_mapItr->first << " construction failed. "
					<< _pzClassPath << std::endl;
		}
		std::string l_sClassMemberName(l_mapItr->first);
		m_mapClassMemberToJavaField.insert(
				std::make_pair<std::string, jfieldID>(l_sClassMemberName,
						l_sJavaField));
		if (strcmp(l_mapItr->second, "Z") == 0) {
			//this class have boolean field, this is because ndb , we are storing int field.
			m_mapClassMemberBoolFields.insert(
					std::make_pair<std::string, bool>(l_sClassMemberName,
							true));
		}
	}
	l_mapItr = _mapDatabaseToJavaFields.begin();
	for (; l_mapItr != _mapDatabaseToJavaFields.end(); ++l_mapItr) {
		std::string l_sDatabaseName(l_mapItr->first);
		std::string l_sMemberName(l_mapItr->second);
		m_mapDatabaseToJavaFieldMap.insert(
				std::make_pair<std::string, std::string>(l_sDatabaseName,
						l_sMemberName));
	}

}

void HopObject::PrepareHopObject(int _iSize) {
	switch (_iSize) {
	case 1: {
		m_objHopCreatedHopObj = m_jniPtr->NewObject(m_callbackClass,
				m_methdHopClassMethod, "");

	}
		break;
	case 2: {
		m_objHopCreatedHopObj = m_jniPtr->NewObject(m_callbackClass,
				m_methdHopClassMethod, "", "");
	}
		break;
	case 3: {
		m_objHopCreatedHopObj = m_jniPtr->NewObject(m_callbackClass,
				m_methdHopClassMethod, "", "", "");
	}
		break;
	case 4: {
		m_objHopCreatedHopObj = m_jniPtr->NewObject(m_callbackClass,
				m_methdHopClassMethod, "", "", "", "");
	}
		break;
	case 5: {
		m_objHopCreatedHopObj = m_jniPtr->NewObject(m_callbackClass,
				m_methdHopClassMethod, "", "", "", "", "");
	}
		break;
	case 6: {
		m_objHopCreatedHopObj = m_jniPtr->NewObject(m_callbackClass,
				m_methdHopClassMethod, "", "", "", "", "", "");
	}
		break;
	case 7: {
		m_objHopCreatedHopObj = m_jniPtr->NewObject(m_callbackClass,
				m_methdHopClassMethod, "", "", "", "", "", "", "");
	}
		break;
	case 8: {
		m_objHopCreatedHopObj = m_jniPtr->NewObject(m_callbackClass,
				m_methdHopClassMethod, "", "", "", "", "", "", "", "");
	}
		break;
	case 9: {
		m_objHopCreatedHopObj = m_jniPtr->NewObject(m_callbackClass,
				m_methdHopClassMethod, "", "", "", "", "", "", "", "", "");
	}
		break;
	case 10: {
		m_objHopCreatedHopObj = m_jniPtr->NewObject(m_callbackClass,
				m_methdHopClassMethod, "", "", "", "", "", "", "", "", "", "");
	}
		break;
	case 11: {
		m_objHopCreatedHopObj = m_jniPtr->NewObject(m_callbackClass,
				m_methdHopClassMethod, "", "", "", "", "", "", "", "", "", "",
				"");
	}
		break;
	case 12: {
		m_objHopCreatedHopObj = m_jniPtr->NewObject(m_callbackClass,
				m_methdHopClassMethod, "", "", "", "", "", "", "", "", "", "",
				"", "");
	}
		break;
	default: {
		printf("[HopObject] Constructor length is too much \n");
		exit(EXIT_FAILURE);
	}
		break;
	}

	if (m_jniPtr->ExceptionCheck()) {
		m_jniPtr->ExceptionDescribe();
		m_jniPtr->ExceptionClear();
		printf("[HopObject] Reset threw an exception at HopObject creation\n");
		return;
	}

}

void HopObject::CreateJavaCompObject(int _iSize) {
	PrepareHopObject(_iSize);

}
void HopObject::SetFinalJavaField(std::string _sTablename, jobject _jobject) {
	std::string l_sMemberName = m_mapDatabaseToJavaFieldMap[_sTablename];
	m_jniPtr->SetObjectField(m_objHopCreatedHopObj,
			m_mapClassMemberToJavaField[l_sMemberName], _jobject);
	if (m_jniPtr->ExceptionCheck()) {
		m_jniPtr->ExceptionDescribe();
		m_jniPtr->ExceptionClear();
		printf("[HopObject] Reset threw an exception at HopObject creation\n");
		return;
	}

}
jobject HopObject::CreateJavaObject(
		std::map<std::string, jobject> & _mapCallBackObjects) {
	PrepareHopObject(_mapCallBackObjects.size());
	std::map<std::string, jobject>::iterator l_mapItr =
			_mapCallBackObjects.begin();
	for (; l_mapItr != _mapCallBackObjects.end(); ++l_mapItr) {
		//this will give the member name
		std::string l_sMemberName = m_mapDatabaseToJavaFieldMap[l_mapItr->first];
		m_jniPtr->SetObjectField(m_objHopCreatedHopObj,
				m_mapClassMemberToJavaField[l_sMemberName], l_mapItr->second);
		if (m_jniPtr->ExceptionCheck()) {
			m_jniPtr->ExceptionDescribe();
			m_jniPtr->ExceptionClear();
			printf(
					"[HopObject] Reset threw an exception at HopObject creation\n");
		}

	}
	return m_objHopCreatedHopObj;
}

void HopObject::SetCallBackClassAndObject(jclass _callbackClass,
		jobject _callbackObject) {
	m_callbackClass = _callbackClass;
	m_callbackCalssObject = _callbackObject;
}
void HopObject::PrepareHopJavaObjects(const char *_pzNewObjectFunctionName,
		std::map<const char*, const char*> &_mapDbColNameToJavaFunctions,
		std::map<const char *, const char *> &_mapDbColNameToSignatures) {

	std::map<const char*, const char *>::iterator l_mapItr =
			_mapDbColNameToJavaFunctions.begin();
	std::map<const char*, const char *>::iterator l_mapItr2 =
			_mapDbColNameToSignatures.begin();
	if (!(strcmp(_pzNewObjectFunctionName, "X") == 0)) {
		m_NewObjectFunction = m_jniPtr->GetMethodID(m_callbackClass,
				_pzNewObjectFunctionName, "()V");
	}

	for (; l_mapItr != _mapDbColNameToJavaFunctions.end(); ++l_mapItr) {

		jmethodID l_mdCallBackMethod = m_jniPtr->GetMethodID(m_callbackClass,
				l_mapItr->second, l_mapItr2->second);
		if (l_mdCallBackMethod == NULL) {
			cout << "[EventProcessor] Java method is not found  "
					<< l_mapItr->first << " construction failed. " << std::endl;
		}
		std::string l_sDbColName(l_mapItr->first);
		m_mapClassMemberToJavaMethod.insert(
				std::make_pair<std::string, jmethodID>(l_sDbColName,
						l_mdCallBackMethod));
		if (strchr(l_mapItr2->second, 'Z') != NULL) {
			//this class have boolean field, this is because ndb , we are storing int field.
			m_mapClassMemberBoolFields.insert(
					std::make_pair<std::string, bool>(l_sDbColName, true));
		}
		++l_mapItr2;
	}

}

void HopObject::FireNewClassMethod() {
	if (m_NewObjectFunction != NULL)
		m_jniPtr->CallVoidMethod(m_callbackCalssObject, m_NewObjectFunction);
}

void HopObject::BuildHopJavaObject(vector<NdbValues> & _refNdbValues) {

	for (int i = 0; i < (int) _refNdbValues.size(); ++i) {
		std::string l_sDatabaseColName(_refNdbValues[i].getColName());

		jmethodID l_javaMethod =
				m_mapClassMemberToJavaMethod[l_sDatabaseColName];
		switch (_refNdbValues[i].getDataType()) {

		case NdbDictionary::Column::Char:
		case NdbDictionary::Column::Varchar:
		case NdbDictionary::Column::Longvarchar: {
			if (_refNdbValues[i].getCharValue() != NULL) {
				jstring l_jStringMember = m_jniPtr->NewStringUTF(
						_refNdbValues[i].getCharValue());
				m_jniPtr->CallVoidMethod(m_callbackCalssObject, l_javaMethod,
						l_jStringMember);
				m_jniPtr->DeleteLocalRef(l_jStringMember);
			} else {
				m_jniPtr->CallVoidMethod(m_callbackCalssObject, l_javaMethod,
				NULL);
			}
		}
			break;
		case NdbDictionary::Column::Unsigned: {
			m_jniPtr->CallVoidMethod(m_callbackCalssObject, l_javaMethod,
					_refNdbValues[i].getUint32Value());
		}
			break;
		case NdbDictionary::Column::Bigint: {
			if (_refNdbValues[i].getInt64Value() != -1) {
				m_jniPtr->CallVoidMethod(m_callbackCalssObject, l_javaMethod,
						_refNdbValues[i].getInt64Value());
			} else {
				m_jniPtr->CallVoidMethod(m_callbackCalssObject, l_javaMethod,
						0);
			}
		}
			break;
		case NdbDictionary::Column::Int: {
			if (m_mapClassMemberBoolFields[l_sDatabaseColName]) {
				//this is boolean field
				bool isTrue =
						_refNdbValues[i].getInt32Value() == 1 ? true : false;
				if (isTrue)
					m_jniPtr->CallVoidMethod(m_callbackCalssObject,
							l_javaMethod,
							JNI_TRUE);
				else
					m_jniPtr->CallVoidMethod(m_callbackCalssObject,
							l_javaMethod,
							JNI_FALSE);
			} else {
				m_jniPtr->CallVoidMethod(m_callbackCalssObject, l_javaMethod,
						_refNdbValues[i].getInt32Value());
			}
		}
			break;
		case NdbDictionary::Column::Binary:
		case NdbDictionary::Column::Varbinary:
		case NdbDictionary::Column::Longvarbinary: {

			if (_refNdbValues[i].m_byteLength != 0) {
				jbyteArray l_jByteArray = m_jniPtr->NewByteArray(
						_refNdbValues[i].m_byteLength);
				m_jniPtr->SetByteArrayRegion(l_jByteArray, 0,
						_refNdbValues[i].m_byteLength,
						(jbyte*) _refNdbValues[i].getByteArray());
				m_jniPtr->CallVoidMethod(m_callbackCalssObject, l_javaMethod,
						l_jByteArray);
				m_jniPtr->DeleteLocalRef(l_jByteArray);
			} else {
				m_jniPtr->CallVoidMethod(m_callbackCalssObject, l_javaMethod,
				NULL);
			}

		}
			break;
		default: {
			printf(
					"[HopObject] Unknown Ndb data type received , Setting Java oject to null -data type: %d\n",
					_refNdbValues[i].getDataType());
			m_jniPtr->CallVoidMethod(m_callbackCalssObject, l_javaMethod,
			NULL);
		}
			break;
		}
	}

}
void HopObject::SimulationBuildHopJavaObject(
		vector<SimulationNdbValues> & _refNdbValues) {

	for (int i = 0; i < (int) _refNdbValues.size(); ++i) {
		std::string l_sDatabaseColName(_refNdbValues[i].getColName());
		jmethodID l_javaMethod =
				m_mapClassMemberToJavaMethod[l_sDatabaseColName];
		switch (_refNdbValues[i].getDataType()) {

		case NdbDictionary::Column::Char:
		case NdbDictionary::Column::Varchar:
		case NdbDictionary::Column::Longvarchar: {
			if (_refNdbValues[i].getCharValue() != NULL) {
				jstring l_jStringMember = m_jniPtr->NewStringUTF(
						_refNdbValues[i].getCharValue());
				m_jniPtr->CallVoidMethod(m_callbackCalssObject, l_javaMethod,
						l_jStringMember);
				m_jniPtr->DeleteLocalRef(l_jStringMember);
			} else {
				m_jniPtr->CallVoidMethod(m_callbackCalssObject, l_javaMethod,
				NULL);
			}
		}
			break;
		case NdbDictionary::Column::Int: {
			m_jniPtr->CallVoidMethod(m_callbackCalssObject, l_javaMethod,
					_refNdbValues[i].getInt32Value());
		}
			break;

		default: {
			printf(
					"[HopObject] Unknown Ndb data type received , Setting Java oject to null -data type: %d\n",
					_refNdbValues[i].getDataType());
			m_jniPtr->CallVoidMethod(m_callbackCalssObject, l_javaMethod,
			NULL);
		}
			break;
		}
	}

}

