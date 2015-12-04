/*
 * JNICallBackObject.h
 *
 *  Created on: Feb 23, 2015
 *      Author: sri
 */

#ifndef HOPSCALLBACKOBJECT_H_
#define HOPSCALLBACKOBJECT_H_
#include <jni.h>
#include <iostream>
#include "HopsReturnObject.h"
#include "HopsSimulationReturnObject.h"
using namespace std;
namespace hopsjni {

		class HopsNdbJNIContainersObject {
		public:
			    HopsNdbJNIContainersObject(JNIEnv *_pJNIPtr);
				virtual ~HopsNdbJNIContainersObject();
				void PrepareJNIList();
				void CreateJNIList();
				jobject GetListOfJNIObjects(){ return mObjArrayList;}

				void PrepareJNIMapAndList(bool _bIsKeyInt);
				jobject GetMapListObjects();
				void AddListObject(jobject _jObject);
				void DeleteLocalMapListReferences();
				void AddMapObjects(const char *_pzKey, jobject _jMapObjects);
				jobject GetMapObjects(){return mArrayListmap;}
				void PrepareJNIMap(bool _bIsKeyInt);
				void CreateJNIMapAndList();
				void CreateJNIMap();
				void SetKeyCreated(bool _bKeyCreated){m_bIsKeyCreated=_bKeyCreated;}
				void DeleteLocalMapReferences();
				void SetTableName(std::string _sTableName){m_TableName=_sTableName;}
				std::string GetTableName(){return m_TableName;}
                void AddingToMapList(int _iKey, jobject _oValue);
                void DeleteMapListReference();
                void SetJniPointerAgain(JNIEnv *ptr){m_jniPtr = ptr;}
		private:
				JNIEnv *m_jniPtr;

				jclass mListClass;
				jmethodID mListConstructer;
				jobject mObjArrayList;
				jmethodID mMethodOfArrayListAdd;

				jclass mIntclass;
				jclass mStringClass;

				jmethodID mIntMethod;
				jmethodID mStringMethod;

				jclass mMapClass;
				jmethodID mMethodOfConstructMap;
				jobject mArrayListmap;
				jobject mKeyOfMap;
				jmethodID mArraymapPut;
				bool m_bIsKeyCreated;
				std::string m_TableName;

				std::map<int,jobject> m_cplusplusmapformaplist;
		};
		  class HopObject {

		  public:
			  HopObject(JNIEnv *_pJNIPtr);
			  virtual ~HopObject();
			  void SetCallBackClassAndObject(jclass _callbackClass, jobject _callbackObject);
			  void SetUpHopObject(const char *_pzClassPath,const char *_pzConstructerSignature, std::map<const char*,const char*> &_mapMemberVariableSignature,std::map<const char*,const char*> &_mapDatabaseToJavaFields);
			  jobject CreateJavaObject(vector<NdbValues> & _refNdbValues);
			  jobject CreateJavaObject(std::map<std::string,jobject> & _mapCallBackObjects);
			  void  CreateJavaCompObject(int _iSize);
			  void SetFinalJavaField(std::string _sTablename, jobject _jobject);
			  jobject GetTheCompObject(){return m_objHopCreatedHopObj;}
			  void SetJniPointerAgain(JNIEnv *ptr){m_jniPtr = ptr;}
			  void PrepareHopJavaObjects(const char *_pzNewObjectFunctionName,std::map<const char*, const char*> &_mapDbColNameToJavaFunctions, std::map<const char *, const char *> &_mapDbColNameToSignatures);
			  void BuildHopJavaObject(vector<NdbValues> & _refNdbValues);
		      void FireNewClassMethod();
		      //Load simulation methods
		      void SimulationBuildHopJavaObject(vector<SimulationNdbValues> & _refNdbValues);
		  private:
			  void PrepareHopObject(int _iSize);
			  jobject m_objHopCreatedHopObj;
			  jobject m_callbackCalssObject;
			  jclass  m_callbackClass;
			  jmethodID m_methdHopClassMethod;
			  JNIEnv *m_jniPtr;
			  std::map<std::string, std::string> m_mapDatabaseToJavaFieldMap;
			  std::map<std::string, jfieldID> m_mapClassMemberToJavaField;
			  std::map<std::string,bool> m_mapClassMemberBoolFields;

			  jmethodID m_NewObjectFunction;
			  std::map<std::string, jmethodID> m_mapClassMemberToJavaMethod;
		  };
}

#endif /* HOPSCALLBACKOBJECT_H_ */
