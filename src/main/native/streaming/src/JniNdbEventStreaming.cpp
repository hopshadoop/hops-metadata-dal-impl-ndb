/*
 * Copyright (C) 2016 Hops.io
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */


#include "JniNdbEventStreaming.h"
#include "JniNdbEventStreamingImp.h"
#include <jni.h>

static JniNdbEventStreamingImp *imp;

JNIEXPORT void JNICALL Java_io_hops_metadata_ndb_JniNdbEventStreaming_startEventAPISession(
		JNIEnv *env, jobject thisObj, jboolean jIsLeader, jstring jConnectionString,
        jstring jDatabaseName) {

imp = new JniNdbEventStreamingImp(env,jIsLeader,jConnectionString,jDatabaseName);  
}


JNIEXPORT void JNICALL Java_io_hops_metadata_ndb_JniNdbEventStreaming_closeEventAPISession(
		JNIEnv *env, jobject thisObj) {
  delete imp;
  
}

