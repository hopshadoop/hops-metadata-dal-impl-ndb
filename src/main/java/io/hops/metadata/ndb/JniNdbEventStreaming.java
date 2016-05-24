/*
 * Copyright (C) 2015 hops.io.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.metadata.ndb;

import io.hops.DalNdbEventStreaming;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author sri
 */
/**
 * A helper to load the native hopsndbeventstreamer code i.e. hopsndbevent.so.
 */
public class JniNdbEventStreaming implements DalNdbEventStreaming {

  private static final Log LOG = LogFactory.getLog(JniNdbEventStreaming.class);

  private static boolean nativeCodeLoaded = false;
  private String SchedulerConfPath;
  private String ResourceTrackerConfPath;
  private String connectionString;
  private String databaseName;
  
  public void init(String SchedulerConfPath,
          String ResourceTrackerConfPath, String connectionString,
        String databaseName) {
    this.SchedulerConfPath = SchedulerConfPath;
    this.ResourceTrackerConfPath = ResourceTrackerConfPath;
    this.connectionString = connectionString;
    this.databaseName = databaseName;
  }
  
  static {
    // Try to load native hopsndbevent library and set fallback flag appropriately

      System.loadLibrary("hopsyarn-1.0");
      LOG.info("Loaded the native-hopsndbevent library");

  }

    // native interface functions to start and close event api session. if same JVM start more session, this will crash
  // or gives buggy java objects !!!
  private native void startEventAPISession(String jpath, String jConnectionString,
        String jDatabaseName);

  private native void closeEventAPISession();

  @Override
  public boolean isNativeCodeLoaded() {
    return nativeCodeLoaded;
  }

  @Override
  public void startHopsNdbEvetAPISession(boolean isLeader) {
    LOG.info(
            "Application is requesting to start the api session... only one session per jvm");
    if(isLeader){
      startEventAPISession(SchedulerConfPath, connectionString, databaseName);
    }else{
      startEventAPISession(ResourceTrackerConfPath, connectionString, databaseName);
    }
    LOG.info("Successfully started the event api....");
  }

  @Override
  public void closeHopsNdbEventAPISession() {
    LOG.info("closing EventAPI session");
    closeEventAPISession();
  }

}
