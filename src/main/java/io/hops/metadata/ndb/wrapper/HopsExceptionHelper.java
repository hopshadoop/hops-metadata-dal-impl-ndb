/*
 * Hops Database abstraction layer for storing the hops metadata in MySQL Cluster
 * Copyright (C) 2015  hops.io
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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package io.hops.metadata.ndb.wrapper;

import com.mysql.clusterj.ClusterJDatastoreException;
import com.mysql.clusterj.ClusterJException;
import io.hops.exception.StorageException;
import io.hops.exception.TransientStorageException;

public class HopsExceptionHelper {
  public static StorageException wrap(ClusterJException e) {
    if (isTransient(e)) {
      return new TransientStorageException(e);
    } else {
      return new StorageException(e);
    }
  }

  private static boolean isTransient(ClusterJException e) {
    if (e instanceof ClusterJDatastoreException) {
      // http://dev.mysql.com/doc/ndbapi/en/ndb-error-classifications.html
      // The classifications can be found in ndberror.h and ndberror.c in the ndb sources
      if (e.getMessage().contains("classification 7")) {
        // Temporary Resource error (TR)
        return true;
      } else if (e.getMessage().contains("classification 8")) {
        // Node Recovery error (NR)
        return true;
      } else if (e.getMessage().contains("classification 9")) {
        // Overload error (OL)
        return true;
      } else if (e.getMessage().contains("classification 10")) {
        // Timeout expired (TO)
        return true;
      } else if (e.getMessage().contains("classification 15")) {
        // Node shutdown (NS)
        return true;
      } else if (e.getMessage().contains("classification 18")) {
        // Internal temporary (IT)
        return true;
      }
    }
    return false;
  }
}
