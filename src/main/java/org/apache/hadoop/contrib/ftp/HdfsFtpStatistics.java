/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.contrib.ftp;

import org.apache.ftpserver.ftplet.FtpStatistics;
import org.apache.ftpserver.impl.DefaultFtpStatistics;

import java.util.Date;

/**
 * MBean interface for the ftp server statistical information.
 * from FtpStatistics interface
 *
 * @author <a href="http://mina.apache.org">Apache MINA Project</a>
 */
public class HdfsFtpStatistics implements HdfsFtpStatisticsMBean {
   private DefaultFtpStatistics ftpStats;

   /**
    * Create a {@link HdfsFtpStatistics} MBean instance based
    *   using data from {@link FtpStatistics}
    * @return The {@link HdfsFtpStatistics} instance
    */
   public HdfsFtpStatistics(DefaultFtpStatistics ftpStats) {
      this.ftpStats = ftpStats;
   }

   /**
    * Get the server start time.
    * @return The {@link Date} when the server started
    */
   public Date getStartTime() {
      return this.ftpStats.getStartTime();
   }

   /**
    * Get number of files uploaded.
    * @return The total number of uploads
    */
   public int getTotalUploadNumber() {
      return this.ftpStats.getTotalUploadNumber();
   }

   /**
    * Get number of files downloaded.
    * @return The total number of downloads
    */
   public int getTotalDownloadNumber() {
      return this.ftpStats.getTotalDownloadNumber();
   }

   /**
    * Get number of files deleted.
    * @return The total number of deletions
    */
   public int getTotalDeleteNumber() {
      return this.ftpStats.getTotalDeleteNumber();
   }

   /**
    * Get total number of bytes uploaded.
    * @return The total number of bytes uploaded
    */
   public long getTotalUploadSize() {
      return this.ftpStats.getTotalUploadSize();
   }

   /**
    * Get total number of bytes downloaded.
    * @return The total number of bytes downloaded
    */
   public long getTotalDownloadSize() {
      return this.ftpStats.getTotalDownloadSize();
   }

   /**
    * Get total directory created.
    * @return The total number of created directories
    */
   public int getTotalDirectoryCreated() {
      return this.ftpStats.getTotalDirectoryCreated();
   }

   /**
    * Get total directory removed.
    * @return The total number of removed directories
    */
   public int getTotalDirectoryRemoved() {
      return this.ftpStats.getTotalDirectoryRemoved();
   }

   /**
    * Get total number of connections
    * @return The total number of connections
    */
   public int getTotalConnectionNumber() {
      return this.ftpStats.getTotalConnectionNumber();
   }

   /**
    * Get current number of connections.
    * @return The current number of connections
    */
   public int getCurrentConnectionNumber() {
      return this.ftpStats.getCurrentConnectionNumber();
   }

   /**
    * Get total login number.
    * @return The total number of logins
    */
   public int getTotalLoginNumber() {
      return this.ftpStats.getTotalLoginNumber();
   }

   /**
    * Get total failed login number.
    * @return The total number of failed logins
    */
   public int getTotalFailedLoginNumber() {
      return this.ftpStats.getTotalFailedLoginNumber();
   }

   /**
    * Get current login number
    * @return The current number of logins
    */
   public int getCurrentLoginNumber() {
      return this.ftpStats.getCurrentLoginNumber();
   }

   /**
    * Get total anonymous login number.
    * @return The total number of anonymous logins
    */
   public int getTotalAnonymousLoginNumber() {
      return this.ftpStats.getTotalAnonymousLoginNumber();
   }

   /**
    * Get current anonymous login number.
    * @return The current number of anonymous logins
    */
   public int getCurrentAnonymousLoginNumber() {
      return this.ftpStats.getCurrentAnonymousLoginNumber();
   }

    /**
     * Reset the cumulative counters.
     */
    public void resetStatisticsCounters() {
      this.ftpStats.resetStatisticsCounters();
    }
}
