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

import java.util.Date;

/**
 * MBean interface for the ftp server statistical information.
 * from FtpStatistics interface
 *
 * @author <a href="http://mina.apache.org">Apache MINA Project</a>
 */
public interface HdfsFtpStatisticsMBean {
    /**
     * Get the server start time.
     * @return The {@link Date} when the server started
     */
    public Date getStartTime();

    /**
     * Get number of files uploaded.
     * @return The total number of uploads
     */
    public int getTotalUploadNumber();

    /**
     * Get number of files downloaded.
     * @return The total number of downloads
     */
    public int getTotalDownloadNumber();

    /**
     * Get number of files deleted.
     * @return The total number of deletions
     */
    public int getTotalDeleteNumber();

    /**
     * Get total number of bytes uploaded.
     * @return The total number of bytes uploaded
     */
    public long getTotalUploadSize();

    /**
     * Get total number of bytes downloaded.
     * @return The total number of bytes downloaded
     */
    public long getTotalDownloadSize();

    /**
     * Get total directory created.
     * @return The total number of created directories
     */
    public int getTotalDirectoryCreated();

    /**
     * Get total directory removed.
     * @return The total number of removed directories
     */
    public int getTotalDirectoryRemoved();

    /**
     * Get total number of connections
     * @return The total number of connections
     */
    public int getTotalConnectionNumber();

    /**
     * Get current number of connections.
     * @return The current number of connections
     */
    public int getCurrentConnectionNumber();

    /**
     * Get total login number.
     * @return The total number of logins
     */
    public int getTotalLoginNumber();

    /**
     * Get total failed login number.
     * @return The total number of failed logins
     */
    public int getTotalFailedLoginNumber();

    /**
     * Get current login number
     * @return The current number of logins
     */
    public int getCurrentLoginNumber();

    /**
     * Get total anonymous login number.
     * @return The total number of anonymous logins
     */
    public int getTotalAnonymousLoginNumber();

    /**
     * Get current anonymous login number.
     * @return The current number of anonymous logins
     */
    public int getCurrentAnonymousLoginNumber();

    /**
     * Reset the cumulative counters.
     */
    public void resetStatisticsCounters();

}
