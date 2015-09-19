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

import java.io.File;
import java.io.IOException;

import org.apache.ftpserver.ftplet.FileSystemFactory;
import org.apache.ftpserver.ftplet.FileSystemView;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

/**
 * HDFS file system based on native file system factory
 * and IPOnWeb HDFSOverFTP.
 *
 * @author <a href="http://mina.apache.org">Apache MINA Project</a> and IpOnWeb
 */
public class HdfsFileSystemFactory implements FileSystemFactory {

    private final Logger LOG = LoggerFactory
            .getLogger(HdfsFileSystemFactory.class);

    private boolean createHome;

    private boolean caseInsensitive = false;

    /**
     * Should the home directories be created automatically
     * @return true if the file system will create the home directory if not available
     */
    public boolean isCreateHome() {
        return createHome;
    }

    /**
     * Set if the home directories be created automatically
     * @param createHome true if the file system will create the home directory if not available
     */

    public void setCreateHome(boolean createHome) {
        this.createHome = createHome;
    }

    /**
     * Is this file system case insensitive.
     * Enabling might cause problems when working against case-sensitive file systems, like on Linux
     * @return true if this file system is case insensitive
     */
    public boolean isCaseInsensitive() {
        return caseInsensitive;
    }

    /**
     * Should this file system be case insensitive.
     * Enabling might cause problems when working against case-sensitive file systems, like on Linux
     * @param caseInsensitive true if this file system should be case insensitive
     */
    public void setCaseInsensitive(boolean caseInsensitive) {
        // Always sensitive
        this.caseInsensitive = false;
    }


    /**
     * Checks if the object does exist
     *
     * @return true if the object does exist
     */
    private boolean doesExist(DistributedFileSystem dfs, Path path) {
        try {
            dfs.getFileStatus(path);
            return true;
        }
        catch (IOException e) {
            return false;
        }
    }

    /**
     * Create the appropriate user file system view.
     */
    public FileSystemView createFileSystemView(User user) throws FtpException {
        HdfsUser huser = (HdfsUser)user;
        synchronized (huser) {
            // create home if does not exist
            if (createHome) {
                String homeDirStr = huser.getHomeDirectory();
                Path homeDirPath = new Path(homeDirStr);

                try {
                    DistributedFileSystem dfs = HdfsOverFtpSystem.getDfs();

                    if (dfs.isFile(homeDirPath)) {
                        LOG.warn("Not a directory " + homeDirStr);
                        throw new FtpException("Not a directory " + homeDirStr);
                    }
                    if (doesExist(dfs, homeDirPath) == false) {
                        dfs.mkdirs(homeDirPath);
                        LOG.info("Created directory " + homeDirStr);
                        if (huser.getName().equals(HdfsOverFtpSystem.getHdfsUser()) == false) {
                            dfs.setOwner(homeDirPath, huser.getName(), huser.getMainGroup());
                        }
                    }
                }
                catch (IOException e) {
                    LOG.warn("Cannot create user home " + homeDirStr + e.toString());
                    throw new FtpException("Cannot create user home " + homeDirStr);
                }
            }

            FileSystemView fsView = new HdfsFileSystemView(user,
                    caseInsensitive);
            return fsView;
        }
    }

}
