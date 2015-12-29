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

import org.apache.ftpserver.ftplet.FtpFile;
import org.apache.ftpserver.ftplet.FileSystemView;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.User;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Implemented FileSystemView to use HdfsFileObject
 *
 * @author <a href="http://mina.apache.org">Apache MINA Project</a> and IpOnWeb
 */

public class HdfsFileSystemView implements FileSystemView {
	private static Logger log = Logger.getLogger(HdfsFileSystemView.class);

	// the root directory will always end with '/'.
	private String rootDir = "/";

	// the first and the last character will always be '/'
	// It is always with respect to the root directory.
	private String currDir = "/";

	private User user;

	// private boolean writePermission;

	private boolean caseInsensitive = false;

	private boolean useVirtUserForCheck = true;

	/**
	 * Constructor - set the user object.
	 */
	protected HdfsFileSystemView(User user) throws FtpException {
		this(user, true, true);
	}

	/**
	 * Constructor - set the user object.
	 */
	protected HdfsFileSystemView(User user, boolean caseInsensitive, boolean useVirtUserForCheck)
			throws FtpException {
		if (user == null) {
			throw new IllegalArgumentException("user cannot be null");
		}
		if (user.getHomeDirectory() == null) {
			throw new IllegalArgumentException(
					"User home directory cannot be null");
		}

		// Always sensitive
		// this.caseInsensitive = caseInsensitive;

		this.useVirtUserForCheck = useVirtUserForCheck;

		// add last '/' if necessary
		String rootDir = user.getHomeDirectory();
		//  rootDir = NativeFileObject.normalizeSeparateChar(rootDir);
		if (!rootDir.endsWith("/")) {
			rootDir += '/';
		}
		this.rootDir = rootDir;
		this.user = user;
		currDir = "/";
	}

	/**
	 * Get the user home directory. It would be the file system root for the
	 * user.
	 */
	public FtpFile getHomeDirectory() {
		return new HdfsFileObject("/", new Path(rootDir), user, useVirtUserForCheck);
	}

    /**
     * Get the current directory.
     */
    public FtpFile getWorkingDirectory() {
        FtpFile fileObj = null;
        if (currDir.equals("/")) {
            fileObj = new HdfsFileObject("/", new Path(rootDir), user, useVirtUserForCheck);
        } else {
            Path path = new Path(rootDir, currDir.substring(1));
            fileObj = new HdfsFileObject(currDir, path, user, useVirtUserForCheck);

        }
        return fileObj;
    }


    /**
     * Get file object.
     */
    public FtpFile getFile(String file) {

        // get actual path object
        String physicalName = HdfsFileObject.getPhysicalName(rootDir,
                currDir, file, caseInsensitive);
        Path fileObj = new Path(physicalName);

        // strip the root directory and return
        String userFileName = physicalName.substring(rootDir.length() - 1);
        return new HdfsFileObject(userFileName, fileObj, user, useVirtUserForCheck);
    }

    /**
     * Change directory.
     */
    public boolean changeWorkingDirectory(String dir) {
        // not a directory - return false
        dir = HdfsFileObject.getPhysicalName(rootDir, currDir, dir,
                caseInsensitive);
        Path dirObj = new Path(dir);
        try {
	        DistributedFileSystem dfs = HdfsOverFtpSystem.getDfs();
	        FileStatus fsDir = dfs.getFileStatus(dirObj);

	        if (!fsDir.isDirectory()) {
	            return false;
	        }

	        // strip user root and add last '/' if necessary
	        dir = dir.substring(rootDir.length() - 1);
	        if (dir.charAt(dir.length() - 1) != '/') {
	            dir = dir + '/';
	        }

	        currDir = dir;
	        return true;
        }
        catch (IOException e) {
        	e.printStackTrace();
        	return false;
        }
    }

	/**
	 * Is the file content random accessible?
	 */
	public boolean isRandomAccessible() {
		return true;
	}

	/**
	 * Dispose file system view - does nothing.
	 */
	public void dispose() {
	}
}
