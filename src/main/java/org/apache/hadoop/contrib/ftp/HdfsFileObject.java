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
import org.apache.ftpserver.ftplet.User;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.StringTokenizer;

/**
 * <strong>Internal class, do not use directly.</strong>
 *
 * This class wraps HDFS file object.
 *
 * @author <a href="http://mina.apache.org">Apache MINA Project</a> and IpOnWeb
 */
public class HdfsFileObject implements FtpFile {

	private final Logger log = LoggerFactory.getLogger(HdfsFileObject.class);

    // the file name with respect to the user root.
    // The path separator character will be '/' and
    // it will always begin with '/'.
    private String fileName;

    private Path path;

    private HdfsUser user;

    /**
     * Constructor, internal do not use directly.
     */
    protected HdfsFileObject(final String fileName, final Path path,
            final User user) {
        if (fileName == null) {
            throw new IllegalArgumentException("fileName can not be null");
        }
        if (path == null) {
            throw new IllegalArgumentException("path can not be null");
        }

        if (fileName.length() == 0) {
            throw new IllegalArgumentException("fileName can not be empty");
        } else if (fileName.charAt(0) != '/') {
            throw new IllegalArgumentException(
                    "fileName must be an absolut path");
        }

        this.fileName = fileName;
        this.path = path;
        this.user = (HdfsUser)user;
    }


	/**
	 * Get full name of the object
	 *
	 * @return full name of the object
	 */
	public String getAbsolutePath() {
        // strip the last '/' if necessary
        String fullName = fileName;
        int filelen = fullName.length();
        if ((filelen != 1) && (fullName.charAt(filelen - 1) == '/')) {
            fullName = fullName.substring(0, filelen - 1);
        }

        return fullName;
	}


	/**
	 * Get short name of the object
	 *
	 * @return short name of the object
	 */
    public String getName() {

        // root - the short name will be '/'
        if (fileName.equals("/")) {
            return "/";
        }

        // strip the last '/'
        String shortName = fileName;
        int filelen = fileName.length();
        if (shortName.charAt(filelen - 1) == '/') {
            shortName = shortName.substring(0, filelen - 1);
        }

        // return from the last '/'
        int slashIndex = shortName.lastIndexOf('/');
        if (slashIndex != -1) {
            shortName = shortName.substring(slashIndex + 1);
        }
        return shortName;
    }


	/**
	 * HDFS has no hidden objects
	 *
	 * @return always false
	 */
	public boolean isHidden() {
		return false;
	}

	/**
	 * Checks if the object is a directory
	 *
	 * @return true if the object is a directory
	 */
	public boolean isDirectory() {
		try {
			log.debug("is directory? : " + path);
			DistributedFileSystem dfs = HdfsOverFtpSystem.getDfs();
			FileStatus fs = dfs.getFileStatus(path);
			return fs.isDir();
		} catch (IOException e) {
			log.debug(path + " is not dir", e);
			return false;
		}
	}

	/**
	 * Get HDFS permissions
	 *
	 * @return HDFS permissions as a FsPermission instance
	 * @throws IOException if path doesn't exist so we get permissions of parent object in that case
	 */
	private FsPermission getPermissions() throws IOException {
//        try {
		DistributedFileSystem dfs = HdfsOverFtpSystem.getDfs();
		return dfs.getFileStatus(path).getPermission();
//        } catch (IOException e) {
//            e.printStackTrace();
//            return null;
//        }
	}

	/**
	 * Checks if the object is a file
	 *
	 * @return true if the object is a file
	 */
	public boolean isFile() {
		try {
			DistributedFileSystem dfs = HdfsOverFtpSystem.getDfs();
			return dfs.isFile(path);
		} catch (IOException e) {
			log.debug(path + " is not file", e);
			return false;
		}
	}

	/**
	 * Checks if the object does exist
	 *
	 * @return true if the object does exist
	 */
	public boolean doesExist() {
		try {
			DistributedFileSystem dfs = HdfsOverFtpSystem.getDfs();
			dfs.getFileStatus(path);
			return true;
		} catch (IOException e) {
			//   log.debug(path + " does not exist", e);
			return false;
		}
	}

	/**
	 * Checks if the user has a read permission on the object
	 *
	 * @return true if the user can read the object
	 */
	public boolean isReadable() {
		try {
			FsPermission permissions = getPermissions();
			if (user.getName().equals(getOwnerName())) {
				if (permissions.toString().substring(0, 1).equals("r")) {
					log.debug("PERMISSIONS: " + path + " - " + " read allowed for user");
					return true;
				}
			} else if (user.isGroupMember(getGroupName())) {
				if (permissions.toString().substring(3, 4).equals("r")) {
					log.debug("PERMISSIONS: " + path + " - " + " read allowed for group");
					return true;
				}
			} else {
				if (permissions.toString().substring(6, 7).equals("r")) {
					log.debug("PERMISSIONS: " + path + " - " + " read allowed for others");
					return true;
				}
			}
			log.debug("PERMISSIONS: " + path + " - " + " read denied");
			return false;
		} catch (IOException e) {
			e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
			return false;
		}
	}

	/**
	 * Gets parent
	 *
	 * @return parent object
	 */
	private HdfsFileObject getParent() {
        int indexOfSlash = fileName.lastIndexOf('/');
        String parentFullName;
        if (indexOfSlash == 0) {
            parentFullName = "/";
        } else {
            parentFullName = fileName.substring(0, indexOfSlash);
        }

        return new HdfsFileObject(parentFullName, path.getParent(), user);
	}

	/**
	 * Checks if the user has a write permission on the object
	 *
	 * @return true if the user has write permission on the object
	 */
	public boolean isWritable() {
		try {
			FsPermission permissions = getPermissions();
			if (user.getName().equals(getOwnerName())) {
				if (permissions.toString().substring(1, 2).equals("w")) {
					log.debug("PERMISSIONS: " + path + " - " + " write allowed for user");
					return true;
				}
			} else if (user.isGroupMember(getGroupName())) {
				if (permissions.toString().substring(4, 5).equals("w")) {
					log.debug("PERMISSIONS: " + path + " - " + " write allowed for group");
					return true;
				}
			} else {
				if (permissions.toString().substring(7, 8).equals("w")) {
					log.debug("PERMISSIONS: " + path + " - " + " write allowed for others");
					return true;
				}
			}
			log.debug("PERMISSIONS: " + path + " - " + " write denied");
			return false;
		} catch (IOException e) {
			return getParent().isWritable();
		}
	}

	/**
	 * Checks if the user has a delete permission on the object
	 *
	 * @return true if the user has delete permission on the object
	 */
	public boolean isRemovable() {
		return isWritable();
	}

	/**
	 * Get owner of the object
	 *
	 * @return owner of the object
	 */
	public String getOwnerName() {
		try {
			DistributedFileSystem dfs = HdfsOverFtpSystem.getDfs();
			FileStatus fs = dfs.getFileStatus(path);
			return fs.getOwner();
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Get group of the object
	 *
	 * @return group of the object
	 */
	public String getGroupName() {
		try {
			DistributedFileSystem dfs = HdfsOverFtpSystem.getDfs();
			FileStatus fs = dfs.getFileStatus(path);
			return fs.getGroup();
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Get link count
	 *
	 * @return 3 is for a directory and 1 is for a file
	 */
	public int getLinkCount() {
		return isDirectory() ? 3 : 1;
	}

	/**
	 * Get last modification date
	 *
	 * @return last modification date as a long
	 */
	public long getLastModified() {
		try {
			DistributedFileSystem dfs = HdfsOverFtpSystem.getDfs();
			FileStatus fs = dfs.getFileStatus(path);
			return fs.getModificationTime();
		} catch (IOException e) {
			e.printStackTrace();
			return 0;
		}
	}

	/**
	 * Set last modification date - not implemented
	 *
	 * @return boolean of success
	 */
	public boolean setLastModified(long time) {
		// Not implemeted
		return true;
	}

	/**
	 * Get a size of the object
	 *
	 * @return size of the object in bytes
	 */
	public long getSize() {
		try {
			DistributedFileSystem dfs = HdfsOverFtpSystem.getDfs();
			FileStatus fs = dfs.getFileStatus(path);
			//log.debug("getSize(): " + path + " : " + fs.getLen());
			return fs.getLen();
		} catch (IOException e) {
			e.printStackTrace();
			return 0;
		}
	}

	/**
	 * Create a new dir from the object
	 *
	 * @return true if dir is created
	 */
	public boolean mkdir() {

		if (!isWritable()) {
			log.debug("No write permission : " + path);
			return false;
		}

		try {
			DistributedFileSystem dfs = HdfsOverFtpSystem.getDfs();
			dfs.mkdirs(path);

			if (HdfsOverFtpSystem.getSetOwner() == true) {
				dfs.setOwner(path, user.getName(), user.getMainGroup());
			}
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}

	/**
	 * Delete object from the HDFS filesystem
	 *
	 * @return true if the object is deleted
	 */
	public boolean delete() {
		try {
			DistributedFileSystem dfs = HdfsOverFtpSystem.getDfs();
			dfs.delete(path, true);
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}

	/**
	 * Move the object to another location
	 *
	 * @param fileObject location to move the object
	 * @return true if the object is moved successfully
	 */
	public boolean move(FtpFile fileObject) {
		try {
			DistributedFileSystem dfs = HdfsOverFtpSystem.getDfs();
			dfs.rename(path, ((HdfsFileObject)fileObject).getPhysicalPath());
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}


    /**
     * Get the physical path object.
     */
    public Path getPhysicalPath() {
        return path;
    }

    /**
     * List files. If not a directory or does not exist, null will be returned.
     */
    public List<FtpFile> listFiles() {

        // is a directory
        if (!isDirectory()) {
            return null;
        }

		if (!isReadable()) {
			log.debug("No read permission : " + path);
			return null;
		}

        // directory - return all the files
        try {
			DistributedFileSystem dfs = HdfsOverFtpSystem.getDfs();
			FileStatus fileStats[] = dfs.listStatus(path);
	        if (fileStats == null) {
	            return null;
	        }

	        // make sure the files are returned in order
	        Arrays.sort(fileStats, new Comparator<FileStatus>() {
	            public int compare(FileStatus f1, FileStatus f2) {
	                return f1.getPath().getName().compareTo(f2.getPath().getName());
	            }
	        });

	        // get the virtual name of the base directory
	        String virtualFileStr = getAbsolutePath();
	        if (virtualFileStr.charAt(virtualFileStr.length() - 1) != '/') {
	            virtualFileStr += '/';
	        }

	        // now return all the files under the directory
	        FtpFile[] virtualFiles = new HdfsFileObject[fileStats.length];
	        for (int i = 0; i < fileStats.length; ++i) {
	            Path fileObj = fileStats[i].getPath();
	            String fileName = virtualFileStr + fileObj.toString();
	            virtualFiles[i] = new HdfsFileObject(fileName, fileObj, user);
	        }

	        return Collections.unmodifiableList(Arrays.asList(virtualFiles));
	    }
		catch (IOException e) {
			e.printStackTrace();
			return null;
		}
    }

	/**
	 * Creates output stream to write to the object
	 *
	 * @param l is not used here
	 * @return OutputStream
	 * @throws IOException
	 */
	public OutputStream createOutputStream(long l) throws IOException {

		// permission check
		if (!isWritable()) {
			throw new IOException("No write permission : " + path);
		}

		try {
			DistributedFileSystem dfs = HdfsOverFtpSystem.getDfs();
			FSDataOutputStream out = dfs.create(path);
			if (HdfsOverFtpSystem.getSetOwner() == true) {
				try {
					dfs.setOwner(path, user.getName(), user.getMainGroup());
				}
				catch (Exception e) {
					log.error("Cannot set permissions on file", e);
				}
			}
			return out;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Creates input stream to read from the object
	 *
	 * @param l is not used here
	 * @return OutputStream
	 * @throws IOException
	 */
	public InputStream createInputStream(long l) throws IOException {
		// permission check
		if (!isReadable()) {
			throw new IOException("No read permission : " + path);
		}
		try {
			DistributedFileSystem dfs = HdfsOverFtpSystem.getDfs();
			FSDataInputStream in = dfs.open(path);
			return in;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

    /**
     * Normalize separate character. Separate character should be '/' always.
     */
    public final static String normalizeSeparateChar(final String pathName) {
        String normalizedPathName = pathName.replace(Path.SEPARATOR_CHAR, '/');
        normalizedPathName = normalizedPathName.replace('\\', '/');
        return normalizedPathName;
    }

    /**
     * Get the physical canonical file name. It works like
     * File.getCanonicalPath().
     *
     * @param rootDir
     *            The root directory.
     * @param currDir
     *            The current directory. It will always be with respect to the
     *            root directory.
     * @param fileName
     *            The input file name.
     * @return The return string will always begin with the root directory. It
     *         will never be null.
     */
    public final static String getPhysicalName(final String rootDir,
            final String currDir, final String fileName) {
        return getPhysicalName(rootDir, currDir, fileName, false);
    }

    public final static String getPhysicalName(final String rootDir,
            final String currDir, final String fileName,
            final boolean caseInsensitive) {

        // get the starting directory
        String normalizedRootDir = normalizeSeparateChar(rootDir);
        if (normalizedRootDir.charAt(normalizedRootDir.length() - 1) != '/') {
            normalizedRootDir += '/';
        }

        String normalizedFileName = normalizeSeparateChar(fileName);
        String resArg;
        String normalizedCurrDir = currDir;
        if (normalizedFileName.charAt(0) != '/') {
            if (normalizedCurrDir == null) {
                normalizedCurrDir = "/";
            }
            if (normalizedCurrDir.length() == 0) {
                normalizedCurrDir = "/";
            }

            normalizedCurrDir = normalizeSeparateChar(normalizedCurrDir);

            if (normalizedCurrDir.charAt(0) != '/') {
                normalizedCurrDir = '/' + normalizedCurrDir;
            }
            if (normalizedCurrDir.charAt(normalizedCurrDir.length() - 1) != '/') {
                normalizedCurrDir += '/';
            }

            resArg = normalizedRootDir + normalizedCurrDir.substring(1);
        } else {
            resArg = normalizedRootDir;
        }

        // strip last '/'
        if (resArg.charAt(resArg.length() - 1) == '/') {
            resArg = resArg.substring(0, resArg.length() - 1);
        }

        // replace ., ~ and ..
        // in this loop resArg will never end with '/'
        StringTokenizer st = new StringTokenizer(normalizedFileName, "/");
        while (st.hasMoreTokens()) {
            String tok = st.nextToken();

            // . => current directory
            if (tok.equals(".")) {
                continue;
            }

            // .. => parent directory (if not root)
            if (tok.equals("..")) {
                if (resArg.startsWith(normalizedRootDir)) {
                    int slashIndex = resArg.lastIndexOf('/');
                    if (slashIndex != -1) {
                        resArg = resArg.substring(0, slashIndex);
                    }
                }
                continue;
            }

            // ~ => home directory (in this case the root directory)
            if (tok.equals("~")) {
                resArg = normalizedRootDir.substring(0, normalizedRootDir
                        .length() - 1);
                continue;
            }

            // if (caseInsensitive) {
            //     Path[] matches = new Path(resArg)
            //             .listFiles(new NameEqualsFileFilter(tok, true));

            //     if (matches != null && matches.length > 0) {
            //         tok = matches[0].getName();
            //     }
            // }

            resArg = resArg + '/' + tok;
        }

        // add last slash if necessary
        if ((resArg.length()) + 1 == normalizedRootDir.length()) {
            resArg += '/';
        }

        // final check
        if (!resArg.regionMatches(0, normalizedRootDir, 0, normalizedRootDir
                .length())) {
            resArg = normalizedRootDir;
        }

        return resArg;
    }

    /**
     * Implements equals by comparing getCanonicalPath() for the underlying file instabnce.
     * Ignores the fileName and User fields
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof HdfsFileObject) {
            String thisCanonicalPath;
            String otherCanonicalPath;
            try {
                thisCanonicalPath = this.path.toString();
                otherCanonicalPath = ((HdfsFileObject) obj).path
                        .toString();
            } catch (Exception e) {
                throw new RuntimeException("Failed to get the path", e);
            }

            return thisCanonicalPath.equals(otherCanonicalPath);
        }
        return false;
    }


	@Override
	public int hashCode() {
		try {
			return path.toString().hashCode();
		} catch (Exception e) {
			return 0;
		}
	}
}
