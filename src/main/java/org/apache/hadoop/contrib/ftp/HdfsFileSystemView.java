package org.apache.hadoop.contrib.ftp;

import org.apache.ftpserver.ftplet.FtpFile;
import org.apache.ftpserver.ftplet.FileSystemView;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.User;

import org.apache.log4j.Logger;

/**
 * Implemented FileSystemView to use HdfsFileObject
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

	/**
	 * Constructor - set the user object.
	 */
	protected HdfsFileSystemView(User user) throws FtpException {
		this(user, true);
	}

	/**
	 * Constructor - set the user object.
	 */
	protected HdfsFileSystemView(User user, boolean caseInsensitive)
			throws FtpException {
		if (user == null) {
			throw new IllegalArgumentException("user can not be null");
		}
		if (user.getHomeDirectory() == null) {
			throw new IllegalArgumentException(
					"User home directory can not be null");
		}

		this.caseInsensitive = caseInsensitive;

		// add last '/' if necessary
		String rootDir = user.getHomeDirectory();
		//  rootDir = NativeFileObject.normalizeSeparateChar(rootDir);
		if (!rootDir.endsWith("/")) {
			rootDir += '/';
		}
		this.rootDir = rootDir;
		this.user = user;

		if (this.changeDirectory(this.rootDir) == false) {
			log.warn("Couild not change to user home directory - " + this.rootDir);
		}
	}

	/**
	 * Get the user home directory. It would be the file system root for the
	 * user.
	 */
	public FtpFile getHomeDirectory() {
		return new HdfsFileObject(this.rootDir, user);
	}

	/**
	 * Get the current directory.
	 */
	public FtpFile getCurrentDirectory() {
		return new HdfsFileObject(currDir, user);
	}

	/**
	 * Get file object.
	 */
	public FtpFile getFile(String file) {
		String path;
		if (file.startsWith("/")) {
			path = file;
		} else if (currDir.length() > 1) {
			path = currDir + "/" + file;
		} else {
			path = "/" + file;
		}
		return new HdfsFileObject(path, user);
	}

	/**
	 * Change directory.
	 */
	public boolean changeDirectory(String dir) {
		String path;
		if (dir.startsWith("/")) {
			path = dir;
		} else if (currDir.length() > 1) {
			path = currDir + "/" + dir;
		} else {
			path = "/" + dir;
		}
		HdfsFileObject file = new HdfsFileObject(path, user);
		if (file.isDirectory() && file.isReadable()) {
			currDir = path;
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Change working directory
	 */
	public FtpFile getWorkingDirectory() {
		return getCurrentDirectory();
	}

	/**
	 * Change working directory
	 */
	public boolean changeWorkingDirectory(String dir) {
		return changeDirectory(dir);
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
