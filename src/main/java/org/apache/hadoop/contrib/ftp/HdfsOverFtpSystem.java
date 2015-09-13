package org.apache.hadoop.contrib.ftp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Class to store DFS connection
 */
public class HdfsOverFtpSystem {

	private static DistributedFileSystem dfs = null;

	public static String HDFS_URI = "";

	private static String hdfsuser = "error";
	private static String hdfsgroup = "error";
	private static boolean setowner = false;

	private final static Logger log = LoggerFactory.getLogger(HdfsOverFtpSystem.class);


	private static void hdfsInit() throws IOException {
		dfs = new DistributedFileSystem();
		Configuration conf = new Configuration();
		conf.set("hadoop.job.ugi", hdfsuser + "," + hdfsgroup);
		try {
			log.debug("Connecting to HDFS " + HDFS_URI + " as " + hdfsuser + ", " + hdfsgroup + "");
			dfs.initialize(new URI(HDFS_URI), conf);

			if (hdfsuser.equals(System.getProperty("user.name")) == false) {
				// Running user is different that HDFS user - use setOwner
				log.debug("Use setOwner");
				setowner = true;
			}
		} catch (URISyntaxException e) {
			log.error("DFS Initialization error", e);
		}
	}

	public static void setHDFS_URI(String HDFS_URI) {
		HdfsOverFtpSystem.HDFS_URI = HDFS_URI;
	}

	/**
	 * Get dfs
	 *
	 * @return dfs
	 * @throws IOException
	 */
	public static DistributedFileSystem getDfs() throws IOException {
		if (dfs == null) {
			hdfsInit();
		}
		return dfs;
	}

	/**
	 * Set hdfsuser and we connect to DFS as a hdfsuser
	 *
	 * @param hdfsuser
	 */
	public static void setHDFSUser(String hdfsuser) {
		HdfsOverFtpSystem.hdfsuser = hdfsuser;
		System.setProperty("HADOOP_USER_NAME", hdfsuser);
	}

	/**
	 * Set hdfsgroup and we connect to DFS as a hdfsgroup
	 *
	 * @param hdfsgroup
	 */
	public static void setHDFSGroup(String hdfsgroup) {
		HdfsOverFtpSystem.hdfsgroup = hdfsgroup;
	}

	/**
	 * Get setowner
	 *
	 * @return setowner
	 */
	public static boolean getSetOwner() {
		return setowner;
	}
}
