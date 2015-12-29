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
package org.apache.hadoop.contrib.ftp.tests.utils;

import java.net.ServerSocket;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;

import org.apache.activemq.ActiveMQConnectionFactory;

import org.apache.commons.lang3.text.StrSubstitutor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsOverFtpServerTestUtils {

	private static final Logger log = LoggerFactory.getLogger(HdfsOverFtpServerTestUtils.class);

	private String TCLUSTER = "testcluster";
	private Configuration conf;
	private File hdfsLocalFiles = null;
	private MiniDFSCluster dFSCluster = null;
	private FileSystem fs;
	private String hdfsUri = "";
	private ConnectionFactory aamqConnFactory = null;
	private Connection aamqConn = null;

	public void subsConfigFile(String inFile, String outFile, Map<String, String> valuesMap) throws Exception {
		StringBuilder fileData = new StringBuilder();
		String line = "";
		BufferedReader inputRdr = null;
		String outputData;
		BufferedWriter outputWrt = null;

		try {
			inputRdr = new BufferedReader(new InputStreamReader(new FileInputStream(inFile), "UTF8"));

	        while ((line = inputRdr.readLine()) != null) {
	            fileData.append(line + "\n");
	        }

			StrSubstitutor strSub = new StrSubstitutor(valuesMap);
			boolean strReplaced = strSub.replaceIn(fileData);

			outputWrt = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(outFile)), "UTF-8"));
			outputWrt.write(fileData.toString());
			outputWrt.flush();
	    }
	    catch (Exception e) {
	    	throw e;
	    }
	    finally {
	    	if (inputRdr!= null) {
		    	inputRdr.close();
		    }
	    	if (outputWrt!= null) {
		    	outputWrt.close();
		    }
	    }
	}

	public int getFreeTCPPort() throws Exception {
		ServerSocket srvSocket = null;
		int port = 0;

		try {
		//for (int i = 10000; i <= 49000; i++) {
			srvSocket = new ServerSocket(0);
			srvSocket.setReuseAddress(true);
			return srvSocket.getLocalPort();
		//}
		}
		catch (Exception e) {
			throw e;
		}
	    finally {
	    	if (srvSocket!= null) {
		    	srvSocket.close();
		    }
	    }
	}

	public String createMiniDFSCluster(String appRoot) throws Exception {
		// Setup local dir to hold HDFS files
		log.info("Create MiniDFS cluster");
		hdfsLocalFiles = new File(appRoot, "hdfs");
		hdfsLocalFiles.mkdirs();
		log.info("Local filesystem for HDFS " + hdfsLocalFiles.toString());

		conf = new Configuration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, hdfsLocalFiles.toString());

		dFSCluster = new MiniDFSCluster.Builder(conf).build();
        fs = FileSystem.get(conf);
        hdfsUri = "hdfs://127.0.0.1:" + dFSCluster.getNameNodePort() + "/";
        conf.set("fs.defaultFS", hdfsUri);

        return hdfsUri;
	}

	public void createHDFSPath(String hdfsPath) throws Exception {
		fs.mkdirs(new Path(hdfsPath));
	}

	public void destroyMiniDFSCluster() throws Exception {
        // Destroy HDFS minicluster
		log.info("Destroy MiniDFS cluster");
        dFSCluster.shutdown();
        hdfsLocalFiles.delete();
	}

	public void createActiveMQServer(String appRoot, Map<String, String> subsConfigFileVals) throws Exception {
		subsConfigFile(appRoot + "activemq.template", appRoot + "activemq.xml", subsConfigFileVals);
		aamqConnFactory = new ActiveMQConnectionFactory("vm://localhost?brokerConfig=xbean:" + appRoot + "activemq.xml");
		aamqConn = aamqConnFactory.createConnection("system", "manager");
		aamqConn.start();
	}

	public void destroyActiveMQServer() throws Exception {
		aamqConn.stop();
		aamqConn.close();
	}
}