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

import org.apache.activemq.ActiveMQConnectionFactory;

import org.apache.ftpserver.ftplet.DefaultFtplet;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.FtpReply;
import org.apache.ftpserver.ftplet.FtpRequest;
import org.apache.ftpserver.ftplet.FtpSession;
import org.apache.ftpserver.ftplet.Ftplet;
import org.apache.ftpserver.ftplet.FtpletContext;
import org.apache.ftpserver.ftplet.FtpletResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Queue management
 */
public class HdfsFtpletQueue extends DefaultFtplet {

    private final Logger LOG = LoggerFactory.getLogger(HdfsFtpletQueue.class);

    private QueueManagerConfig queueMgrConfig = null;
    private QueueManager queueMgr = null;

    // If there is an error in adding to the queue it is a warning
    // A separate piece with the queue broker needs to make sure the files in HDFS
    // get added to the queue on startup if not there

    /**
     * Create the Queue Ftplet
     *
     * @throws Exception
     */
    public HdfsFtpletQueue(QueueManagerConfig queueMgrConfig) {
        this.queueMgrConfig = queueMgrConfig;
        queueMgr = new QueueManager(queueMgrConfig);
        queueMgr.connectToQueue();
    }

    @Override
    public FtpletResult onUploadEnd(FtpSession session, FtpRequest request) throws FtpException, IOException {
        String currDir = session.getFileSystemView().getWorkingDirectory().getAbsolutePath();
        String fileName = request.getArgument();
        String fullFilePath = "";
        String clientAddr = session.getClientAddress().getAddress().getHostAddress();

        // Go back through proxy? ick
        // Compoent restarts - gets new port - how to know?

        if (currDir == "/") {
            fullFilePath = "" + currDir + "" + fileName;
        }
        else {
            fullFilePath = "" + currDir + "/" + fileName;
        }

        try {
            queueMgr.sendMessage(clientAddr + "|" + fullFilePath);
        }
        catch (Exception e) {
            LOG.warn("Exception", e);
        }

        return FtpletResult.DEFAULT;
    }

    @Override
    public void destroy() {
        queueMgr.closeQueue();
    }
}
