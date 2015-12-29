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

/**
 * Queue manager
 */
public class QueueManagerConfig {
    private String appName = "";
    private String compExportName = "";
    private String discoveryMethod = "";
    private String queueHost = "";
    private int queuePort = 0;
    private String queueName = "";
    private String queueUser = "";
    private String queuePassword = "";

    /**
     * Create the Queue Manager config
     */
    public QueueManagerConfig(String appName, String compExportName, String discoveryMethod, String queueHost, int queuePort, String queueName, String queueUser, String queuePassword) {
        this.appName = appName;
        this.compExportName = compExportName;
        this.discoveryMethod = discoveryMethod;
        this.queueHost = queueHost;
        this.queuePort = queuePort;
        this.queueName = queueName;
        this.queueUser = queueUser;
        this.queuePassword = queuePassword;
    }

    /**
     * Application name - Slider cluster name
     *
     * @return application name
     */
    public String getAppName() {
        return appName;
    }

    /**
     * Component export name - in Slider componentinstancedata
     *
     * @return component name
     */
    public String getCompExportName() {
        return compExportName;
    }

    /**
     * Discovery method - file or slider
     *
     * @return discovery method
     */
    public String getDiscoveryMethod() {
        return discoveryMethod;
    }

    /**
     * Set queue host
     */
    public void setQueueHost(String queueHost) {
        this.queueHost = queueHost;
    }

    /**
     * Queue host
     *
     * @return queue host
     */
    public String getQueueHost() {
        return queueHost;
    }

    /**
     * Set queue port
     */
    public void setQueuePort(int queuePort) {
        this.queuePort = queuePort;
    }

    /**
     * Queue port
     *
     * @return queue port
     */
    public int getQueuePort() {
        return queuePort;
    }

    /**
     * Queue name
     *
     * @return queue name
     */
    public String getQueueName() {
        return queueName;
    }

    /**
     * Queue username
     *
     * @return queue username
     */
    public String getQueueUser() {
        return queueUser;
    }

    /**
     * Queue password
     *
     * @return queue password
     */
    public String getQueuePassword() {
        return queuePassword;
    }
}
