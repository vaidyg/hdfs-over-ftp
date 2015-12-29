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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Queue manager
 */
public class QueueManager implements ExceptionListener {
    private final Logger LOG = LoggerFactory.getLogger(QueueManager.class);

    private int queueRetryTimeout = 15;
    private boolean connected = false;
    private QueueManagerConfig queueMgrConfig = null;
    private Connection conn = null;
    private MessageProducer producer = null;
    private Session sess = null;

    /**
     * Create the Queue Manager
     */
    public QueueManager(QueueManagerConfig queueMgrConfig) {
        this.queueMgrConfig = queueMgrConfig;
    }

    /**
     * Send message to the queue
     */
    public void connectToQueue() {
        while (connected != true) {
            try {
                boolean gotQueuePort = false;
                LOG.info("Attempting connection");
                if (queueMgrConfig.getDiscoveryMethod().equalsIgnoreCase("slider")) {
                    gotQueuePort = getQueueLocationSlider();
                }
                else {
                    // Using config values
                    gotQueuePort = true;
                }

                if (gotQueuePort == false) {
                    LOG.info("Could not get queue host and port");
                }
                else {
                    ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(new URI("tcp://" + queueMgrConfig.getQueueHost() + ":" + queueMgrConfig.getQueuePort() + ""));
                    conn = factory.createConnection(queueMgrConfig.getQueueUser(), queueMgrConfig.getQueuePassword());
                    conn.setExceptionListener(this);
                    conn.start();
                    sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    Queue queue = sess.createQueue(queueMgrConfig.getQueueName());
                    producer = sess.createProducer(queue);
                    producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
                    connected = true;
                    LOG.info("Connected to queue - " + queueMgrConfig.getQueueName() + " @ " + queueMgrConfig.getQueueHost() + ":" + queueMgrConfig.getQueuePort());
                }
            }
            catch (URISyntaxException e) {
                LOG.warn("URI exception", e);
            }
            catch (JMSException e) {
                LOG.warn("JMS exception", e);
            }
            catch (Exception e) {
                LOG.warn("Exception", e);
            }

            if (connected != true) {
                try {
                    Thread.sleep(queueRetryTimeout * 1000);
                }
                catch (InterruptedException e) {
                    LOG.warn("Interrupted exception", e);
                }
            }
        }
    }

    /**
     * Send message to the queue
     */
    public void sendMessage(String message) {
        try {
            producer.send(sess.createTextMessage(message));
            LOG.info("File uploaded - " + message);
        }
        catch (JMSException e) {
            LOG.warn("JMS exception", e);
        }
    }

    /**
     * Handle JMS exceptions
     */
    public void onException(JMSException jmse) {
        String exceptioMsg = jmse.getErrorCode();
        LOG.warn("JMS onexception", exceptioMsg);
        connected = false;
        connectToQueue();
    }

    public void closeQueue() {
        try {
            conn.close();
        }
        catch (JMSException e) {
            LOG.warn("JMS exception", e);
        }
        catch (Exception e) {
            LOG.warn("Exception", e);
        }
    }

    private boolean getQueueLocationSlider() {
        boolean retVal = false;
        Process proc;
        String compURL = "ws/v1/slider/publisher/slider/componentinstancedata";
        String appMaster = "";
        String queueHostPort = "";

        try {
            LOG.debug("Running " + "slider status " + queueMgrConfig.getAppName());
            proc = Runtime.getRuntime().exec("slider status " + queueMgrConfig.getAppName());
            proc.waitFor();
            BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
            String line = "";
            while ((line = reader.readLine())!= null) {
                if (line.indexOf("info.am.web.url") >= 0) {
                    // http:// - find this
                    int startURLPos = line.indexOf("http://");
                    int endURLPos = line.indexOf("\"", startURLPos);
                    appMaster = line.substring(startURLPos, endURLPos);
                    break;
                }
            }
            reader.close();
            proc.destroy();

            if (appMaster != "") {
                LOG.debug("App master URL " + appMaster);
                // Get the component export
                URL appMastURL = new URL(appMaster + compURL);
                HttpURLConnection httpConn = (HttpURLConnection)appMastURL.openConnection();
                httpConn.connect();

                int responseCode = httpConn.getResponseCode();
                reader = new BufferedReader(new InputStreamReader(httpConn.getInputStream()));
                line = "";

                //.openwire_port":"hqchworkert02.corp.fin:50485"
                while ((line = reader.readLine()) != null) {
                    if (line.indexOf("." + queueMgrConfig.getCompExportName()) >= 0) {
                        int startURLPos = line.indexOf("." + queueMgrConfig.getCompExportName());
                        startURLPos = line.indexOf(":", startURLPos);
                        startURLPos = line.indexOf("\"", startURLPos);
                        int endURLPos = line.indexOf("\"", startURLPos + 5);
                        queueHostPort = line.substring(startURLPos + 1, endURLPos);
                        break;
                    }
                }
                reader.close();

                if (queueHostPort != "") {
                    LOG.debug("Queue URL " + queueHostPort);
                    String[] queueHostPortParts = queueHostPort.split(":");
                    queueMgrConfig.setQueueHost(queueHostPortParts[0]);
                    queueMgrConfig.setQueuePort(Integer.parseInt(queueHostPortParts[1]));
                    retVal = true;
                }
            }
        }
        catch (Exception e) {
            LOG.warn("Exception", e);
        }
        finally {
            return retVal;
        }
    }
}
