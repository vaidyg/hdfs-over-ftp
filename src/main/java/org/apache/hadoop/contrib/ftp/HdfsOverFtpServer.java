package org.apache.hadoop.contrib.ftp;

import org.apache.ftpserver.impl.DefaultDataConnectionConfiguration;
import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.DataConnectionConfigurationFactory;
import org.apache.ftpserver.DataConnectionConfiguration;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.log4j.Logger;

import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.ParseException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Properties;

/**
 * Start-up class of FTP server
 */
public class HdfsOverFtpServer {

	private static Logger log = Logger.getLogger(HdfsOverFtpServer.class);

	private static int port = 0;
	private static int sslPort = 0;
	private static String passivePorts = null;
	private static String sslPassivePorts = null;
	private static String hdfsUri = null;
	private static String approot = "";

	public static void main(String[] args) throws Exception {
		CommandLineParser parser = new DefaultParser();
		Options options = new Options();
		options.addOption("h", "help", false, "WYSIWYG");
		options.addOption(OptionBuilder.withLongOpt("approot")
			.withDescription("use FILE-path with trailing slash")
			.hasArg()
			.withArgName("FILE")
			.create());

		try {
			CommandLine line = parser.parse(options, args);
			if (line.hasOption("approot")) {
				approot = line.getOptionValue("approot");

				if (approot.endsWith("/") == false) {
					approot += "/";
				}
			}
			else if (line.hasOption("help")) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("HdfsOverFtpServer", options);
				System.exit(1);
			}
		}
		catch(Exception e) {
			log.fatal("Unexpected command line parse exception:" + e.getMessage());
			System.exit(1);
		}

		loadConfig();

		if (port != 0) {
			startServer();
		}

		if (sslPort != 0) {
			startSSLServer();
		}
	}

	/**
	 * Load configuration
	 *
	 * @throws IOException
	 */
	private static void loadConfig() throws IOException {
		Properties props = new Properties();
		//props.load(new FileInputStream(loadResource(approot + "hdfs-over-ftp.properties")));
		File propFile = new File(approot + "hdfs-over-ftp.properties");
		if (propFile.exists() == false) {
			throw new RuntimeException("Resource not found: " + approot + "hdfs-over-ftp.properties");
		}

		props.load(new FileInputStream(propFile));

		try {
			port = Integer.parseInt(props.getProperty("port"));
			log.info("port is set. ftp server will be started");
		} catch (Exception e) {
			log.info("port is not set. so ftp server will not be started");
		}

		try {
			sslPort = Integer.parseInt(props.getProperty("ssl-port"));
			log.info("ssl-port is set. ssl server will be started");
		} catch (Exception e) {
			log.info("ssl-port is not set. so ssl server will not be started");
		}

		if (port != 0) {
			passivePorts = props.getProperty("data-ports");
			if (passivePorts == null) {
				log.fatal("data-ports is not set");
				System.exit(1);
			}
		}

		if (sslPort != 0) {
			sslPassivePorts = props.getProperty("ssl-data-ports");
			if (sslPassivePorts == null) {
				log.fatal("ssl-data-ports is not set");
				System.exit(1);
			}
		}

		hdfsUri = props.getProperty("hdfs-uri");
		if (hdfsUri == null) {
			log.fatal("hdfs-uri is not set");
			System.exit(1);
		}

		String hdfsuser = props.getProperty("hdfsuser");
		if (hdfsuser == null) {
			hdfsuser = System.getProperty("user.name");
			log.info("Setting to running user - " + hdfsuser);
		}
		HdfsOverFtpSystem.setHDFSUser(hdfsuser);

		String hdfsgroup = props.getProperty("hdfsgroup");
		if (hdfsgroup == null) {
			log.fatal("hdfsgroup is not set");
			System.exit(1);
		}
		HdfsOverFtpSystem.setHDFSGroup(hdfsgroup);
	}

	/**
	 * Starts FTP server
	 *
	 * @throws Exception
	 */
	public static void startServer() throws Exception {
		log.info("Starting Hdfs-Over-Ftp server. port: " + port + " data-ports: " + passivePorts + " hdfs-uri: " + hdfsUri);

		HdfsOverFtpSystem.setHDFS_URI(hdfsUri);

		FtpServerFactory serverFactory = new FtpServerFactory();
		ListenerFactory listenFactory = new ListenerFactory();

		DataConnectionConfigurationFactory dataConFactory = new DataConnectionConfigurationFactory();
		DataConnectionConfiguration dataCon = dataConFactory.createDataConnectionConfiguration();
		dataConFactory.setPassivePorts(passivePorts);

		listenFactory.setDataConnectionConfiguration(dataCon);
		listenFactory.setPort(port);
		serverFactory.addListener("default", listenFactory.createListener());

		File userFile = new File(approot + "users.properties");
		if (userFile.exists() == false) {
			throw new RuntimeException("Resource not found: " + approot + "users.properties");
		}

		HdfsUserManager userManager = new HdfsUserManager();
		userManager.setFile(userFile);
		userManager.configure();
		serverFactory.setUserManager(userManager);
		serverFactory.setFileSystem(new HdfsFileSystemManager());

		FtpServer server = serverFactory.createServer();
		server.start();
	}

	/**
	 * Starts SSL FTP server
	 *
	 * @throws Exception
	 */
	public static void startSSLServer() throws Exception {
		log.info("Starting Hdfs-Over-Ftp SSL server. ssl-port: " + sslPort + " ssl-data-ports: " + sslPassivePorts + " hdfs-uri: " + hdfsUri);

		HdfsOverFtpSystem.setHDFS_URI(hdfsUri);

		FtpServerFactory serverFactory = new FtpServerFactory();
		ListenerFactory listenFactory = new ListenerFactory();

		DataConnectionConfigurationFactory dataConFactory = new DataConnectionConfigurationFactory();
		DataConnectionConfiguration dataCon = dataConFactory.createDataConnectionConfiguration();
		dataConFactory.setPassivePorts(sslPassivePorts);

		listenFactory.setDataConnectionConfiguration(dataCon);
		listenFactory.setPort(sslPort);

		MySslConfiguration ssl = new MySslConfiguration();
		ssl.setKeystoreFile(new File(approot + "ftp.jks"));
		ssl.setKeystoreType("JKS");
		ssl.setKeyPassword("333333");

		listenFactory.setSslConfiguration(ssl);
		listenFactory.setImplicitSsl(true);
		serverFactory.addListener("default", listenFactory.createListener());

		File userFile = new File(approot + "users.properties");
		if (userFile.exists() == false) {
			throw new RuntimeException("Resource not found: " + approot + "users.properties");
		}

		HdfsUserManager userManager = new HdfsUserManager();
		userManager.setFile(userFile);
		userManager.configure();
		serverFactory.setUserManager(userManager);
		serverFactory.setFileSystem(new HdfsFileSystemManager());

		FtpServer server = serverFactory.createServer();
		server.start();
	}
}
