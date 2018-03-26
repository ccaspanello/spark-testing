package com.github.mini.cluster.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ClusterProperties {

  private static final Logger LOG = LoggerFactory.getLogger( ClusterProperties.class );

  private static final String PROPERTIES_FILE = "cluster.properties";

  //HDFS
  private static final String HDFS_NAMENODE_PORT_KEY = "hdfs.namenode.port";
  private static final String HDFS_NAMENODE_HTTP_PORT_KEY = "hdfs.namenode.http.port";
  private static final String HDFS_TEMP_DIR_KEY = "hdfs.temp.dir";
  private static final String HDFS_NUM_DATANODES_KEY = "hdfs.num.datanodes";
  private static final String HDFS_ENABLE_PERMISSIONS_KEY = "hdfs.enable.permissions";
  private static final String HDFS_FORMAT_KEY = "hdfs.format";
  private static final String HDFS_ENABLE_RUNNING_USER_AS_PROXY_USER = "hdfs.enable.running.user.as.proxy.user";

  // YARN
  private static final String YARN_NUM_NODE_MANAGERS_KEY = "yarn.num.node.managers";
  private static final String YARN_NUM_LOCAL_DIRS_KEY = "yarn.num.local.dirs";
  private static final String YARN_NUM_LOG_DIRS_KEY = "yarn.num.log.dirs";
  private static final String YARN_RM_ADDRESS_KEY = "yarn.resource.manager.address";
  private static final String YARN_RM_HOSTNAME_KEY = "yarn.resource.manager.hostname";
  private static final String YARN_RM_SCHEDULER_ADDRESS_KEY = "yarn.resource.manager.scheduler.address";
  private static final String YARN_RM_WEBAPP_ADDRESS_KEY = "yarn.resource.manager.webapp.address";
  private static final String YARN_RM_RESOURCE_TRACKER_ADDRESS_KEY = "yarn.resource.manager.resource.tracker.address";

  // CLEANUP
  private static final String HDFS_CLEANUP = "hdfs.cleanup";
  private static final String YARN_CLEANUP = "yarn.cleanup";
  private static final String CLEANUP = "cleanup";
  private static final String HDFS_CLEANUP_DEFAULT = "true";
  private static final String YARN_CLEANUP_DEFAULT = "true";
  private static final String CLEANUP_DEFAULT = "true";

  private static final String SPARK_DIST_CLASSPATH = "SPARK_DIST_CLASSPATH";

  private static Properties properties = null;

  public ClusterProperties(){
    try {
      LOG.info( "Reading properties file: {}", PROPERTIES_FILE );
      InputStream inputStream = ClusterProperties.class.getClassLoader().getResourceAsStream( PROPERTIES_FILE );
      properties = new Properties();
      properties.load( inputStream );
    } catch ( IOException e ) {
      throw new RuntimeException( "Unexpected error trying to read properties file.", e );
    }
  }

  public int getHdfsNameNodePort() {
    return Integer.parseInt( properties.getProperty( HDFS_NAMENODE_PORT_KEY ) );
  }

  public int getHdfsNameNodeHttpPort() {
    return Integer.parseInt( properties.getProperty( HDFS_NAMENODE_HTTP_PORT_KEY ));
  }

  public String getHdfsTempDir() {
    return properties.getProperty( HDFS_TEMP_DIR_KEY );
  }

  public int getHdfsNumDataNodes() {
    return Integer.parseInt( properties.getProperty( HDFS_NUM_DATANODES_KEY ));
  }

  public boolean getHdfsEnablePermissions() {
    return Boolean.parseBoolean( properties.getProperty( HDFS_ENABLE_PERMISSIONS_KEY ) );
  }

  public boolean getHdfsFormat() {
    return Boolean.parseBoolean( properties.getProperty( HDFS_FORMAT_KEY ) );
  }

  public boolean getHdfsEnableRunningUserAsProxyUser() {
    return Boolean.parseBoolean( properties.getProperty( HDFS_ENABLE_RUNNING_USER_AS_PROXY_USER ) );
  }

  public int getYarnNumNodeManagers() {
    return Integer.parseInt( properties.getProperty( YARN_NUM_NODE_MANAGERS_KEY ) );
  }

  public int getYarnNumLocalDirs() {
    return Integer.parseInt( properties.getProperty( YARN_NUM_LOCAL_DIRS_KEY ) );
  }

  public int getYarnNumLogDirs() {
    return Integer.parseInt( properties.getProperty( YARN_NUM_LOG_DIRS_KEY ) );
  }

  public String getYarnResourceManagerAddress() {
    return properties.getProperty( YARN_RM_ADDRESS_KEY );
  }

  public String getYarnResourceManagerHostName() {
    return properties.getProperty( YARN_RM_HOSTNAME_KEY );
  }

  public String getYarnResourceManagerWebappAddress() {
    return properties.getProperty( YARN_RM_WEBAPP_ADDRESS_KEY );
  }

  public String getYarnResourceManagerResourceTrackerAddress() {
    return properties.getProperty( YARN_RM_RESOURCE_TRACKER_ADDRESS_KEY );
  }

  public String getYarnResourceManagerSchedulerAddress() {
    return properties.getProperty( YARN_RM_SCHEDULER_ADDRESS_KEY );
  }

  public boolean getHdfsCleanup() {
    return Boolean.parseBoolean( properties.getProperty( HDFS_CLEANUP, HDFS_CLEANUP_DEFAULT ) );
  }

  public boolean getYarnCleanup() {
    return Boolean.parseBoolean( properties.getProperty( YARN_CLEANUP, YARN_CLEANUP_DEFAULT ) );
  }

  public boolean getCleanup() {
    return Boolean.parseBoolean( properties.getProperty( CLEANUP, CLEANUP_DEFAULT ) );
  }
}
