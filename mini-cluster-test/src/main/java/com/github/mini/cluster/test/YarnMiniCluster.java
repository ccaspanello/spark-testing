package com.github.mini.cluster.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class YarnMiniCluster implements MiniCluster {

  private static final Logger LOG = LoggerFactory.getLogger( YarnMiniCluster.class );

  private static final String ERROR_MISS_NM_NODES = "ERROR: Missing required config: Number of Node Managers";
  private static final String ERROR_MISS_NM_LOCAL_DIRS = "ERROR: Missing required config: Number of Local Dirs";
  private static final String ERROR_MISS_NM_LOG_DIRS = "ERROR: Missing required config: Number of Log Dirs";
  private static final String ERROR_MISS_RM_ADDRESS = "ERROR: Missing required config: Yarn Resource Manager Address";
  private static final String ERROR_MISS_RM_HOSTNAME = "ERROR: Missing required config: Yarn Resource Manager Hostname";
  private static final String ERROR_MISS_SCHEDULER_ADDRESS = "ERROR: Missing required config: Yarn Resource Manager Scheduler Address";
  private static final String ERROR_MISS_TRACKER_ADDRESS = "ERROR: Missing required config: Yarn Resource Manager Resource Tracker Address";
  private static final String ERROR_MISS_WEBAPP_ADDRESS = "ERROR: Missing required config: Yarn Resource Manager Webapp Address";
  private static final String ERROR_MISS_CONFIG = "ERROR: Missing required config: Configuration";
  private static final String START_YARN = "YARN: Starting MiniYarnCluster";
  private static final String STOP_YARN = "YARN: Stopping MiniYarnCluster";

  private static final String TRUE = "true";
  private static final String CLASSES_DIR = "./target/classes";
  private static final String TARGET_DIR = "./target/";

  private String testName = getClass().getName();
  private Integer numResourceManagers = 1;
  private Boolean enableHa = false;
  private Integer numNodeManagers;
  private Integer numLocalDirs;
  private Integer numLogDirs;
  private String resourceManagerAddress;
  private String resourceManagerHostname;
  private String resourceManagerSchedulerAddress;
  private String resourceManagerResourceTrackerAddress;
  private String resourceManagerWebappAddress;
  private Configuration configuration;

  private MiniYARNCluster miniYARNCluster;

  public Configuration getConfig() {
    return configuration;
  }

  public YarnMiniCluster( ClusterProperties clusterProperties, Configuration configuration ) {
    this.numNodeManagers = clusterProperties.getYarnNumNodeManagers();
    this.numLocalDirs = clusterProperties.getYarnNumLocalDirs();
    this.numLogDirs = clusterProperties.getYarnNumLogDirs();
    this.resourceManagerAddress = clusterProperties.getYarnResourceManagerAddress();
    this.resourceManagerHostname = clusterProperties.getYarnResourceManagerHostName();
    this.resourceManagerSchedulerAddress = clusterProperties.getYarnResourceManagerSchedulerAddress();
    this.resourceManagerResourceTrackerAddress = clusterProperties.getYarnResourceManagerResourceTrackerAddress();
    this.resourceManagerWebappAddress = clusterProperties.getYarnResourceManagerWebappAddress();
    this.configuration = configuration;
  }
  @Override
  public void start() throws Exception {
    LOG.info( START_YARN ) ;

    configuration.set( YarnConfiguration.RM_ADDRESS, resourceManagerAddress );
    configuration.set( YarnConfiguration.RM_HOSTNAME, resourceManagerHostname );
    configuration.set( YarnConfiguration.RM_SCHEDULER_ADDRESS, resourceManagerSchedulerAddress );
    configuration.set( YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS, resourceManagerResourceTrackerAddress );
    configuration.set( YarnConfiguration.RM_WEBAPP_ADDRESS, resourceManagerWebappAddress );
    configuration.set( YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, TRUE );

    miniYARNCluster = new MiniYARNCluster( testName, numResourceManagers, numNodeManagers,
      numLocalDirs, numLogDirs, enableHa );

    miniYARNCluster.serviceInit( configuration );
    miniYARNCluster.init( configuration );
    miniYARNCluster.start();

  }

  @Override
  public void stop( boolean cleanUp ) throws Exception {
    LOG.info( STOP_YARN );
    miniYARNCluster.stop();
    if ( cleanUp ) {
      cleanUp();
    }
  }


  @Override
  public void cleanUp() throws Exception {
    // Depending on if we are running in the module or the parent
    // project, the target folder will be in a different location.
    // We don't want to nuke the entire target directory, unless only
    // the mini cluster is using it.
    // A reasonable check to keep things clean is to check for the existence
    // of ./target/classes and only delete the mini cluster temporary dir if true.
    // Delete the entire ./target if false
    if ( new File( CLASSES_DIR ).exists() ) {
      FileOperation.deleteFolder( TARGET_DIR + testName );
    } else {
      FileOperation.deleteFolder( TARGET_DIR );
    }
  }
}