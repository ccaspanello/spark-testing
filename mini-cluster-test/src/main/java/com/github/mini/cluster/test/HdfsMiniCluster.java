package com.github.mini.cluster.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class HdfsMiniCluster implements MiniCluster {

  private static final Logger LOG = LoggerFactory.getLogger( HdfsMiniCluster.class );

  private static final String START_DFS = "HDFS: Starting MiniDfsCluster";
  private static final String STOP_DFS = "HDFS: Stopping MiniDfsCluster";

  private static final String STAR = "*";
  private static final String DFS_PERMISSIONS = "dfs.permissions";
  private static final String TST_BUILD_DATE = "test.build.data";
  private static final String HDP_PRXY_HOSTS = "hadoop.proxyuser.%s.hosts";
  private static final String HDP_PRXY_GROUPS = "hadoop.proxyuser.%s.groups";
  private static final String PRPTY_USERNAME = "user.name";

  private MiniDFSCluster miniDFSCluster;

  private Integer hdfsNamenodePort;
  private Integer hdfsNamenodeHttpPort;
  private String hdfsTempDir;
  private Integer hdfsNumDatanodes;
  private Boolean hdfsEnablePermissions;
  private Boolean hdfsFormat;
  private Boolean hdfsEnableRunningUserAsProxyUser;
  private Configuration hdfsConfig;

  public HdfsMiniCluster( ClusterProperties clusterProperties, Configuration hdfsConfig) {
    this.hdfsNamenodePort = clusterProperties.getHdfsNameNodePort();
    this.hdfsNamenodeHttpPort = clusterProperties.getHdfsNameNodeHttpPort();
    this.hdfsTempDir = clusterProperties.getHdfsTempDir();
    this.hdfsNumDatanodes = clusterProperties.getHdfsNumDataNodes();
    this.hdfsEnablePermissions = clusterProperties.getHdfsEnablePermissions();
    this.hdfsFormat = clusterProperties.getHdfsFormat();
    this.hdfsEnableRunningUserAsProxyUser = clusterProperties.getHdfsEnableRunningUserAsProxyUser();
    this.hdfsConfig = hdfsConfig;
  }

  @Override
  public void start() throws Exception {
    LOG.info( START_DFS );

    if ( null != hdfsEnableRunningUserAsProxyUser && hdfsEnableRunningUserAsProxyUser ) {
      hdfsConfig.set( String.format( HDP_PRXY_HOSTS, System.getProperty( PRPTY_USERNAME ) ), STAR );
      hdfsConfig.set( String.format( HDP_PRXY_GROUPS, System.getProperty( PRPTY_USERNAME ) ), STAR );
    }

    hdfsConfig.setBoolean( DFS_PERMISSIONS, hdfsEnablePermissions );
    System.setProperty( TST_BUILD_DATE, hdfsTempDir );

    miniDFSCluster = new MiniDFSCluster.Builder( hdfsConfig )
      .nameNodePort( hdfsNamenodePort )
      .nameNodeHttpPort( hdfsNamenodeHttpPort == null ? 0 : hdfsNamenodeHttpPort )
      .numDataNodes( hdfsNumDatanodes )
      .format( hdfsFormat )
      .racks( null )
      .build();

  }

  @Override
  public void stop( boolean cleanUp ) throws Exception {
    LOG.info( STOP_DFS );
    miniDFSCluster.shutdown();
    if ( cleanUp ) {
      cleanUp();
    }
  }

  @Override
  public void cleanUp() throws Exception {
    FileOperation.deleteFolder( hdfsTempDir );
  }

  public FileSystem getHdfsFileSystemHandle() throws Exception {
    return miniDFSCluster.getFileSystem();
  }
}

