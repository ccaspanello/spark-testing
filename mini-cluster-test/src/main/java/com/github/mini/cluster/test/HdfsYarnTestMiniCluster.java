package com.github.mini.cluster.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;

public class HdfsYarnTestMiniCluster {

  private static final Logger LOG = LoggerFactory.getLogger( HdfsYarnTestMiniCluster.class );

  private static final String STARTING_HDFS = "=========== Starting HDFS Mini Cluster ===========";
  private static final String HDFS_STARTED = "========= HDFS Mini Cluster Initialized ==========";
  private static final String STARTING_YARN = "=========== Starting Yarn Mini Cluster ===========";
  private static final String YARN_STARTED = "========= Yarn Mini Cluster Initialized ==========";
  private static final String WAITING_RM_START = "Waiting for Resource Manager Start";
  private static final String RM_STARTED = "Resource Manager Initialized at port: {0}";
  private static final String RM_FAILED = "Failed to Initialize Resource Manager";
  private static final String YARN_SITE_PATH = "You can find yarn-site.xml at: {0}";
  private static final String COPY_ASSEMBLY_TO_HDFS = "Copying assembly: {0} to HDFS";
  private static final String ERROR = "Error:";

  private static final int MAX_WAITING_TIME = 30000;
  private static final int PERIOD = 1000;
  private static final String CONF_DIR_PRF = "cluster_conf";
  private static final String YARN_CONF_FILE = "yarn-site.xml";

  private static final String FS_DEFAULT = "fs.defaultFS";
  private static final String YARN_NDMG_DEL_DBG_DELAY = "yarn.nodemanager.delete.debug-delay-sec";

  private ClusterProperties properties;
  private HdfsMiniCluster dfsCluster;
  private YarnMiniCluster yarnCluster;
  private String confDirPath = null;
  private boolean hdfsCleanup;
  private boolean yarnCleanup;
  private boolean cleanup;

  private final YarnConfiguration configuration;

  public HdfsYarnTestMiniCluster( ClusterProperties properties ) {
    this.properties = properties;
    this.hdfsCleanup = properties.getHdfsCleanup();
    this.yarnCleanup = properties.getYarnCleanup();
    this.cleanup = properties.getCleanup();

    this.configuration = new YarnConfiguration( new Configuration() );
    this.configuration.setClass( YarnConfiguration.RM_SCHEDULER, FifoScheduler.class, ResourceScheduler.class );
  }

  public void start() throws Exception {
    setUpHDFS();
    setUpYarn();

    //create temporary directory
    confDirPath = FileOperation.createTemporaryDirectory( CONF_DIR_PRF );
    //save the yarn configuration to a file
    String yarnSitePath = saveConfigs( configuration, confDirPath, YARN_CONF_FILE );
    LOG.info( YARN_SITE_PATH + yarnSitePath );
  }

  public void stop() throws Exception {
    if ( dfsCluster != null ) {
      try {
        dfsCluster.stop( hdfsCleanup );
      } catch ( Exception e ) {
        LOG.error( ERROR, e );
      }
    }
    if ( yarnCluster != null ) {
      try {
        yarnCluster.stop( yarnCleanup );
      } catch ( Exception e ) {
        LOG.error( ERROR, e );
      }
    }
    //cleaning temp directory
    if ( cleanup ) {
      FileOperation.deleteFolder( confDirPath );
    }
  }

  private void setUpHDFS() throws Exception {
    LOG.info( STARTING_HDFS );
    dfsCluster = new HdfsMiniCluster(properties, configuration);
    dfsCluster.start();
    LOG.info( HDFS_STARTED );
  }

  private void setUpYarn() throws Exception {
    LOG.info( STARTING_YARN );
    this.configuration.set( FS_DEFAULT, dfsCluster.getHdfsFileSystemHandle().getUri().toString() ); // use HDFS
    this.configuration.setInt( YARN_NDMG_DEL_DBG_DELAY, 60000 );

    // Disable resource checks by default
    if ( !this.configuration.getBoolean(
      YarnConfiguration.YARN_MINICLUSTER_CONTROL_RESOURCE_MONITORING,
      YarnConfiguration.DEFAULT_YARN_MINICLUSTER_CONTROL_RESOURCE_MONITORING ) ) {
      this.configuration.setBoolean( YarnConfiguration.NM_PMEM_CHECK_ENABLED, false );
      this.configuration.setBoolean( YarnConfiguration.NM_VMEM_CHECK_ENABLED, false );
    }

    CountDownLatch resourceManagerWait = new CountDownLatch( 1 );
    yarnCluster = new YarnMiniCluster(properties, configuration);
    yarnCluster.start();


    Timer waitForResourceManager = new Timer();
    waitForResourceManager.schedule( new TimerTask() {
      int waitingTime = 0;

      @Override public void run() {
        LOG.info( WAITING_RM_START );
        if ( yarnCluster.getConfig().get( YarnConfiguration.RM_ADDRESS ) != null || waitingTime >= MAX_WAITING_TIME ) {
          LOG.info(
            yarnCluster.getConfig().get( YarnConfiguration.RM_ADDRESS ) != null
              ? String.format( RM_STARTED, yarnCluster.getConfig().get( YarnConfiguration.RM_ADDRESS ) )
              : RM_FAILED
          );
          resourceManagerWait.countDown();
          waitForResourceManager.cancel();
        }
        waitingTime += PERIOD;
      }
    }, 0, PERIOD );
    resourceManagerWait.await();
    LOG.info( YARN_STARTED );
  }

  public String getConfDirPath() {
    return confDirPath;
  }

  public static String saveConfigs( Configuration configuration, String confDirPath, String filename )
    throws IOException {
    String path = confDirPath + File.separator + filename;
    try ( FileOutputStream fos = new FileOutputStream( path ) ) {
      configuration.writeXml( fos );
    }
    return path;
  }
}