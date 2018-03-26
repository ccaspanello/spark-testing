package com.github.mini.cluster.test;

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by ccaspanello on 3/21/18.
 */
public class MiniClusterMain {

  private static final Logger LOG = LoggerFactory.getLogger( MiniDFSCluster.class );

  public static void main( String[] args ) throws Exception {

    HdfsYarnTestMiniCluster hdfsYarnMiniCluster = new HdfsYarnTestMiniCluster( new ClusterProperties() );
    Thread t;

    Runtime.getRuntime().addShutdownHook( t = new Thread() {
      @Override
      public void run() {
        stopCluster( hdfsYarnMiniCluster );
        LOG.debug( "=========== MiniCluster Stopped ===================" );
      }
    } );

    try {
      LOG.debug( "=========== MiniCluster Starting ===================" );
      hdfsYarnMiniCluster.start();
    } catch ( Exception e ) {
      LOG.error( "Error starting the Mini Cluster: " , e );
      t.run();
      t.join();
    }
  }

  private static void stopCluster( HdfsYarnTestMiniCluster hdfsYarnMiniCluster ) {
    try {
      hdfsYarnMiniCluster.stop();
    } catch ( Exception e ) {
      LOG.error( "Error shutting down the Mini Cluster:", e );
    }
  }

}
