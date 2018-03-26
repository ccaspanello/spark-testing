package com.github.mini.cluster.test;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertTrue;

/**
 * Created by ccaspanello on 3/20/18.
 */
public class SparkPiIntegrationTest {

  private static final Logger LOG = LoggerFactory.getLogger( SparkPiIntegrationTest.class );

  //private static ClusterProperties properties;
  //private static HdfsYarnTestMiniCluster hdfsYarnMiniCluster;

  @BeforeClass
  public static void beforeAll() throws Exception {
    //properties = new ClusterProperties();
    //hdfsYarnMiniCluster = new HdfsYarnTestMiniCluster( properties );
    //hdfsYarnMiniCluster.start();
  }

  @AfterClass
  public static void afterAll() throws Exception {
    //hdfsYarnMiniCluster.stop();
  }

  @Test
  public void test() throws Exception {
    LOG.info( "test" );

    //String config = hdfsYarnMiniCluster.getConfDirPath();
    String config = "/Users/ccaspanello/Desktop/spark-testing/cluster_conf/yarn-site.xml";

    Map<String, String> env = new HashMap<>();
    env.put( "SPARK_HOME", "/Users/ccaspanello/spark" );
    env.put( "HADOOP_CONF_DIR", config );
    env.put("HADOOP_USER_NAME", "devuser");
    env.put( "SPARK_DIST_CLASSPATH", "/Users/ccaspanello/Downloads/hadoop-common-test-0.22.0.jar" );

    SparkLauncher sparkLauncher = new SparkLauncher( env );

    //sparkLauncher.setMaster( "local[*]" );

    sparkLauncher.setMaster( "yarn" );
    sparkLauncher.setDeployMode( "client" );

    sparkLauncher.setAppResource( "/Users/ccaspanello/spark/examples/jars/spark-examples_2.11-2.1.0.jar" );
    sparkLauncher.setMainClass( "org.apache.spark.examples.SparkPi" );
    sparkLauncher.addAppArgs( "10" );

    run(sparkLauncher);


  }

  private void run(SparkLauncher sparkLauncher) throws Exception{

    // Create listener to track different statuses
    CompletableFuture<Void> isRunning= new CompletableFuture<>();
    CompletableFuture<Boolean> isSuccessful = new CompletableFuture<>();
    SparkAppHandle.Listener listener = new SparkAppHandle.Listener() {
      @Override
      public void stateChanged( SparkAppHandle sparkAppHandle ) {
        SparkAppHandle.State state = sparkAppHandle.getState();

        // Track Submit Status
        if(state.equals( SparkAppHandle.State.RUNNING )||state.equals( SparkAppHandle.State.SUBMITTED )){
          isRunning.complete( null );
        }

        // Track Completion Status
        if ( state.equals( SparkAppHandle.State.KILLED ) || state.equals( SparkAppHandle.State.FAILED ) || state
          .equals( SparkAppHandle.State.LOST ) ) {
          isSuccessful.complete(false);
        } else if ( state.equals( SparkAppHandle.State.FINISHED ) ) {
          isSuccessful.complete(true);
        }
      }

      @Override
      public void infoChanged( SparkAppHandle sparkAppHandle ) {
        // no-op
      }
    };

    // Start Application with Listener
    sparkLauncher.startApplication( listener );

    // Wait until Application is Running
    try{
      isRunning.get(10, TimeUnit.SECONDS);
    }catch(TimeoutException e){
      throw new RuntimeException("Spark Application not running within 10 seconds.");
    }

    // Wait until Application is Complete
    try{
      boolean successful = isSuccessful.get(1, TimeUnit.MINUTES);
      assertTrue( successful );
    }catch(TimeoutException e){
      throw new RuntimeException("Spark Application not completed within 1 minute.");
    }
  }

}
