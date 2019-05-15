package com.github.ccaspanello;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
public class SparkRunner {

  private static final String SPARK_HOME = "SPARK_HOME";
  private static final String HADOOP_CONF_DIR = "HADOOP_CONF_DIR";
  private static final String HADOOP_USER_NAME = "HADOOP_USER_NAME";

  public static void main( String[] args ) throws Exception {
    log.info( "main(args: {})", args );

    String hadoopUserName = "devuser";
    System.setProperty( "java.util.logging.SimpleFormatter.format", "%5$s%6$s%n" );
    System.setProperty( HADOOP_USER_NAME, hadoopUserName );

    String sparkHome = "/Users/ccaspanello/spark/spark-2.4.0-bin-hadoop2.6";
    String localResource = "/Users/ccaspanello/git/ccaspanello/spark-testing/spark-pi/target/spark-pi-1.0-SNAPSHOT.jar";
    String appResource = "hdfs:///user/devuser/ccaspanello/spark-pi-1.0-SNAPSHOT.jar";
    String mainClass = "com.github.ccaspanello.spark.testing.Main";
    String hadoopConfDir = "/Users/ccaspanello/Desktop/hordor26";

    copyToHdfs( hadoopConfDir, localResource, appResource );

    Map<String, String> env = new HashMap<>();
    env.put( SPARK_HOME, sparkHome );
    env.put( HADOOP_CONF_DIR, hadoopConfDir );
    env.put( HADOOP_USER_NAME, hadoopUserName );

    SparkLauncher launcher = new SparkLauncher( env );
    launcher.setAppResource( appResource );
    launcher.setMainClass( mainClass );
    launcher.setVerbose( true );
    launcher.addAppArgs( "10" );

    launcher.setMaster( "yarn" );
    launcher.setDeployMode( "cluster" );

    launcher.setConf( "spark.eventLog.enabled", "true" );
    launcher.setConf( "spark.eventLog.dir", "hdfs:///spark2-history/" );
    launcher.setConf( "spark.hadoop.yarn.timeline-service.enabled", "false" );
    launcher.setConf( "spark.yarn.archive", "hdfs:/hdp/apps/2.6.5.0-292/spark2/spark2-hdp-yarn-archive.tar.gz" );
    launcher.setConf("spark.driver.extraJavaOptions","-Dhdp.version=2.6.5.0-292");
    launcher.setConf("spark.yarn.am.extraJavaOptions","-Dhdp.version=2.6.5.0-292");

    CompletableFuture<Void> isRunning = new CompletableFuture<>();
    CompletableFuture<Void> isComplete = new CompletableFuture<>();

    SparkAppHandle sparkAppHandle = launcher.startApplication( new SparkAppHandle.Listener() {
      @Override
      public void stateChanged( SparkAppHandle sparkAppHandle ) {
        log.info( "stateChanged( sparkAppHandle: {})", sparkAppHandle.getState() );
        SparkAppHandle.State state = sparkAppHandle.getState();
        if ( state.equals( SparkAppHandle.State.CONNECTED ) ) {
          isRunning.complete( null );
        } else if ( state.isFinal() | state.equals( SparkAppHandle.State.FAILED ) ) {
          isComplete.complete( null );
        }
      }

      @Override
      public void infoChanged( SparkAppHandle sparkAppHandle ) {
        log.info( "infoChanged( sparkAppHandle: {})", sparkAppHandle );
      }
    } );

    sparkAppHandle.getAppId();

    isRunning.get( 2, TimeUnit.MINUTES );
    isComplete.get();
  }

  private static void copyToHdfs( String hadoopConfDir, String localResource, String appResource ) {
    try {
      Configuration conf = new Configuration();
      conf.set( "fs.defaultFS", "hdfs://hordor26-n1:8020" );
      FileSystem fs = FileSystem.get( conf );
      fs.copyFromLocalFile( new Path( localResource ), new Path( appResource ) );
    } catch ( IOException e ) {
      throw new RuntimeException( "Unable to copy file to HDFS.", e );
    }
  }
}
