package com.github.mini.cluster.test;

/**
 * Created by ccaspanello on 3/21/18.
 */
public class Main {

  public static void main(String[] args) throws Exception {
    HdfsYarnTestMiniCluster hdfsYarnMiniCluster = new HdfsYarnTestMiniCluster( new ClusterProperties() );
    hdfsYarnMiniCluster.start();
  }

}
