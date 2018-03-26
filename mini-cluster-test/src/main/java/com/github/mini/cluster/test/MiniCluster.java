package com.github.mini.cluster.test;

public interface MiniCluster {
  void start() throws Exception;

  void stop( boolean cleanUp ) throws Exception;

  void cleanUp() throws Exception;
}