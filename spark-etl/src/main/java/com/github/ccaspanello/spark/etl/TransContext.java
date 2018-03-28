package com.github.ccaspanello.spark.etl;

import org.apache.spark.sql.SparkSession;

/**
 * TODO Make Thread Safe if used inside executors
 *
 * Created by ccaspanello on 2/4/18.
 */
public class TransContext {

  private final SparkSession sparkSession;
  private final StepRegistry stepRegistry;

  public TransContext( SparkSession sparkSession, StepRegistry stepRegistry ) {
    this.sparkSession = sparkSession;
    this.stepRegistry = stepRegistry;
  }

  public SparkSession getSparkSession() {
    return sparkSession;
  }

  public StepRegistry getStepRegistry() {
    return stepRegistry;
  }
}
