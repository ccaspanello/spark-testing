package com.github.ccaspanello.spark.etl.api;

import com.github.ccaspanello.spark.etl.StepRegistry;
import com.github.ccaspanello.spark.etl.TransContext;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Step Interface
 * <p>
 * Interface for classes that hold step logic.
 * <p>
 * Created by ccaspanello on 1/29/2018.
 */
public interface Step extends Serializable {

  /**
   * Executes Step Logic
   */
  void execute();

  //<editor-fold desc="Getters & Setters">
  void setSparkSession( SparkSession spark );

  SparkSession getSparkSession();

  void setStepRegistry( StepRegistry stepRegistry );

  StepRegistry getStepRegistry();

  StepMeta getStepMeta();

  Set<Hop> getIncoming();

  Set<Hop> getOutgoing();

  Set<String> getResultFiles();

  boolean isTerminating();

  //</editor-fold>
}
