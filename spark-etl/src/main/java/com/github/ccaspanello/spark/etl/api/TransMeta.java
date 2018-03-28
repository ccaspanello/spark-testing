package com.github.ccaspanello.spark.etl.api;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TransMeta
 * <p>
 * Transformation model, able to be serialized/deserialized.
 * <p>
 * Created by ccaspanello on 1/29/18.
 */
public class TransMeta implements Serializable {

  private final String name;
  private final List<StepMeta> steps;
  private final List<HopMeta> hops;

  public TransMeta( String name ) {
    this.name = name;
    this.steps = new ArrayList<>();
    this.hops = new ArrayList<>();
  }

  //<editor-fold desc="Getters & Setters">
  public String getName() {
    return name;
  }

  public List<StepMeta> getSteps() {
    return steps;
  }

  public List<HopMeta> getHops() {
    return hops;
  }
  //</editor-fold>

}
