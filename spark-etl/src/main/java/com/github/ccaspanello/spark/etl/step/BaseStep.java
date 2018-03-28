package com.github.ccaspanello.spark.etl.step;

import com.github.ccaspanello.spark.etl.StepRegistry;
import com.github.ccaspanello.spark.etl.TransContext;
import com.github.ccaspanello.spark.etl.api.Hop;
import com.github.ccaspanello.spark.etl.api.HopType;
import com.github.ccaspanello.spark.etl.api.Step;
import com.github.ccaspanello.spark.etl.api.StepMeta;
import org.apache.spark.sql.SparkSession;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Common Base Step Logic
 * <p>
 * Created by ccaspanello on 1/29/2018.
 */
public abstract class BaseStep<E extends StepMeta> implements Step {

  private final E meta;

  private SparkSession sparkSession;
  private StepRegistry stepRegistry;
  private Set<Hop> incoming;
  private Set<Hop> outgoing;
  private Set<String> resultFiles;

  public BaseStep( E meta ) {
    this.incoming = new HashSet<>(  );
    this.outgoing = new HashSet<>(  );
    this.resultFiles = new HashSet<>(  );
    this.meta = meta;
  }

  public Hop outgoingHop( HopType hopType ) {
    return outgoing.stream().filter( hop -> hop.getHopMeta().getHopType().equals( hopType ) ).findFirst().get();
  }

  //<editor-fold desc="Getters & Setters">
  public Set<Hop> getIncoming() {
    return incoming;
  }

  public Set<Hop> getOutgoing() {
    return outgoing;
  }

  public SparkSession getSparkSession() {
    return sparkSession;
  }

  public void setSparkSession( SparkSession sparkSession ) {
    this.sparkSession = sparkSession;
  }

  public StepRegistry getStepRegistry() {
    return stepRegistry;
  }

  public void setStepRegistry( StepRegistry stepRegistry ) {
    this.stepRegistry = stepRegistry;
  }

  public E getStepMeta() {
    return meta;
  }

  public Set<String> getResultFiles(){
    return resultFiles;
  }

  public boolean isTerminating(){
    return incoming.size() > 0 && outgoing.size() == 0;
  }
  //</editor-fold>
}
