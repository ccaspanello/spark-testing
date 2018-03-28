package com.github.ccaspanello.spark.etl.api;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;

/**
 * HopMeta
 * <p>
 * Created by ccaspanello on 1/29/18.
 */
public class HopMeta implements Serializable {

  /**
   * Incoming Step Reference
   */
  private final String incoming;

  /**
   * Outgoing Step Reference
   */
  private final String outgoing;

  private final HopType hopType;

  public HopMeta( StepMeta incoming, StepMeta outgoing ) {
    this.incoming = incoming.getName();
    this.outgoing = outgoing.getName();
    this.hopType = HopType.DEFAULT;
  }

  public HopMeta( StepMeta incoming, StepMeta outgoing, HopType hopType ) {
    this.incoming = incoming.getName();
    this.outgoing = outgoing.getName();
    this.hopType = hopType;
  }

  //<editor-fold desc="Getters & Setters">
  public String getIncoming() {
    return incoming;
  }

  public String getOutgoing() {
    return outgoing;
  }

  public HopType getHopType() {
    return hopType;
  }
  //</editor-fold>

}
