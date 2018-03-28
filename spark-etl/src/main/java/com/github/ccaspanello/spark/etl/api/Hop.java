package com.github.ccaspanello.spark.etl.api;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.jgrapht.graph.DefaultEdge;

import javax.rmi.CORBA.StubDelegate;

/**
 * Hop
 *
 * Wraps the DefaultEdge to define the to/from steps.  This wrapper will cast the source/target to Steps which by
 * default are not accessible.
 *
 * Created by ccaspanello on 1/29/18.
 */
public class Hop extends DefaultEdge {

    private HopMeta hopMeta;
    private Dataset<Row> data;

    public Hop(HopMeta hopMeta){
        this.hopMeta = hopMeta;
    }

    //<editor-fold desc="Getters & Setters">
    public Step incomingStep() {
        return (Step) getSource();
    }

    public Step outgoingStep() {
        return (Step) getTarget();
    }

    public Dataset<Row> getData() {
        return data;
    }

    public void setData( Dataset<Row> data ) {
        this.data = data;
    }

    public HopMeta getHopMeta() {
        return hopMeta;
    }
    //</editor-fold>

}
