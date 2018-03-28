package com.github.ccaspanello.spark.etl.api;

import com.github.ccaspanello.spark.etl.TransContext;
import com.github.ccaspanello.spark.etl.StepRegistry;
import com.github.ccaspanello.spark.etl.step.rowsFromResult.RowsFromResult;
import com.github.ccaspanello.spark.etl.step.rowsToResult.RowsToResult;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Transformation
 * <p>
 * Materialized version of TransMeta data; logical steps wired with hops.
 * <p>
 * Created by ccaspanello on 1/29/18.
 */
public class Transformation implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger( Transformation.class );

  private final TransContext transContext;
  private final String name;
  private TransMeta transMeta;

  public Transformation( TransContext transContext, TransMeta transMeta ) {
    this.name = transMeta.getName();
    this.transContext = transContext;
    this.transMeta = transMeta;
  }

  /**
   * Execute transformation
   */
  public Result execute() {
    List<Step> executionPlan = createExecutionPlan();
    executePlan( executionPlan );
    return new Result( executionPlan );
  }

  public Result executeSubtrans( Dataset<Row> incomming ) {
    List<Step> executionPlan = createExecutionPlan();

    // Inject Starting Dataset
    RowsFromResult rowsFromResult = (RowsFromResult) executionPlan.stream().filter(
      step -> step.getClass().equals( RowsFromResult.class ) )
      .findFirst().get();
    rowsFromResult.setInitialDataset( incomming );

    executePlan( executionPlan );

    // Return Ending Dataset
    RowsToResult rowsToResult =
      (RowsToResult) executionPlan.stream().filter( step -> step.getClass().equals( RowsToResult.class ) ).findFirst()
        .get();
    return new Result(executionPlan);
  }

  private List<Step> createExecutionPlan() {
    DirectedGraph<Step, Hop> graph = createGraph( transMeta, transContext.getStepRegistry() );
    LOG.warn( "STEP ORDER" );
    LOG.warn( "=============================" );
    List<Step> executionPlan = new ArrayList<>();
    TopologicalOrderIterator<Step, Hop> orderIterator = new TopologicalOrderIterator<>( graph );
    while ( orderIterator.hasNext() ) {
      Step step = orderIterator.next();
      LOG.warn( "Step -> {}", step.getStepMeta().getName() );
      Set<Hop> incoming = graph.incomingEdgesOf( step );
      Set<Hop> outgoing = graph.outgoingEdgesOf( step );

      LOG.warn( "   - Incoming: {}", incoming.size() );
      LOG.warn( "   - Outgoing: {}", outgoing.size() );

      step.getIncoming().addAll( incoming );
      step.getOutgoing().addAll( outgoing );

      executionPlan.add( step );
    }
    return executionPlan;
  }

  private void executePlan( List<Step> executionPlan ) {
    LOG.warn( "RUNNING STEPS" );
    LOG.warn( "=============================" );
    for ( Step step : executionPlan ) {
      LOG.warn( "***** -> {}", step.getStepMeta().getName() );
      step.setSparkSession( transContext.getSparkSession() );
      step.setStepRegistry( transContext.getStepRegistry() );
      step.execute();
    }
  }

  private DirectedGraph<Step, Hop> createGraph( TransMeta transMeta, StepRegistry stepRegistry ) {
    DirectedGraph<Step, Hop> graph = new DefaultDirectedGraph<>( Hop.class );

    // Create and Map Steps
    Map<String, Step> map = new HashMap<>();
    for ( StepMeta stepMeta : transMeta.getSteps() ) {
      map.put( stepMeta.getName(), stepRegistry.createStep( stepMeta ) );
    }

    for ( StepMeta stepMeta : transMeta.getSteps() ) {
      graph.addVertex( map.get( stepMeta.getName() ) );
    }

    for ( HopMeta hopMeta : transMeta.getHops() ) {
      Step incoming = map.get( hopMeta.getIncoming() );
      Step outgoing = map.get( hopMeta.getOutgoing() );
      graph.addEdge( incoming, outgoing, new Hop( hopMeta ) );
    }
    return graph;
  }

  //<editor-fold desc="Getters & Setters">
  public String getName() {
    return name;
  }
  //</editor-fold>

}
