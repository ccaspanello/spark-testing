package com.github.ccaspanello.spark.etl.step.log;

import com.github.ccaspanello.spark.etl.step.BaseStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by ccaspanello on 3/27/18.
 */
public class WriteToLog extends BaseStep<WriteToLogMeta> {

  private static final Logger LOG = LoggerFactory.getLogger( WriteToLog.class );

  public WriteToLog( WriteToLogMeta meta) {
    super( meta );
  }

  @Override
  public void execute() {
      getIncoming().stream().forEach( hop -> {
        try{
          LOG.info("Hop: {} - {} -> {}", hop.incomingStep().getStepMeta().getName(), hop.outgoingStep().getStepMeta().getName());
          LOG.info("Write to Log: {}", getStepMeta().getName());
          hop.getData().show();
          LOG.info("Count: {}", hop.getData().count());
        }catch(Exception e){
          throw new RuntimeException("Unexpected Error in step: "+getStepMeta().getName(), e);
        }
      } );
  }
}