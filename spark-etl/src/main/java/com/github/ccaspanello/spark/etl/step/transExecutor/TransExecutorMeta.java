package com.github.ccaspanello.spark.etl.step.transExecutor;

import com.github.ccaspanello.spark.etl.api.TransMeta;
import com.github.ccaspanello.spark.etl.step.BaseStepMeta;

/**
 * Created by ccaspanello on 3/27/18.
 */
public class TransExecutorMeta extends BaseStepMeta {

  private TransMeta transMeta;

  public TransExecutorMeta( String name ) {
    super( name );
  }

  public TransMeta getTransMeta() {
    return transMeta;
  }

  public void setTransMeta( TransMeta transMeta ) {
    this.transMeta = transMeta;
  }
}
