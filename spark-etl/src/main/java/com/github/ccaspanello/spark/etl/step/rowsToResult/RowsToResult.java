package com.github.ccaspanello.spark.etl.step.rowsToResult;

import com.github.ccaspanello.spark.etl.step.BaseStep;

/**
 * Created by ccaspanello on 3/27/18.
 */
public class RowsToResult extends BaseStep<RowsToResultMeta> {

  public RowsToResult( RowsToResultMeta meta ) {
    super( meta );
  }

  @Override
  public void execute() {
    // No-Op
  }
}
