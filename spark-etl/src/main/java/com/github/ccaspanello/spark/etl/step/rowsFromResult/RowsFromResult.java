package com.github.ccaspanello.spark.etl.step.rowsFromResult;

import com.github.ccaspanello.spark.etl.step.BaseStep;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Created by ccaspanello on 3/27/18.
 */
public class RowsFromResult extends BaseStep<RowsFromResultMeta> {

  private Dataset<Row> initialDataset;

  public RowsFromResult( RowsFromResultMeta meta ) {
    super( meta );
  }

  @Override
  public void execute() {
    getOutgoing().forEach( hop -> hop.setData( initialDataset ) );
  }

  public void setInitialDataset( Dataset<Row> initialDataset ) {
    this.initialDataset = initialDataset;
  }
}
