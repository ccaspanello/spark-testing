package com.github.ccaspanello.spark.etl.step.rowsToResult;

import com.github.ccaspanello.spark.etl.step.BaseStep;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Created by ccaspanello on 3/27/18.
 */
public class RowsToResult extends BaseStep<RowsToResultMeta> {

  public RowsToResult( RowsToResultMeta meta ) {
    super( meta );
  }

  @Override
  public void execute() {

  }

  public Dataset<Row> getOutputDataset() {
    return getIncoming().stream().findFirst().get().getData();
  }
}
