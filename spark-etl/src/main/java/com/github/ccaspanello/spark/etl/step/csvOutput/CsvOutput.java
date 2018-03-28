package com.github.ccaspanello.spark.etl.step.csvOutput;

import com.github.ccaspanello.spark.etl.step.BaseStep;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

/**
 * Created by ccaspanello on 3/27/18.
 */
public class CsvOutput extends BaseStep<CsvOutputMeta> {

  public CsvOutput( CsvOutputMeta meta ) {
    super( meta );
  }

  @Override
  public void execute() {
    String path = getStepMeta().getPath();

    getResultFiles().add(path);

    Dataset<Row> incoming = getIncoming().stream().findFirst().get().getData();
    incoming.write().mode( SaveMode.Overwrite ).option( "header", "true" ).csv(path);

    getOutgoing().forEach( hop -> hop.setData( incoming ) );
  }
}
