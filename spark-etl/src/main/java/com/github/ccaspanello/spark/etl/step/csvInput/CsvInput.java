package com.github.ccaspanello.spark.etl.step.csvInput;

import com.github.ccaspanello.spark.etl.step.BaseStep;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.util.List;

/**
 * Created by ccaspanello on 3/28/18.
 */
public class CsvInput extends BaseStep<CsvInputMeta> {

  public CsvInput( CsvInputMeta meta ) {
    super( meta );
  }

  @Override
  public void execute() {


    if ( getStepMeta().isUsePathsFromStream() ) {

      Dataset<Row> result = null;

      List<Row> rows = getIncoming().stream().findFirst().get().getData().collectAsList();

      for ( Row row : rows ) {
        int index = row.fieldIndex( "Filename" );
        Dataset<Row> ds = getSparkSession().read().option( "header", "true" ).csv( row.getString( index ) );
        if ( result == null ) {
          result = ds;
        } else {
          result = result.union( ds );
        }
      }
      setOutgoing( result );
    } else {
      Dataset<Row> result = getSparkSession().read().option( "header", "true" ).csv( getStepMeta().getPath() );
      setOutgoing( result );
    }


  }

  private void setOutgoing( Dataset<Row> outgoing ) {
    getOutgoing().forEach( hop -> hop.setData( outgoing ) );
  }
}
