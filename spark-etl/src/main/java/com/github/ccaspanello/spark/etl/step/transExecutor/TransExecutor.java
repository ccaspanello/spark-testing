package com.github.ccaspanello.spark.etl.step.transExecutor;

import com.github.ccaspanello.spark.etl.TransContext;
import com.github.ccaspanello.spark.etl.api.HopType;
import com.github.ccaspanello.spark.etl.api.Result;
import com.github.ccaspanello.spark.etl.api.Transformation;
import com.github.ccaspanello.spark.etl.step.BaseStep;
import com.github.ccaspanello.spark.etl.step.rowsToResult.RowsToResult;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ccaspanello on 3/27/18.
 */
public class TransExecutor extends BaseStep<TransExecutorMeta> {

  private static final Logger LOG = LoggerFactory.getLogger( TransExecutor.class );

  public TransExecutor( TransExecutorMeta meta ) {
    super( meta );
  }

  @Override
  public void execute() {
    // Only accepts one hop
    Dataset<Row> incoming = getIncoming().stream().findFirst().get().getData();

    TransContext transContext = new TransContext( getSparkSession(), getStepRegistry() );
    Transformation transformation = new Transformation(transContext, getStepMeta().getTransMeta());
    Result result = transformation.executeSubtrans( incoming );

    // Find RowsToResult Step
    RowsToResult rowsToResult = (RowsToResult) result.getDatasets().keySet().stream().filter(step -> step.getClass().equals( RowsToResult.class )).findFirst().get();
    Dataset<Row> resultRows = rowsToResult.getIncoming().stream().findFirst().get().getData();
    Dataset<Row> resultFiles = resultFilesFromResult(result);

    outgoingHop( HopType.MAIN ).setData(incoming);
    outgoingHop( HopType.RESULT_ROWS ).setData(resultRows);
    outgoingHop( HopType.RESULT_FILES ).setData(resultFiles);
  }

  private Dataset<Row> resultFilesFromResult(Result result){
    // Define Columns
    List<StructField> fields = new ArrayList<>();
    StructField field = DataTypes.createStructField( "Filename", DataTypes.StringType, false );
    fields.add( field );

    StructType schema = DataTypes.createStructType( fields );

    List<Row> resultFiles = new ArrayList<>(  );
    result.getResultFiles().stream().forEach( s -> resultFiles.add( RowFactory.create(s)) );

      return getSparkSession().createDataFrame( resultFiles, schema );
  }
}
