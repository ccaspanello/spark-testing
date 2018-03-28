package com.github.ccaspanello.spark.etl.api;

import com.github.ccaspanello.spark.etl.TransContext;
import com.github.ccaspanello.spark.etl.GuiceExampleModule;
import com.github.ccaspanello.spark.etl.step.calc.CalculatorMeta;
import com.github.ccaspanello.spark.etl.step.csvInput.CsvInputMeta;
import com.github.ccaspanello.spark.etl.step.csvOutput.CsvOutputMeta;
import com.github.ccaspanello.spark.etl.step.datagrid.Column;
import com.github.ccaspanello.spark.etl.step.datagrid.DataGridMeta;
import com.github.ccaspanello.spark.etl.step.log.WriteToLogMeta;
import com.github.ccaspanello.spark.etl.step.rowsFromResult.RowsFromResultMeta;
import com.github.ccaspanello.spark.etl.step.rowsToResult.RowsToResultMeta;
import com.github.ccaspanello.spark.etl.step.transExecutor.TransExecutorMeta;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ccaspanello on 2/4/18.
 */
@Guice( modules = GuiceExampleModule.class )
public class TransformationTest {

  private static final Logger LOG = LoggerFactory.getLogger( TransformationTest.class );

  @Inject
  private TransContext transContext;

  private File output;

  @BeforeTest
  public void beforeTest() {
    try {
      output = File.createTempFile( "test-", "" );
      // Delete file since it is created automatically.
      output.delete();
    } catch ( IOException e ) {
      throw new RuntimeException( "Unexpected error", e );
    }
  }

  @Test
  public void testSimple() throws Exception {
    TransMeta transMeta = parentTransMeta();

    Transformation transformation = new Transformation( transContext, transMeta );
    transformation.execute();

  }

  public TransMeta parentTransMeta() {

    // Data Grid
    DataGridMeta dataGrid = new DataGridMeta( "Data Grid" );
    dataGrid.getColumns().add( new Column( "key", DataTypes.StringType ) );
    dataGrid.getColumns().add( new Column( "value", DataTypes.IntegerType ) );
    for ( int i = 0; i < 25; i++ ) {
      List<String> row = new ArrayList<>();
      row.add("key"+i);
      row.add(""+i);
      dataGrid.getData().add( row );
    }

    // TransExecutor
    TransExecutorMeta transExecutor = new TransExecutorMeta( "Trans Executor" );
    TransMeta subTransMeta = childSubTransMeta();
    transExecutor.setTransMeta( subTransMeta );

    // CSV Input
    CsvInputMeta csvInput = new CsvInputMeta("CSV Input");
    csvInput.setPath( "/Users/ccaspanello/Desktop/temp/childOutput1" );
    csvInput.setUsePathsFromStream(true);

    // Write to Log
    WriteToLogMeta main = new WriteToLogMeta( "Write To Log: Main" );
    WriteToLogMeta resultRows = new WriteToLogMeta( "Write To Log: Result Rows" );
    WriteToLogMeta resultFiles = new WriteToLogMeta( "Write To Log: Result Files" );

    // Trans Meta
    TransMeta transMeta = new TransMeta( "Parent Transformation" );
    transMeta.getSteps().add( dataGrid );
    transMeta.getSteps().add( transExecutor );
    transMeta.getSteps().add( main );
    transMeta.getSteps().add( csvInput );
    transMeta.getSteps().add( resultRows );
    transMeta.getSteps().add( resultFiles );

    transMeta.getHops().add( new HopMeta( dataGrid, transExecutor ) );
    transMeta.getHops().add( new HopMeta( transExecutor, main, HopType.MAIN ) );
    transMeta.getHops().add( new HopMeta( transExecutor, resultRows, HopType.RESULT_ROWS ) );
    transMeta.getHops().add( new HopMeta( transExecutor, csvInput, HopType.RESULT_FILES ) );
    transMeta.getHops().add( new HopMeta( csvInput, resultFiles ) );

    return transMeta;
  }

  private TransMeta childSubTransMeta() {

    RowsFromResultMeta rowsFromResult = new RowsFromResultMeta( "Rows from result" );

    CalculatorMeta calculator = new CalculatorMeta( "Child Calculator" );
    calculator.setColumnName( "Child Calc" );
    calculator.setFieldA( "value" );
    calculator.setFieldB( "value" );

    CsvOutputMeta csvOutput1 = new CsvOutputMeta( "CSV Output 1" );
    csvOutput1.setPath("/Users/ccaspanello/Desktop/temp/childOutput1");

    CsvOutputMeta csvOutput2 = new CsvOutputMeta( "CSV Output 2" );
    csvOutput2.setPath("/Users/ccaspanello/Desktop/temp/childOutput2");

    RowsToResultMeta rowsToResult = new RowsToResultMeta( "Rows to result" );

    TransMeta transMeta = new TransMeta( "Child Transformation" );
    transMeta.getSteps().add( rowsFromResult );
    transMeta.getSteps().add( calculator );
    transMeta.getSteps().add( csvOutput1 );
    transMeta.getSteps().add( csvOutput2 );
    transMeta.getSteps().add( rowsToResult );

    transMeta.getHops().add( new HopMeta( rowsFromResult, calculator ) );
    transMeta.getHops().add( new HopMeta( calculator, csvOutput1 ) );
    transMeta.getHops().add( new HopMeta( csvOutput1, csvOutput2 ) );
    transMeta.getHops().add( new HopMeta( csvOutput2, rowsToResult) );

    return transMeta;
  }

}
