package com.github.ccaspanello.spark.etl;

import com.github.ccaspanello.spark.etl.api.Step;
import com.github.ccaspanello.spark.etl.api.StepMeta;
import com.github.ccaspanello.spark.etl.step.calc.Calculator;
import com.github.ccaspanello.spark.etl.step.calc.CalculatorMeta;
import com.github.ccaspanello.spark.etl.step.csvInput.CsvInput;
import com.github.ccaspanello.spark.etl.step.csvInput.CsvInputMeta;
import com.github.ccaspanello.spark.etl.step.csvOutput.CsvOutput;
import com.github.ccaspanello.spark.etl.step.csvOutput.CsvOutputMeta;
import com.github.ccaspanello.spark.etl.step.datagrid.DataGrid;
import com.github.ccaspanello.spark.etl.step.datagrid.DataGridMeta;
import com.github.ccaspanello.spark.etl.step.log.WriteToLog;
import com.github.ccaspanello.spark.etl.step.log.WriteToLogMeta;
import com.github.ccaspanello.spark.etl.step.rowsFromResult.RowsFromResult;
import com.github.ccaspanello.spark.etl.step.rowsFromResult.RowsFromResultMeta;
import com.github.ccaspanello.spark.etl.step.rowsToResult.RowsToResult;
import com.github.ccaspanello.spark.etl.step.rowsToResult.RowsToResultMeta;
import com.github.ccaspanello.spark.etl.step.transExecutor.TransExecutor;
import com.github.ccaspanello.spark.etl.step.transExecutor.TransExecutorMeta;
import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.Assert.assertEquals;

/**
 * Step Registry Test
 *
 * Created by ccaspanello on 1/29/18.
 */
public class StepRegistryTest {

  @Test
  public static void test(){
    StepRegistry stepRegistry = new StepRegistry();
    stepRegistry.init();

    Map<Class<? extends StepMeta>, Class<? extends Step>> registry = stepRegistry.getRegistry();

    assertEquals(registry.size(), 8);
    assertEquals(registry.get(CalculatorMeta.class), Calculator.class);
    assertEquals(registry.get(CsvInputMeta.class), CsvInput.class);
    assertEquals(registry.get(CsvOutputMeta.class), CsvOutput.class);
    assertEquals(registry.get(DataGridMeta.class), DataGrid.class);
    assertEquals(registry.get(WriteToLogMeta.class), WriteToLog.class);

    assertEquals(registry.get(RowsFromResultMeta.class), RowsFromResult.class);
    assertEquals(registry.get(RowsToResultMeta.class), RowsToResult.class);
    assertEquals(registry.get(TransExecutorMeta.class), TransExecutor.class);
  }
}
