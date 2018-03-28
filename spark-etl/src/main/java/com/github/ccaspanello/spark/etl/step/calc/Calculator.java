package com.github.ccaspanello.spark.etl.step.calc;

import com.github.ccaspanello.spark.etl.step.BaseStep;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by ccaspanello on 3/27/18.
 */
public class Calculator extends BaseStep<CalculatorMeta> {

  private static final Logger LOG = LoggerFactory.getLogger( Calculator.class );

  public Calculator( CalculatorMeta meta ) {
    super( meta );
  }

  @Override
  public void execute() {
    String colName = getStepMeta().getColumnName();
    String expression = getStepMeta().getFieldA() + "+" + getStepMeta().getFieldB();
    Dataset<Row> result = getIncoming().stream().findFirst().get().getData()
      .withColumn( colName, org.apache.spark.sql.functions.expr( expression ) );
    getOutgoing().forEach( hop -> hop.setData( result ) );
  }
}
