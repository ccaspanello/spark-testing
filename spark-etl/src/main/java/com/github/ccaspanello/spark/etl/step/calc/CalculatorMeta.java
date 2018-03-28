package com.github.ccaspanello.spark.etl.step.calc;

import com.github.ccaspanello.spark.etl.step.BaseStepMeta;

/**
 * Created by ccaspanello on 3/27/18.
 */
public class CalculatorMeta extends BaseStepMeta {

  private String columnName;
  private String fieldA;
  private String fieldB;

  public CalculatorMeta( String name ) {
    super( name );
  }

  public String getColumnName() {
    return columnName;
  }

  public void setColumnName( String columnName ) {
    this.columnName = columnName;
  }

  public String getFieldA() {
    return fieldA;
  }

  public void setFieldA( String fieldA ) {
    this.fieldA = fieldA;
  }

  public String getFieldB() {
    return fieldB;
  }

  public void setFieldB( String fieldB ) {
    this.fieldB = fieldB;
  }
}
