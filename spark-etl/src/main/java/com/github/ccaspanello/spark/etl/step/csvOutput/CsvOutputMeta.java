package com.github.ccaspanello.spark.etl.step.csvOutput;

import com.github.ccaspanello.spark.etl.step.BaseStepMeta;

/**
 * Created by ccaspanello on 3/27/18.
 */
public class CsvOutputMeta extends BaseStepMeta {

  private String path;

  public CsvOutputMeta( String name ) {
    super( name );
  }

  public String getPath() {
    return path;
  }

  public void setPath( String path ) {
    this.path = path;
  }
}
