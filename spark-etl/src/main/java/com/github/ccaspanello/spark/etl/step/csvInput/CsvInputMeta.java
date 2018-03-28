package com.github.ccaspanello.spark.etl.step.csvInput;

import com.github.ccaspanello.spark.etl.step.BaseStepMeta;

/**
 * Created by ccaspanello on 3/28/18.
 */
public class CsvInputMeta extends BaseStepMeta {

  private String path;
  private boolean usePathsFromStream;

  public CsvInputMeta( String name ) {
    super( name );
  }

  public String getPath() {
    return path;
  }

  public void setPath( String path ) {
    this.path = path;
  }

  public boolean isUsePathsFromStream() {
    return usePathsFromStream;
  }

  public void setUsePathsFromStream( boolean usePathsFromStream ) {
    this.usePathsFromStream = usePathsFromStream;
  }
}
