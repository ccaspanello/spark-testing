package com.github.ccaspanello.spark.etl.step.datagrid;

import com.github.ccaspanello.spark.etl.step.BaseStep;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
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
 * Data Grid Step Logic
 * <p>
 * Created by ccaspanello on 1/29/18.
 */
public class DataGrid extends BaseStep<DataGridMeta> {

  private static final Logger LOG = LoggerFactory.getLogger( DataGrid.class );

  public DataGrid( DataGridMeta meta ) {
    super( meta );
  }

  @Override
  public void execute() {
    JavaSparkContext context = new JavaSparkContext( getSparkSession().sparkContext() );

    List<List<String>> list = getStepMeta().getData();
    List<Column> columns = getStepMeta().getColumns();

    // Define Columns
    List<StructField> fields = new ArrayList<>();
    for ( Column column : columns ) {
      StructField field = DataTypes.createStructField( column.getName(), column.getType(), column.isNullable() );
      fields.add( field );
    }

    StructType schema = DataTypes.createStructType( fields );

    // Create Row RDD
    JavaRDD<Row> rdd = context.parallelize( list ).map( ( List<String> row ) -> {

      // TODO Research if we need to convert the objects based on the DataType or if there is a better way.
      List<Object> convertedRow = new ArrayList<>();
      for ( int i = 0; i < columns.size(); i++ ) {
        Object value = convert( columns.get( i ), row.get( i ) );
        convertedRow.add( value );
      }

      return RowFactory.create( convertedRow.toArray() );
    } );

    Dataset<Row> result = getSparkSession().createDataFrame( rdd.collect(), schema );
    LOG.info( "ROW COUNT for {}: {}", getStepMeta().getName(), result.count() );
    getOutgoing().stream().forEach( h -> h.setData( result ) );
  }

  // TODO See if there is a better way of doing this using the DataTypes API, if not break out into a utility class.
  private Object convert( Column column, String value ) {
    switch ( column.getType().typeName() ) {
      case "string":
        return value;
      case "integer":
        return Integer.valueOf( value );
      default:
        return null;
    }

  }
}
