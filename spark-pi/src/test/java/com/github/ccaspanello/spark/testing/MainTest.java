package com.github.ccaspanello.spark.testing;

import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Created by ccaspanello on 2/16/18.
 */
public class MainTest {

  @Rule
  public ExpectedException expectedEx = ExpectedException.none();

  public static final String MSG1 = "Invalid number of arguments 0.  Valid number of arguments is 1.";
  public static final String MSG2 = "Invalid number of arguments 2.  Valid number of arguments is 1.";
  public static final String MSG3 = "Input foo is not an integer.  Please enter an integer.";

  @Before
  public void beforeTest(){
    SparkSession.builder().master( "local[*]" ).getOrCreate();
  }

  @Test
  public void testParameterCountError1(){

    expectedEx.expect( SparkPiException.class );
    expectedEx.expectMessage( MSG1 );

    Main.main( new String[]{} );
  }

  @Test
  public void testParameterCountError2(){

    expectedEx.expect( SparkPiException.class );
    expectedEx.expectMessage( MSG2 );

    Main.main( new String[]{"1","2"} );
  }

  @Test
  public void testParseError(){

    expectedEx.expect( SparkPiException.class );
    expectedEx.expectMessage( MSG3 );

    Main.main( new String[]{"foo"} );
  }

  @Test
  public void testValidParameters(){
    Main.main( new String[]{"100"} );
  }
}
