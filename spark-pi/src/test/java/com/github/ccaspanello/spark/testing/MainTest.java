package com.github.ccaspanello.spark.testing;

import org.apache.spark.SparkContext;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;

/**
 * Created by ccaspanello on 2/16/18.
 */
@Guice(modules = GuiceExampleModule.class)
public class MainTest {

  public static final String MSG1 = "Invalid number of arguments 0.  Valid number of arguments is 1.";
  public static final String MSG2 = "Invalid number of arguments 2.  Valid number of arguments is 1.";
  public static final String MSG3 = "Input foo is not an integer.  Please enter an integer.";

  @Inject
  private SparkContext sc;

  @Test(expectedExceptions = SparkPiException.class, expectedExceptionsMessageRegExp = MSG1)
  public void testParameterCountError1(){
    Main.main( new String[]{} );
  }

  @Test(expectedExceptions = SparkPiException.class, expectedExceptionsMessageRegExp = MSG2)
  public void testParameterCountError2(){
    Main.main( new String[]{"1","2"} );
  }

  @Test(expectedExceptions = SparkPiException.class, expectedExceptionsMessageRegExp = MSG3)
  public void testParseError(){
    Main.main( new String[]{"foo"} );
  }
  @Test
  public void testValidParameters(){
    Main.main( new String[]{"100"} );
  }
}
