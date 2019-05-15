package com.github.ccaspanello.spark.testing;

import org.junit.Test;

import static org.junit.Assert.assertEquals;


/**
 * Created by ccaspanello on 12/29/17.
 */
public class SparkPiTest extends AbstractSparkTest {

  @Test
  public void testRun1() {
    SparkPi sparkPi = new SparkPi(spark);
    double result = sparkPi.run( 1);
    assertEquals(3.14, result, 0.1);
  }

  @Test
  public void testRun10() {
    SparkPi sparkPi = new SparkPi(spark);
    double result = sparkPi.run( 10);
    assertEquals(3.14, result, 0.05);
  }

}
