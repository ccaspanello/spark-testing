package com.github.ccaspanello.spark.testing;

import org.apache.spark.SparkContext;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;

import static org.testng.Assert.assertEquals;

/**
 * Created by ccaspanello on 12/29/17.
 */
@Guice(modules = GuiceExampleModule.class)
public class SparkPiTest {

  @Inject
  private SparkContext sc;

  @Test
  public void testRun1() {
    SparkPi sparkPi = new SparkPi(sc);
    double result = sparkPi.run( 1);
    assertEquals(3.14, result, 0.1);
  }

  @Test
  public void testRun10() {
    SparkPi sparkPi = new SparkPi(sc);
    double result = sparkPi.run( 10);
    assertEquals(3.14, result, 0.05);
  }

}
