package com.github.ccaspanello.spark.testing;

import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;

public class AbstractSparkTest {

  protected SparkSession spark;

  @Before
  public void before(){
    spark = SparkSession.builder().master( "local[*]" ).getOrCreate();
  }

  @After
  public void after(){
    spark.stop();
  }

}
