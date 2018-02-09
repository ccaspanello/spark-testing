/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.ccaspanello.spark.testing;

import org.apache.spark.SparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;

/**
 * Main Spark Application
 * <p>
 * Created by ccaspanello on 1/25/17.
 */
public class Main {

  private static final Logger LOG = LoggerFactory.getLogger( Main.class );

  public static void main( String[] args ) {
    int input = parseArgs( args );
    SparkContext sparkContext = SparkContext.getOrCreate();
    SparkPi sparkPi = new SparkPi( sparkContext );
    double result = sparkPi.run( Integer.parseInt( args[ 0 ] ) );
    LOG.info( "Result: {}" );
  }

  private static int parseArgs( String[] args ) {
    if ( args.length == 1 ) {
      return parseArg( args[ 0 ] );
    } else {
      String msg = "Invalid number of arguments %s.  Valid number of arguments is 1.";
      throw new SparkPiException( String.format( msg, args.length ) );
    }
  }

  private static int parseArg( String arg ) {
    try {
      return Integer.valueOf( arg );
    } catch ( NumberFormatException e ) {
      String msg = "Input %s is not an integer.  Please enter an integer.";
      throw new SparkPiException( String.format(msg, arg) );
    }
  }

}
