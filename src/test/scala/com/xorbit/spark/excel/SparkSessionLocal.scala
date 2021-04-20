package com.xorbit.spark.excel

import org.apache.spark.sql.SparkSession

trait SparkSessionLocal {
  lazy val spark : SparkSession = SparkSession.builder()
    .appName("testing")
    .master("local[*]")
    .getOrCreate()
}
