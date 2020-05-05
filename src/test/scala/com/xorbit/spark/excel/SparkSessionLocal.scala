package com.xorbit.spark.excel

import org.apache.spark.sql.SparkSession

object SparkSessionLocal {
  def apply(): SparkSession = SparkSession.builder()
    .appName("testing")
    .master("local[*]")
    .getOrCreate()
}
