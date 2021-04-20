package com.xorbit.spark.excel

import org.apache.spark.sql.SparkSession

trait SparkSessionLocal {
  def withSparkContext(testMethod: (SparkSession) => Any) {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Spark test")
      .getOrCreate()
    try {
      testMethod(spark)
    }
    finally spark.close()
  }
}
