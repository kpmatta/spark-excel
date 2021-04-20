package com.xorbit.spark.excel

import org.apache.spark.sql.SparkSession

trait SparkSessionLocal {
  def withSparkContext(testMethod: (SparkSession) => Any) {
    val spark = SparkSession.builder()
      .appName("udf testings")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()
    try {
      testMethod(spark)
    }
    finally spark.stop()
  }
}
