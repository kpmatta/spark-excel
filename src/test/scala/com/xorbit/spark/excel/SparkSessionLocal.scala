package com.xorbit.spark.excel

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

trait SparkSessionLocal extends AnyFunSuite with Matchers  with BeforeAndAfterEach {
  var spark : SparkSession = _
  override def beforeEach() {
    spark = SparkSession.builder()
      .appName("udf testings")
      .master("local[*]")
      .config("spark.driver.bindAddress","127.0.0.1")
      .getOrCreate()
  }

  override def afterEach() {
    spark.stop()
  }
}
