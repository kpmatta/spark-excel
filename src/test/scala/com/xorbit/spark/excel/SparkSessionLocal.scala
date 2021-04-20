package com.xorbit.spark.excel

import org.apache.spark.sql.SparkSession
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSuite

trait SparkSessionLocal extends org.scalatest.FunSuite with Matchers with BeforeAndAfter {
  var spark : SparkSession = _
  before {
      spark = SparkSession.builder()
        .appName("testing")
        .master("local")
        .config("spark.driver.bindAddress","127.0.0.1")
        .getOrCreate()
  }

  after {
    spark.close()
  }

}
