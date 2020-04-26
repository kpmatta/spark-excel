package com.xorbit.spark.excel

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

class ExcelReaderTest extends org.scalatest.FunSuite {

  test ("Read excel") {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Test").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val schema = StructType(List(
      StructField("ID", IntegerType, true),
      StructField("Name", StringType, true),
      StructField("City", StringType, true),
      StructField("Date", StringType, true),
      StructField("Value", DoubleType, true)
    ))

    val df = spark.read
      .format("com.xorbit.spark.excel")
      .option("headerIndex", 1)
      .option("startRowIndex", 2)
      .option("endRowIndex", 3)
      .option("startColIndex", 1)
      .option("endColIndex", 5)
      .schema(schema)
      .load("sample.xlsx")

    df.printSchema()
    df.show()
  }
}
