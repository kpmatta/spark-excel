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
      StructField("UID", IntegerType, true),
      StructField("ios2", StringType, true),
      StructField("ios3", StringType, true),
      StructField("code3", IntegerType, true),
      StructField("FIPS", IntegerType, true),
      StructField("Admin2", StringType, true),
      StructField("Lat", DoubleType, true),
      StructField("Combined_Key", StringType, true),
      StructField("Population", IntegerType, true),
      StructField("Date", StringType, true),
      StructField("Case", IntegerType, true),
      StructField("Long", DoubleType, true),
      StructField("Country", StringType, true),
      StructField("State", StringType, true)
    ))

    val df = spark.read
      .format("com.xorbit.spark.excel")
      .option("headerIndex", 1)
      .option("startRowIndex", 2)
      .option("endRowIndex", 400000)
      .option("startColIndex", 1)
      .option("endColIndex", schema.size)
      .schema(schema)
      .load("us_deaths.xlsx")

    println(df.count())
    df.printSchema()
    df.show()
  }
}
