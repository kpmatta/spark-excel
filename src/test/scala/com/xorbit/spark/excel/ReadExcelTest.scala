package com.xorbit.spark.excel

class ReadExcelTest extends org.scalatest.FunSuite {
  test("Test getHeader") {
    SparkSessionLocal()
    val header = ReadExcel.getHeader(
      "us_corona_data.xlsx",
      "",
      5,
      1,
      -1)

    println(header.mkString(","))
  }
}
