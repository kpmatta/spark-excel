package com.xorbit.spark.excel

class ReadExcelTest extends org.scalatest.FunSuite {
  test("Test getHeader") {
    SparkSessionLocal()
    val header = ReadExcel.getHeader(
       System.getProperty("user.dir") + "/TestFiles/us_corona_data.xlsx",
      "",
      1,
      1,
      -1)

    println(header.mkString(","))
  }
}
