package com.xorbit.spark.excel

class ReadExcelTest extends SparkSessionLocal {

  test("Test getHeader") {
    val headerOpt = ReadExcel.getHeader(
       System.getProperty("user.dir") + "/TestFiles/us_corona_data.xlsx",
      "",
      1,
      1,
      -1)

    headerOpt.get.mkString(",") shouldBe "UID,iso2,iso3,code3,FIPS,Admin2,Lat,Combined_Key,Population,Date,Case,Long,Country/Region,Province/State"
  }

  test("Test with wrong header index") {
    val headerOpt = ReadExcel.getHeader(
      System.getProperty("user.dir") + "/TestFiles/Sample.xlsx",
      "",
      10,
      1,
      -1)

    headerOpt shouldBe None
  }
}
