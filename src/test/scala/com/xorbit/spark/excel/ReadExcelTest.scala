package com.xorbit.spark.excel

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ReadExcelTest extends AnyWordSpec with Matchers with SparkSessionLocal {

  "ReadExcelTest" should {
    "Test getHeader" in withSparkContext { (spark) =>
      val headerOpt = ReadExcel.getHeader(
        System.getProperty("user.dir") + "/TestFiles/us_corona_data.xlsx",
        "",
        1,
        1,
        -1)

      headerOpt.get.mkString(",") shouldBe "UID,iso2,iso3,code3,FIPS,Admin2,Lat,Combined_Key,Population,Date,Case,Long,Country/Region,Province/State"
    }
  }

  "ReadExcelTest" should {
    "Test with wrong header index" in withSparkContext { (spark) =>
      val headerOpt = ReadExcel.getHeader(
        System.getProperty("user.dir") + "/TestFiles/Sample.xlsx",
        "",
        10,
        1,
        -1)

      headerOpt shouldBe None
    }
  }
}
