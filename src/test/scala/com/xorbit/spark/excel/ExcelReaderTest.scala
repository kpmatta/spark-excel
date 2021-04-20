package com.xorbit.spark.excel

import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ExcelReaderTest extends AnyWordSpec with Matchers with SparkSessionLocal {

  "ExcelReaderTest-us_corona_data" ignore {
    "Read excel" in withSparkContext { (spark) =>

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

      spark.time {
        val df = spark.read
          .format("com.xorbit.spark.excel")
          .option("headerIndex", 1)
          .option("startDataRowIndex", 2)
          .option("endDataRowIndex", 1000)
          .option("startColIndex", 1)
          .option("endColIndex", schema.size)
          .option("inferSchema", "true")
          //        .schema(schema)
          .load(System.getProperty("user.dir") + "/TestFiles/us_corona_data.xlsx")

        df.count() shouldBe 999
        df.printSchema()
        df.show()
      }
    }
  }

  "ExcelReaderTest-Sample.xlsx" ignore {
    "Read Excel" in withSparkContext { (spark) =>

      spark.sparkContext.setLogLevel("ERROR")
      val schema = StructType(List(
        StructField("Id", IntegerType, true),
        StructField("Name", StringType, true),
        StructField("City", StringType, true),
        StructField("Date", DateType, true),
        StructField("Value1", DecimalType(16, 4), true),
        StructField("Calc1", StringType, true)
      ))

      val df = spark.read
        .format("com.xorbit.spark.excel")
        .option("headerIndex", 10) // ignores this option,  header reading as schema is provided
        .option("startDataRowIndex", 4)
        .option("endDataRowIndex", -1)
        .option("startColIndex", 1)
        .option("endColIndex", -1)
        .option("inferSchema", true) // ignores this option, as schema is provided
        .schema(schema)
        .load(System.getProperty("user.dir") + "/TestFiles/Sample.xlsx")

      println(df.count())
      df.printSchema()
      df.show()
    }
  }

  "Pretty Print : Two Dimensional Data" ignore {
    def prettyPrint2D(data: Array[Array[Any]],
                      colSeparator: Boolean = false,
                      rowSeparator: Boolean = false): Unit = {

      val printRowSep = (dashLine: String) => if (rowSeparator) println(dashLine)

      // get max string length of each column
      val maxLens = data.transpose.map(_.map(_.toString.length).max) //10,8,10,6

      // zip max field length to each value and format the string with left padding and spaces
      val paddedData = data
        .map(r => r.zip(maxLens))
        .map(r => r.map(c => f"%%${c._2 + 2}s".format(c._1 + " ")))

      val headColSep = if (colSeparator) "+" else ""
      val colSepChar = if (colSeparator) "|" else ""

      val dashLine = paddedData.head.map(r => "-" * r.length).mkString(headColSep, headColSep, headColSep)

      printRowSep(dashLine)
      paddedData.foreach { row =>
        println(row.mkString(colSepChar, colSepChar, colSepChar))
        printRowSep(dashLine)
      }
    }

    val data: Array[Array[Any]] = Array(
      Array("Alabama", "New york", "Iowa", "Oregon"),
      Array("Montgomery", "Albany", "Des Moines", "Salem"),
      Array(10.21, 3, 1234, 123456789))

    prettyPrint2D(data, colSeparator = true)

    /*
    output will be:
    +------------+----------+------------+-----------+
    |    Alabama | New york |       Iowa |    Oregon |
    +------------+----------+------------+-----------+
    | Montgomery |   Albany | Des Moines |     Salem |
    +------------+----------+------------+-----------+
    |      10.21 |        3 |       1234 | 123456789 |
    +------------+----------+------------+-----------+
     */
  }
}
