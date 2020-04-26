package com.xorbit.spark

import org.apache.spark.sql.DataFrameReader

package object excel {

  implicit class ExcelDataFrameReader(val dataFrameReader : DataFrameReader) extends AnyVal {

    def excel(
               headerIndex: Int = 1,
               startRowIndex : Int = 2,
               endRowIndex : Int = -1,
               startColIndex : Int = 1,
               endColIndex : Int = -1,
               timestampFormat: String = null
             ): DataFrameReader = {
      Map(
        "headerIndex" -> headerIndex,
        "startRowIndex" -> startRowIndex,
        "endRowIndex" -> endRowIndex,
        "startColIndex" -> startColIndex,
        "endColIndex" -> endColIndex,
        "timestampFormat" -> timestampFormat
      ).foldLeft(dataFrameReader.format("com.xorbit.spark.excel")) {
        case (dfReader, (key, value)) =>
          value match {
            case null => dfReader
            case v => dfReader.option(key, v.toString)
          }
      }
    }

  }
}
