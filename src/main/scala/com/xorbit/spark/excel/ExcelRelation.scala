package com.xorbit.spark.excel

import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, PrunedScan, TableScan}
import org.apache.spark.sql.types._

case class ExcelRelation (
    filePath : String,
    sheetName : String,
    headerIndex: Int = 1,
    startRowIndex : Int = 2,
    endRowIndex : Int = -1,
    startColIndex : Int = 1,
    endColIndex : Int = -1,
    userSchema : StructType = null,
    timestampFormat: String = null)(@transient val sqlContext: SQLContext)
  extends BaseRelation
    with TableScan
    with PrunedScan {

  override def schema: StructType = userSchema

  override def buildScan(): RDD[sql.Row] = {
    buildScan(schema.map(_.name).toArray)
  }

  override def buildScan(requiredColumns: Array[String]): RDD[sql.Row] = {
    val endColumnIndex = startColIndex + schema.size -1
    val data = ReadExcel.readData(
      filePath,
      sheetName,
      startRowIndex,
      endRowIndex,
      startColIndex,
      endColumnIndex,
      schema,
      requiredColumns)

    val dataRows = data
      .map( arrTokens => sql.Row.fromSeq(arrTokens))

    sqlContext.sparkContext.parallelize(dataRows)
  }
}

