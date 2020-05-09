package com.xorbit.spark.excel

import com.xorbit.spark.excel.util.InferSchema
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
    inferSchema : Boolean = false,
    timestampFormat: String = null)(@transient val sqlContext: SQLContext)
  extends BaseRelation
    with TableScan
    with PrunedScan {

  private lazy val inferredSchema : StructType = if (userSchema == null) inferSchemaFromData() else userSchema

  override def schema: StructType = inferredSchema

  override def buildScan(): RDD[sql.Row] = {
    buildScan(schema.map(_.name).toArray)
  }

  override def buildScan(requiredColumns: Array[String]): RDD[sql.Row] = {
    val calcEndIColIndex = startColIndex + schema.size - 1
    val data = ReadExcel.readData(
      filePath,
      sheetName,
      startRowIndex,
      endRowIndex,
      startColIndex,
      calcEndIColIndex,
      schema,
      if(requiredColumns.isEmpty) schema.map(_.name).toArray else requiredColumns)

    val dataRows = data
      .map(arrTokens => sql.Row.fromSeq(arrTokens))

    sqlContext.sparkContext.parallelize(dataRows)
  }

  def inferSchemaFromData() : StructType = {
    val header = ReadExcel.getHeader(filePath, sheetName, headerIndex, startColIndex, endColIndex)
    if(!inferSchema) {
      StructType(header.map(name => StructField(s"$name", StringType)))
    }
    else {
      val dataTypes = InferSchema(
        filePath,
        sheetName,
        startRowIndex,
        startRowIndex + 10,
        startColIndex,
        startColIndex + header.length-1)
      StructType(header.zip(dataTypes).map{case (name, dtype) => StructField(name, dtype)})
    }
  }
}

