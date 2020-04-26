package com.xorbit.spark.excel

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

class DefaultSource
  extends RelationProvider
  with SchemaRelationProvider
  with CreatableRelationProvider {
  override def createRelation( sqlContext: SQLContext,
                               parameters: Map[String, String]): ExcelRelation = {
    createRelation(sqlContext, parameters, null)
  }

  override def createRelation( sqlContext: SQLContext,
                               parameters: Map[String, String],
                               schema: StructType): ExcelRelation = {

    val filePath = parameters("path")
    val headerIndex : Int = parameters.getOrElse("headerIndex", "1").toInt
    val startRowIndex : Int = parameters.getOrElse("startRowIndex" , s"${headerIndex +1}").toInt
    val endRowIndex : Int = parameters.getOrElse("endRowIndex", "-1").toInt
    val startColIndex : Int = parameters.getOrElse("startColIndex", "1").toInt
    val endColIndex : Int = parameters.getOrElse("endColIndex", "-1").toInt
    val sheetName : String = parameters.getOrElse("sheetName", "")
    val userSchema : StructType = schema
    val timestampFormat: String = parameters.getOrElse("timestampFormat", null)

    ExcelRelation(
      filePath,
      sheetName,
      headerIndex,
      startRowIndex,
      endRowIndex,
      startColIndex,
      endColIndex,
      userSchema,
      timestampFormat)(sqlContext)
  }

  override def createRelation( sqlContext: SQLContext,
                               mode: SaveMode,
                               parameters: Map[String, String],
                               data: DataFrame): ExcelRelation = {

    throw new UnsupportedOperationException("Writing to excel is not implemented yet")
    // TODO: Unsupported
    createRelation(sqlContext, parameters, data.schema)
  }
}
