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
    val startDataRowIndex : Int = parameters.getOrElse("startDataRowIndex" , s"${headerIndex +1}").toInt
    val endDataRowIndex : Int = parameters.getOrElse("endDataRowIndex", "-1").toInt
    val startColIndex : Int = parameters.getOrElse("startColIndex", "1").toInt
    val endColIndex : Int = parameters.getOrElse("endColIndex", "-1").toInt
    val sheetName : String = parameters.getOrElse("sheetName", "")
    val userSchema : StructType = schema
    val timestampFormat: String = parameters.getOrElse("timestampFormat", null)

    assert(headerIndex > 0 || headerIndex == -1, "headerIndex  is one based index, -1 for ignore header, default is 1")
    assert(startDataRowIndex > 0, "startDataRowIndex is one based index, default is headerIndex+1")
    assert(endDataRowIndex > 0 || endDataRowIndex == -1, "endDataRowIndex is one based index, -1 for all Rows, default is -1")
    assert(startColIndex > 0, "startColIndex is one based index, default is 1" )
    assert(endColIndex > 0 || endColIndex == -1, "endColIndex is one based index, -1 for all columns, default is -1")

    ExcelRelation(
      filePath,
      sheetName,
      headerIndex,
      startDataRowIndex,
      endDataRowIndex,
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
