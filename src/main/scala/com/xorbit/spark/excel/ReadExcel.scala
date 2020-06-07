package com.xorbit.spark.excel

import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.poi.ss.usermodel.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import com.monitorjbl.xlsx.StreamingReader
import com.xorbit.spark.excel.util.CellConversion
import scala.collection.JavaConverters._
import scala.util.control.Breaks._

object ReadExcel {

  /**
   * readFromHadoop
   */
  def openFile(location: String): FSDataInputStream = {
    val spark = SparkSession.builder().getOrCreate()
    val path = new Path(location)
    FileSystem
      .get(path.toUri, spark.sparkContext.hadoopConfiguration)
      .open(path)
  }

  /**
   * getHeader
   */
  def getHeader(filePath : String,
                sheetName : String,
                headerRowIdx : Int,
                startColIndex : Int,
                endColIndex : Int): Option[List[String]] = {

    assert(headerRowIdx > -1, "headerIndex is one based index, 0 for ignore header, default is 1")
    assert(startColIndex > 0, "startColIndex is one based index, default is 1" )
    assert(endColIndex > 0 || endColIndex == -1, "endColIndex is one based index, -1 for all columns, default is -1")

    if (headerRowIdx == -1 && endColIndex != -1) {
      Option(Range(0, endColIndex - startColIndex+1).map(i => s"_C$i").toList)
    }
    else {
      val is = openFile(filePath)
      val workbook = StreamingReader.builder()
        .rowCacheSize(10)
        .bufferSize(4096)
        .open(is)

      try {
        val sheet = if (sheetName.trim.isEmpty) workbook.getSheetAt(0) else workbook.getSheet(sheetName)
        if(headerRowIdx == 0) {
          Option(sheet.asScala.head.asScala.zipWithIndex.map( c => s"_C${c._2}").toList)
        }
        else {
          sheet.asScala
            .find(_.getRowNum+1 == headerRowIdx)
            .map{ row =>
              getRow(row, startColIndex, endColIndex)
                  .zipWithIndex.map{case(s, i) => if(s == null) s"_C${i}" else s}
            }
        }
      }
      finally {
        if(is != null) is.close()
        if(workbook != null) workbook.close()
      }
    }
  }

  def isIndexInside(index : Int, startIndex : Int, endIndex : Int) : Boolean = {
    if (index >= startIndex && (endIndex == -1 || index <= endIndex))
      true
    else
      false
  }

  def isIndexOutside(index : Int, endIndex : Int) : Boolean = {
    if(endIndex != -1 && index > endIndex)
      true
    else
      false
  }

  /**
   * readData
   */
  def readData(filePath : String,
               sheetName : String,
               startDataRow : Int,
               endDataRow : Int,
               startColIdx : Int,
               endColIdx : Int,
               schema : StructType,
               requiredColumns: Array[String] ): Array[Array[Any]] = {

    assert(schema.size == requiredColumns.length)
    val schemaNamesIdxMap = schema.map(_.name).zipWithIndex.toMap
    val colIdxMap = requiredColumns.map(colName => schemaNamesIdxMap(colName)).zipWithIndex.toMap

    val data = collection.mutable.ArrayBuffer.empty[Array[Any]]

    val is = openFile(filePath)
    val workbook = StreamingReader.builder()
      .rowCacheSize(1000)
      .bufferSize(4096)
      .open(is)

    try {
      val sheet = if (sheetName.trim.isEmpty) workbook.getSheetAt(0) else workbook.getSheet(sheetName)
      breakable {
        for ( row <- sheet.asScala) {
          val rowIdx = row.getRowNum + 1
          if(isIndexInside(rowIdx, startDataRow, endDataRow)) {
            data += getDataRow(row, startColIdx, endColIdx, schema, colIdxMap)
          }
          else if (isIndexOutside(rowIdx, endDataRow)) {
            break
          }
        }
      }
      data.toArray
    }
    finally {
      if (is != null) is.close()
      if(workbook != null) workbook.close()
    }
  }



  /**
   * getRow
   */
  def getRow( row : Row,
              startColIdx : Int,
              endColIdx : Int): List[String] = {

   val header = collection.mutable.ListBuffer.empty[String]
    breakable {
      for (cell <- row.asScala) {
        val idx = cell.getColumnIndex + 1
        if(isIndexInside(idx, startColIdx, endColIdx)) {
          header += CellConversion.toStringVal(cell)
        }
        else if(isIndexOutside(idx, endColIdx)) {
          break
        }
      }
    }
    header.toList
  }

  /**
   *
   * getDataRow
   */
  def getDataRow( row : Row,
                  startColIdx : Int,
                  endColIdx : Int,
                  schema : StructType,
                  colIdxMap : Map[Int, Int]): Array[Any] = {

    val numberOfCols = endColIdx - startColIdx + 1
    val rowData = new Array[Any](numberOfCols)
    breakable {
      for(cell <- row.asScala) {
        val colIdx = cell.getColumnIndex+1

        if (isIndexInside(colIdx, startColIdx, endColIdx)) {
          val offsetIdx = colIdx - startColIdx
          val colShuffleIdx = colIdxMap(offsetIdx)
          rowData(colShuffleIdx) = CellConversion.castTo(cell, schema(offsetIdx).dataType)
        }
        else if (isIndexOutside(colIdx, endColIdx)) {
          break
        }
      }
    }
    rowData
  }
}

