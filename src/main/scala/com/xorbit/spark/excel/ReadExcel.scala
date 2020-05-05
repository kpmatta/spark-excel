package com.xorbit.spark.excel

import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.poi.ss.usermodel.{Cell, CellType, DataFormatter, DateUtil, Row}
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import com.monitorjbl.xlsx.StreamingReader
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
                endColIndex : Int): Array[String] = {

    assert(headerRowIdx > 0 || headerRowIdx == -1, "headerIndex  is one based index, -1 for ignore header, default is 1")
    assert(startColIndex > 0, "startColIndex is one based index, default is 1" )
    assert(endColIndex > 0 || endColIndex == -1, "endColIndex is one based index, -1 for all columns, default is -1")

    if (headerRowIdx == -1 && endColIndex != -1) {
      Range(0, endColIndex - startColIndex+1).map(i => s"_C$i").toArray
    }
    else {
      val is = openFile(filePath)
      val workbook = StreamingReader.builder()
        .rowCacheSize(10)
        .bufferSize(4096)
        .open(is)

      try {
        val sheet = if (sheetName.trim.isEmpty) workbook.getSheetAt(0) else workbook.getSheet(sheetName)
        if(headerRowIdx == -1) {
          sheet.asScala.head.asScala.map( cell => s"${getVal(cell, StringType).toString}").toArray
        }
        else {
          var header = Array.empty[String]
          breakable {
            for (row <- sheet.asScala) {
              if (row.getRowNum + 1 == headerRowIdx) {
                header = getRow(row, startColIndex, endColIndex)
                break
              }
            }
          }
          header
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
            data.append(getRow(row, startColIdx, endColIdx, schema, colIdxMap))
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

  val dataFormat = new DataFormatter()

  val getNumericValue: Cell => Double = (cell : Cell) => {
    cell.getCellType match {
      case CellType.FORMULA =>
        cell.getCachedFormulaResultType match {
          case CellType.NUMERIC => cell.getNumericCellValue
          case CellType.STRING => cell.getRichStringCellValue.toString.toDouble
//          case CellType.BLANK => null
          case _ => dataFormat.formatCellValue(cell).toDouble
        }
      case CellType.NUMERIC => cell.getNumericCellValue
      case CellType.STRING => cell.getStringCellValue.toDouble
//      case CellType.BLANK => null
      case _ => dataFormat.formatCellValue(cell).toDouble
    }
  }

  val getStringVal: Cell => String = (cell : Cell) => {
    cell.getCellType match {
      case CellType.FORMULA =>
        cell.getCachedFormulaResultType match {
          case CellType.NUMERIC => cell.getNumericCellValue.toString
          case CellType.STRING => cell.getStringCellValue
//          case CellType.BLANK => null
          case _ => dataFormat.formatCellValue(cell)
        }

      case CellType.NUMERIC => cell.getNumericCellValue.toString
      case CellType.STRING => cell.getStringCellValue
//      case CellType.BLANK => null
      case _ => dataFormat.formatCellValue(cell)
    }
  }

  val getVal: (Cell, DataType) => Any = (cell : Cell, dataType : sql.types.DataType) => {
    if(cell.getCellType == CellType.BLANK)
      null
    else {
      try {
        dataType match {
          case _: ByteType => getNumericValue(cell).toByte
          case _: ShortType => getNumericValue(cell).toShort
          case _: IntegerType => getNumericValue(cell).toInt
          case _: LongType => getNumericValue(cell).toLong
          case _: FloatType => getNumericValue(cell).floatValue()
          case _: DoubleType => getNumericValue(cell)
          case _: BooleanType => getStringVal(cell).toBoolean
          case _: DecimalType => Decimal(getNumericValue(cell))
          case _: TimestampType => new java.sql.Timestamp(DateUtil.getJavaDate(getNumericValue(cell)).getTime)
          case _: DateType => new java.sql.Date(DateUtil.getJavaDate(getNumericValue(cell)).getTime)
          case _: StringType => getStringVal(cell)
          case _ => throw new RuntimeException(s"Unsupported type: ${dataType.toString}")
        }
      }
      catch {
        case ex: Exception => println(ex.toString)
        null
      }
    }
  }

  /**
   * getRow
   */
  def getRow( row : Row,
              startColIdx : Int,
              endColIdx : Int): Array[String] = {

   val header = collection.mutable.ArrayBuffer.empty[String]
    breakable {
      for (cell <- row.asScala) {
        val idx = cell.getColumnIndex + 1
        if(isIndexInside(idx, startColIdx, endColIdx)) {
          header += getVal(cell, StringType).toString
        }
        else if(isIndexOutside(idx, endColIdx)) {
          break
        }
      }
    }
    header.toArray
  }

  /**
   *
   * getRow
   */
  def getRow( row : Row,
              startColIdx : Int,
              endColIdx : Int,
              schema : StructType,
              colIdxMap : Map[Int, Int]): Array[Any] = {

    val numberOfCols = endColIdx - startColIdx + 1
    val rowData = new Array[Any](numberOfCols)
    breakable {
      for(cell <- row.asScala) {
        val colIdx = cell.getColumnIndex+1
        val offsetIdx = colIdx - startColIdx
        val colShuffleIdx = colIdxMap(offsetIdx)

        if (isIndexInside(colIdx, startColIdx, endColIdx)) {
          rowData(colShuffleIdx) = getVal(cell, schema(offsetIdx).dataType)
        }
        else if (isIndexOutside(colIdx, endColIdx)) {
          break
        }
      }
    }
    rowData
  }
}

