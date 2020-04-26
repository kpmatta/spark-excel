package com.xorbit.spark.excel

import java.io.{File, FileInputStream}

import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.poi.ss.usermodel.{Cell, CellType, DataFormatter, Row}
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import scala.util.control.Breaks._

object ReadExcel {

  def readFromHadoop(location: String): FSDataInputStream = {
    val spark = SparkSession.builder().getOrCreate()

    val path = new Path(location)
    FileSystem.get(path.toUri, spark.sparkContext.hadoopConfiguration).open(path)
  }

  val dataFormat = new DataFormatter()

  val numericValue: Cell => Double = (cell : Cell) => {
    cell.getCellType match {
      case CellType.NUMERIC => cell.getNumericCellValue
      case CellType.STRING => cell.getStringCellValue.toDouble
      case _ => dataFormat.formatCellValue(cell).toDouble
    }
  }

  val stringVal: Cell => String = (cell : Cell) => {
    cell.getCellType match {
      case CellType.NUMERIC => cell.getNumericCellValue.toString
      case CellType.STRING => cell.getStringCellValue
      case _ => dataFormat.formatCellValue(cell)
    }
  }

  val getVal: (Cell, DataType) => Any = (cell : Cell, dataType : sql.types.DataType) => {
    dataType match {
      case _ : IntegerType => numericValue(cell).toInt
      case _ : FloatType => numericValue(cell).toFloat
      case _ : DoubleType => numericValue(cell)
      case _ => stringVal(cell)
    }
  }

  def getRow( row : Row,
              startColIdx : Int,
              endColIdx : Int,
              schema : StructType,
              colIdxMap : Map[Int, Int]): Array[Any] = {

    val numberOfCols = endColIdx - startColIdx + 1
    val cellItr = row.cellIterator()
    val rowData = new Array[Any](numberOfCols)
    breakable {
      while (cellItr.hasNext) {
        val cell = cellItr.next()
        val colIdx = cell.getColumnIndex+1
        val offsetIdx = colIdx - startColIdx
        val colShuffleIdx = colIdxMap(offsetIdx)

        if (colIdx >= startColIdx && colIdx <= endColIdx) {
          rowData(colShuffleIdx) = getVal(cell, schema(offsetIdx).dataType)
        }
        else if (colIdx > endColIdx)
          break
      }
    }
    rowData
  }

  def readData(filePath : String,
               sheetName : String,
               startDataRow : Long,
               endDataRow : Long,
               startColIdx : Int,
               endColIdx : Int,
               schema : StructType,
               requiredColumns: Array[String] ): Array[Array[Any]] = {

    assert(schema.size == requiredColumns.size)
    val schemaNamesIdxMap = schema.map(_.name).zipWithIndex.toMap
    val colIdxMap = requiredColumns.map(colName => schemaNamesIdxMap(colName)).zipWithIndex.map(_.swap).toMap

    val data = collection.mutable.ArrayBuffer.empty[Array[Any]]

    val fs = readFromHadoop(filePath)
    val workbook = new XSSFWorkbook(fs)
    val sheet = if (sheetName.isEmpty)
      workbook.getSheetAt(0)
    else
      workbook.getSheet(sheetName)

    breakable {
      val rowItr = sheet.rowIterator()
      while (rowItr.hasNext) {
        val row = rowItr.next()
        val rowIdx = row.getRowNum + 1
        if (rowIdx >= startDataRow && rowIdx <= endDataRow) {
          data.append(getRow(row, startColIdx, endColIdx, schema, colIdxMap))
        }
        else if (rowIdx > endDataRow)
          break
      }
    }

    data.toArray
  }
}

