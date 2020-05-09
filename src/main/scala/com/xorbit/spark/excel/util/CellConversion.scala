package com.xorbit.spark.excel.util

import java.text.SimpleDateFormat

import org.apache.poi.ss.usermodel.{Cell, CellType, DataFormatter, DateUtil}
import org.apache.spark.sql
import org.apache.spark.sql.types._

object CellConversion {
  val dateFormat = "yyyy-MM-dd"
  val timestampFormat = "yyyy-MM-dd HH:mm:ss.SSS"

  val dataFormat = new DataFormatter()

  def toNumericValue (cell : Cell): Double = {
    cell.getCellType match {
      case CellType.FORMULA =>
        cell.getCachedFormulaResultType match {
          case CellType.NUMERIC => cell.getNumericCellValue
          case CellType.STRING => cell.getRichStringCellValue.toString.toDouble
           case _ => dataFormat.formatCellValue(cell).toDouble
        }
      case CellType.NUMERIC => cell.getNumericCellValue
      case CellType.STRING => cell.getStringCellValue.toDouble
      case _ => dataFormat.formatCellValue(cell).toDouble
    }
  }

  def toStringVal (cell : Cell): String = {
    cell.getCellType match {
      case CellType.FORMULA =>
        cell.getCachedFormulaResultType match {
          case CellType.NUMERIC => cell.getNumericCellValue.toString
          case CellType.STRING => cell.getStringCellValue
          case CellType.BLANK => null
          case _ => dataFormat.formatCellValue(cell)
        }

      case CellType.NUMERIC => cell.getNumericCellValue.toString
      case CellType.STRING => cell.getStringCellValue
      case CellType.BLANK => null
      case _ => dataFormat.formatCellValue(cell)
    }
  }

  def toBooleanVal(cell : Cell): Boolean = {
    cell.getCellType match {
      case CellType.BOOLEAN => cell.getBooleanCellValue
      case CellType.BLANK => assert(assertion = false, "Should be handled before calling this method"); false
      case _ => dataFormat.formatCellValue(cell).toBoolean
    }
  }

  def numberToDate(value : Double) : java.sql.Date= {
    new java.sql.Date(DateUtil.getJavaDate(value).getTime)
  }

  def stringToDate(value : String): java.sql.Date = {
    new java.sql.Date(new SimpleDateFormat(dateFormat).parse(value).getTime)
  }

  def numberToTimeStamp(value : Double) : java.sql.Timestamp = {
    new java.sql.Timestamp(DateUtil.getJavaDate(value).getTime)
  }

  def stringToTimeStamp(value : String) : java.sql.Timestamp = {
    new java.sql.Timestamp(new SimpleDateFormat(timestampFormat).parse(value).getTime)
  }

  def toDate(cell : Cell) : java.sql.Date = {
    cell.getCellType match {
      case CellType.FORMULA =>
        cell.getCachedFormulaResultType match {
          case CellType.NUMERIC => numberToDate(cell.getNumericCellValue)
          case CellType.STRING => stringToDate(cell.getStringCellValue)
          case _ => stringToDate(dataFormat.formatCellValue(cell))
        }

      case CellType.NUMERIC => numberToDate(cell.getNumericCellValue)
      case CellType.STRING => stringToDate(cell.getStringCellValue)
      case _ => stringToDate(dataFormat.formatCellValue(cell))
    }
  }

  def toTimeStamp(cell : Cell) : java.sql.Timestamp = {
    cell.getCellType match {
      case CellType.FORMULA =>
        cell.getCachedFormulaResultType match {
          case CellType.NUMERIC => numberToTimeStamp(cell.getNumericCellValue)
          case CellType.STRING => stringToTimeStamp(cell.getStringCellValue)
          case _ => stringToTimeStamp(dataFormat.formatCellValue(cell))
        }

      case CellType.NUMERIC => numberToTimeStamp(cell.getNumericCellValue)
      case CellType.STRING => stringToTimeStamp(cell.getStringCellValue)
      case _ => stringToTimeStamp(dataFormat.formatCellValue(cell))
    }
  }

  def castTo(cell : Cell, dataType : sql.types.DataType): Any = {
    if(cell.getCellType == CellType.BLANK)
      null
    else {
      try {
        dataType match {
          case _: ByteType => toNumericValue(cell).toByte
          case _: ShortType => toNumericValue(cell).toShort
          case _: IntegerType => toNumericValue(cell).toInt
          case _: LongType => toNumericValue(cell).toLong
          case _: FloatType => toNumericValue(cell).floatValue()
          case _: DoubleType => toNumericValue(cell)
          case _: BooleanType => toBooleanVal(cell)
          case _: DecimalType => BigDecimal(toNumericValue(cell))
          case _: TimestampType => toTimeStamp(cell)
          case _: DateType => toDate(cell)
          case _: StringType => toStringVal(cell)
          case _ => throw new RuntimeException(s"Unsupported type: ${dataType.toString}")
        }
      }
      catch {
        case ex: Exception => println(ex.toString)
          null
      }
    }
  }
}
