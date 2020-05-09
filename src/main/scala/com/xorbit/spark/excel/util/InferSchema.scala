package com.xorbit.spark.excel.util

import com.monitorjbl.xlsx.StreamingReader
import com.xorbit.spark.excel.ReadExcel._

import scala.collection.JavaConverters._
import scala.util.control.Breaks.{break, breakable}
import org.apache.poi.ss.usermodel.{Cell, CellType, DateUtil, Row}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object InferSchema {

  def apply(filePath: String,
            sheetName: String,
            startRowIndex: Int,
            endRowIndex: Int,
            startColIndex: Int,
            endColIndex: Int): Array[DataType] = {

    val rddData = getDataCellsRDD(filePath, sheetName, startRowIndex, endRowIndex, startColIndex, endColIndex)
    val startType = Array.fill[DataType](endColIndex - startColIndex+1)(NullType)
    val finalTypes = rddData.aggregate(startType)(inferRowType, mergeRowType)

    finalTypes.map {
      case z: NullType => StringType
      case other => other
    }
  }

  def inferRowType(soFar: Array[DataType], next: Array[DataType]): Array[DataType] = {
    soFar.zip(next).map{ case(s,n) => inferField(s, n) }
  }

  def mergeRowType(first : Array[DataType], second : Array[DataType]): Array[DataType] = {
    first.zip(second).map{ case (f,s) => findTightestCommonType(f,s).getOrElse(NullType) }
  }

  def getDataCellsRDD(filePath: String,
                      sheetName: String,
                      startRowIndex: Int,
                      endRowIndex: Int,
                      startColIndex: Int,
                      endColIndex: Int): RDD[Array[DataType]] = {

    val data = collection.mutable.ArrayBuffer.empty[Array[DataType]]
    val is = openFile(filePath)
    val workbook = StreamingReader.builder()
      .rowCacheSize(1000)
      .bufferSize(4096)
      .open(is)

    try {
      val sheet = if (sheetName.trim.isEmpty) workbook.getSheetAt(0) else workbook.getSheet(sheetName)
      breakable {
        for (row <- sheet.asScala) {
          val rowIdx = row.getRowNum + 1
          if (isIndexInside(rowIdx, startRowIndex, endRowIndex)) {
            data.append(getRowCells(row, startColIndex, endColIndex))
          }
          else if (isIndexOutside(rowIdx, endRowIndex)) {
            break
          }
        }
      }
      data.toArray
    }
    finally {
      if (is != null) is.close()
      if (workbook != null) workbook.close()
    }

    val spark = SparkSession.builder().getOrCreate()
    spark.sparkContext.parallelize(data)
  }

  def getRowCells(row: Row,
                  startColIdx: Int,
                  endColIdx: Int): Array[DataType] = {

    val numberOfCols = endColIdx - startColIdx + 1
    val rowData = new Array[Cell](numberOfCols)
    breakable {
      for (cell <- row.asScala) {
        val colIdx = cell.getColumnIndex + 1
        if (isIndexInside(colIdx, startColIdx, endColIdx)) {
          rowData(cell.getColumnIndex) = cell
        }
        else if (isIndexOutside(colIdx, endColIdx)) {
          break
        }
      }
    }
    rowData.map(cellTypeToSparkType)
  }

  def cellTypeToSparkType(cell : Cell) : DataType = {
    if (cell == null)
      NullType
    else {
      cell.getCellType match {
        case CellType.FORMULA =>
          cell.getCachedFormulaResultType match {
            case CellType.STRING => StringType
            case CellType.NUMERIC => DoubleType
            case _ => NullType
          }

        case CellType.STRING => StringType
        case CellType.BOOLEAN => BooleanType
        case CellType.NUMERIC => if (DateUtil.isCellDateFormatted(cell)) TimestampType else DoubleType
        case _ => NullType
      }
    }
  }

  /**
   * Infer type of string field. Given known type Double, and a string "1", there is no
   * point checking if it is an Int, as the final type must be Double or higher.
   */
  private[excel] def inferField(typeSoFar: DataType, field: DataType): DataType = {
    // Defining a function to return the StringType constant is necessary in order to work around
    // a Scala compiler issue which leads to runtime incompatibilities with certain Spark versions;
    // see issue #128 for more details.
    def stringType(): DataType = {
      StringType
    }

    if (field == NullType) {
      typeSoFar
    } else {
      (typeSoFar, field) match {
        case (NullType, ct) => ct
        case (DoubleType, DoubleType) => DoubleType
        case (BooleanType, BooleanType) => BooleanType
        case (TimestampType, TimestampType) => TimestampType
        case (StringType, _) => stringType()
        case (_, _) => stringType()
      }
    }
  }

  /**
   * Copied from internal Spark api
   * org.apache.spark.sql.catalyst.analysis.HiveTypeCoercion
   */
  private val numericPrecedence: IndexedSeq[DataType] =
    IndexedSeq[DataType](ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, TimestampType)

  /**
   * Copied from internal Spark api
   * org.apache.spark.sql.catalyst.analysis.HiveTypeCoercion
   */
  val findTightestCommonType: (DataType, DataType) => Option[DataType] = {
    case (t1, t2) if t1 == t2 => Some(t1)
    case (NullType, t1) => Some(t1)
    case (t1, NullType) => Some(t1)
    case (StringType, t2) => Some(StringType)
    case (t1, StringType) => Some(StringType)

    // Promote numeric types to the highest of the two and all numeric types to unlimited decimal
    case (t1, t2) if Seq(t1, t2).forall(numericPrecedence.contains) =>
      val index = numericPrecedence.lastIndexWhere(t => t == t1 || t == t2)
      Some(numericPrecedence(index))

    case _ => None
  }
}
