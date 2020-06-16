# Spark Excel Reader
A simple yet powerful spark excel reading library to read excel files into Spark DataFrames.
It can read huge excel files as it is reading excel with steaming reader.

![Scala CI](https://github.com/kpmatta/spark-excel/workflows/Scala%20CI/badge.svg?branch=master)

# Options

- headerIndex           : optional, default is 1
- startDataRowIndex     : optional, default is headerIndex + 1    
- endDataRowIndex       : optional, default is -1
- startColIndex         : optional, default is 1
- endColIndex           : optional, default is -1
- sheetName             : optional, default to first sheet in the excel
- inferSchema           : optional, If no schema provided, infers column type from the data. Ignores this option if schema provided.
- timestampFormat       : optional


# Example:

Creating spark dataframe from the excel
```scala
import org.apache.spark.sql.types._
import org.apache.spark.sql._

   val schema = StructType(List(
      StructField("Id", IntegerType, true),
      StructField("Name", StringType, true),
      StructField("City", StringType, true),
      StructField("Date", DateType, true),
      StructField("Value1", DecimalType(16, 4), true),
      StructField("Calc1", StringType, true)
    ))

    val spark = SparkSession.builder().getOrCreate()
    val filePath = System.getProperty("user.dir") + "/TestFiles/Sample.xlsx"

    val df = spark.read
      .format("com.xorbit.spark.excel")
      .option("sheetName", "")              // Sheet name, default to first sheet.
      .option("headerIndex", 3)             // One based Header row index
      .option("startDataRowIndex", 2)       // One based Starting Data row index
      .option("endDataRowIndex", 100)       // One based End Data row index
      .option("startColIndex", 1)           // One based Start column index
      .option("endColIndex", -1)            // One based End column index
      .option("inferSchema", true)          // Infer Schema
      .schema(schema)                       // Schema
      .load(filePath)                       // Excel file location

    println(df.count())
    df.printSchema()
    df.show()
```

## Building the Source
Using SBT build tool with scala 2.12
- sbt assembly
