package sample

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.concat_ws
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

object Main {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Odd Occurrence Parquet")
    .master("local[*]")
    .getOrCreate()
  
  val VALUE_FIELD: String = "value"
  val KEY_FIELD: String = "key"

  def main(args: Array[String]): Unit = {
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)
    val inputPath = args(0)
    val outputPath = args(1)

    val combinedDF: DataFrame = readDatasetsFromPath(inputPath)
    val transformedDF: DataFrame = transformDF(combinedDF)

    if (isValidateDF(transformedDF)) {
      val dfToSave = prepareToSave(transformedDF)
      writeOutput(dfToSave, outputPath)
    } else {
      throw new Exception("Invalid dataset, please check it.")
    }

    spark.stop()
  }

  private def writeOutput(transformedDF: DataFrame, outputPath: String): Unit =
    transformedDF
      .select(concat_ws("\t", transformedDF(KEY_FIELD), transformedDF(VALUE_FIELD)))
      .write
      .option("header", "true")
      .option("delimiter", "\t")
      .text(s"$outputPath/output_${System.currentTimeMillis}.tsv")

  def isValidateDF(df: DataFrame): Boolean =
    df
      .groupBy(KEY_FIELD, VALUE_FIELD)
      .agg(count(VALUE_FIELD).cast(IntegerType).as("count"))
      .filter("count % 2 != 0")
      .groupBy(KEY_FIELD).agg(count(KEY_FIELD).cast(IntegerType).as("count"))
      .filter(col("count") > 1)
      .count() == 0

  def transformDF(df: DataFrame): DataFrame = df
      .groupBy(KEY_FIELD, VALUE_FIELD)
      .agg(count(VALUE_FIELD).cast(IntegerType).as("count"))
      .filter("count % 2 != 0")

  def prepareToSave(df: DataFrame): DataFrame =
    df.select(KEY_FIELD, VALUE_FIELD).orderBy(KEY_FIELD)

  def combineFiles(
    fs: FileSystem = FileSystem.get(new Configuration()),
    df: DataFrame,
    path: Path
  ): DataFrame = {
    fs.listStatus(path).foldLeft(df) { (df, file) =>
      file.getPath.toString match {
        case filePath if filePath.endsWith(".csv") =>
          df.union(spark.read.option("header", "true").csv(filePath))
        case filePath if filePath.endsWith(".tsv") =>
          df.union(spark.read.option("sep", "\t").option("header", "true").csv(filePath))
        case _ =>
          combineFiles(fs, df, file.getPath)
      }
    }
  }

  def readDatasetsFromPath(inputDir: String): DataFrame = {
    val schema = StructType(Seq(
      StructField(KEY_FIELD, StringType, nullable = true),
      StructField(VALUE_FIELD, StringType, nullable = true)
    ))

    val emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    val combinedDF = combineFiles(df = emptyDF, path = new Path(inputDir))
    combinedDF.na.fill("0")
  }
}
