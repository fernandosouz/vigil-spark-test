package sample

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.count
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType

object Main {
  val spark = SparkSession
    .builder()
    .appName("Odd Occurrence Parquet")
    .master("local[*]")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    val inputDir = args(0)

    val combinedDF: DataFrame = readDatasetsFromPath(inputDir)
    val transformedDF: DataFrame = transformDF(combinedDF)
    transformedDF.write

    transformedDF.show()
    spark.stop()
  }

  def transformDF(df: DataFrame): DataFrame =
    df
      .groupBy("key", "value")
      .agg(count("value").cast(IntegerType).as("count"))
      .filter("count % 2 != 0")
      .select("key", "value")
      .orderBy("key")

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

  def readDatasetsFromPath(inputDir: String) = {
    val schema = StructType(Seq(
      StructField("key", StringType, nullable = true),
      StructField("value", StringType, nullable = true)
    ))

    val emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    val combinedDF = combineFiles(df = emptyDF, path = new Path(inputDir))
    combinedDF.na.fill("0")
  }
}
