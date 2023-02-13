import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import sample.Main

class MainTest extends AnyFunSuite with Matchers {
  val spark = SparkSession
    .builder()
    .appName("TransformDFTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  test("should return the correct result for a small input DataFrame") {
    val inputDF = Seq(
      (1, 2),
      (1, 2),
      (1, 3),
      (2, 0),
      (2, 0),
      (2, 0)
    ).toDF("key", "value")

    val expectedDF = Seq(
      (1, 3),
      (2, 0)
    ).toDF("key", "value")

    val resultDF = Main.transformDF(inputDF)

    resultDF.orderBy("key", "value").collect() should contain theSameElementsAs expectedDF.orderBy("key", "value").collect()
  }

  test("should handle empty DataFrames correctly") {
    val inputDF = Seq.empty[(Int, Int)].toDF("key", "value")

    val resultDF = Main.transformDF(inputDF)

    resultDF.count() should be(0)
  }

  test("should handle DataFrames with no odd-count values") {
    val inputDF = Seq(
      (1, 2),
      (1, 2),
      (1, 3),
      (1, 3),
      (2, 4),
      (2, 4)
    ).toDF("key", "value")

    val resultDF = Main.transformDF(inputDF)

    resultDF.count() should be(0)
  }
}