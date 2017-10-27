package Task2

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SparkSQL {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession
      .builder()
      .appName("spark-sql-basic")
      .master("local[*]")
      .getOrCreate()

    val jsonPath = "data sources/task 2/sampletweets.json"

    val tweets = sparkSession.read.json(jsonPath)

    tweets.printSchema()
  }
}