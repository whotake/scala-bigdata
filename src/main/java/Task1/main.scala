package Task1

import org.apache.spark.{SparkConf, SparkContext}

object TestScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()

    conf.setAppName("Spark Hello World")
    conf.setMaster("local[2]")

    val sc = new SparkContext(conf)
    val data = sc.textFile("data sources/task 1/products.csv")

    val result = data.flatMap(item => item.split(" ")).map(item => (item, 1)).reduceByKey(_ + _)

    result.foreach(println)

  }
}