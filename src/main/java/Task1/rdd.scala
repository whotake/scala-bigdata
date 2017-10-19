package Task1

import org.apache.spark.{SparkConf, SparkContext}

object Rdd {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()

    conf.setAppName("RDD")
    conf.setMaster("local[2]")

    val sc = new SparkContext(conf)
    val text_file = sc.textFile("data sources/task 1/nums.txt")

    //Task 1
    val int_data = text_file.map(x => x.split(" ").toList.map(x => x.toInt))

    val sum = int_data.map(x => x.sum)
    val min = int_data.map(x => x.min)
    val max = int_data.map(x => x.max)
    val multiples_five = int_data.map(x => x.filter(x => x % 5 == 0).sum)
    val distinct = int_data.map(x => x.distinct)

    //Task 2

    val flat_int_data = int_data.flatMap(x => x)

    val flat_sum = flat_int_data.reduce(_ + _)
    val flat_mult = flat_int_data.filter(x => x % 5 ==0).sum
    val flat_min = flat_int_data.min
    val flat_max = flat_int_data.max
    val flat_distinct = flat_int_data.distinct

  }
}