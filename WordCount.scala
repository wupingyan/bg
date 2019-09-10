package mydemo

import org.apache.spark.{SparkConf, SparkContext}


//使用Scala程序实现WordCount
object WordCount {

  //定义主方法
  def main(args: Array[String]): Unit = {
    //创建SparkConf对象
    val conf = new SparkConf().setAppName("My Scala WordCount")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    //使用sc对象执行相应的算子
    sc.textFile(args(0))
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .saveAsTextFile(args(1))

    //停止sc
    sc.stop()
  }
}
