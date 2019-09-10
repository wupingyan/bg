package demo

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MyNetworkWordCount {
  def main(args: Array[String]) {
    //创建一个本地的StreamingContext，并设置两个工作线程，批处理时间间隔 3 秒
    val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //创建一个DStream对象，并连接到netcat的服务器端
    val lines = ssc.socketTextStream("192.168.157.81", 1234, StorageLevel.MEMORY_AND_DISK_SER)

    //采集数据，并处理
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

    //打印结果
    wordCounts.print()

    //启动StreamingContext，开始计算
    ssc.start()

    //等待计算结束
    ssc.awaitTermination()
  }
}
