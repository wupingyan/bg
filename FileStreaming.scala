package demo

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FileStreaming {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("FileStreaming")
    val ssc = new StreamingContext(conf,Seconds(2))

    //从本地目录中读取数据：如果有新文件产生，就会读取进来
    val lines = ssc.textFileStream("d:\\dowload\\spark123")

    //打印结果
    lines.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
