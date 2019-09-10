package demo

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MyFlumeStream {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkFlumeNGWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))

    //创建FlumeEvent的DStream
    val flumeEvent = FlumeUtils.createStream(ssc,"192.168.157.1",1234)

    //将FlumeEvent中的事件转成字符串
    val lineDStream = flumeEvent.map( e => {
       new String(e.event.getBody.array)
    })

    //输出结果
    lineDStream.print()

    ssc.start()
    ssc.awaitTermination();
  }
}

