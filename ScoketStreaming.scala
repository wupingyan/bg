package demo
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ScoketStreaming {

  def main(args: Array[String]) {
    //创建一个本地的StreamingContext，含2个工作线程
    val conf = new SparkConf().setMaster("local[4]").setAppName("ScoketStreaming")
    val sc = new StreamingContext(conf,Seconds(5))   //每隔10秒统计一次字符总数

    //创建珍一个DStream，连接127.0.0.1 :7788
    val lines = sc.socketTextStream("127.0.0.1",7788)
    //打印数据
    lines.print()

    sc.start()         //开始计算
    sc.awaitTermination()   //通过手动终止计算，否则一直运行下去
  }
}
