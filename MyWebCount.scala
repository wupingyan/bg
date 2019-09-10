package mydemo

import org.apache.spark.{SparkConf, SparkContext}

//取出点击量前两名的网页
object MyWebCount {

  def main(args: Array[String]): Unit = {
    //定义conf对象
    val conf = new SparkConf().setAppName("My Web Count Example").setMaster("local")
    //创建SparkContext对象
    val sc = new SparkContext(conf)

    //读入数据，并切分数据:
    // 192.168.88.1 - - [30/Jul/2017:12:54:38 +0800] "GET /MyDemoWeb/hadoop.jsp HTTP/1.1" 200 242
    val rdd1 = sc.textFile("D:\\vidoes\\localhost_access_log.2017-07-30.txt").map(
      line => {
        //得到 .jsp的位置
        val index1 = line.indexOf("\"")
        val index2 = line.lastIndexOf("\"")

        //得到子串 :  GET /MyDemoWeb/hadoop.jsp HTTP/1.1
        val line1 = line.substring(index1+1,index2)
        val index3 = line1.indexOf(" ")
        val index4 = line1.lastIndexOf(" ")

        //得到子串 :  /MyDemoWeb/hadoop.jsp
        val line2 = line1.substring(index3+1,index4)

        //得到具体的网页
        val line3 = line2.substring(line2.lastIndexOf("/")+1)

        //返回一个元组
        (line3,1)
      }
    )

    //按照Key进行聚合操作
    val rdd2 = rdd1.reduceByKey(_+_)

    //按照value进行排序
    val rdd3 = rdd2.sortBy(_._2,false)

    //输出
    println(rdd3.take(2).toBuffer)
    sc.stop()
  }
}
