package mydemo

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

object MyWebPartition {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D://vidoes")

    //定义conf对象
    val conf = new SparkConf().setAppName("My Web Count Example").setMaster("local")
    //创建SparkContext对象
    val sc = new SparkContext(conf)

    //读入数据，并切分数据:
    // 192.168.88.1 - - [30/Jul/2017:12:54:38 +0800] "GET /MyDemoWeb/hadoop.jsp HTTP/1.1" 200 242
    val rdd1 = sc.textFile("D://vidoes//localhost_access_log.2017-07-30.txt").map(
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
        (line3,line)
      }
    )

    //得到所有的key: 就是网页的名字
    val rdd2 = rdd1.map(_._1).distinct().collect()

    //创建分区器
    val myPartitioner =  new MyPartitioner(rdd2)

    //根据访问的jsp文件的名字进行分区
    val rdd3 = rdd1.partitionBy(myPartitioner)

    //输出
    rdd3.saveAsTextFile("d://out1")
    //println(rdd3.collect.toBuffer)
    sc.stop()

  }
}

////定义自己的分区器
class MyPartitioner( allJSPNames :Array[String]) extends Partitioner{
  //定义Map集合保存分区的条件
  val partitionMap = new mutable.HashMap[String,Int]()

  var PartID = 0;
  for(name <- allJSPNames){
    partitionMap.put(name,PartID)
    PartID += 1
  }

  override def numPartitions: Int = partitionMap.size

  override def getPartition(key: Any): Int = {
    // 根据jsp的名字返回对应的分区
    partitionMap.getOrElse(key.toString,0)
  }
}


















