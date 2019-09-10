package sparksql

import org.apache.spark.sql.{Row, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

object SpecifyingSchema {
  def main(args: Array[String]) {
      //创建Spark Session对象
    val spark = SparkSession.builder().master("local").appName("UnderstandingSparkSession").getOrCreate()

    //从指定的地址创建RDD
    val personRDD = spark.sparkContext.textFile("D:\\temp\\student.txt").map(_.split(" "))

    //通过StructType直接指定每个字段的schema
    val schema = StructType(
      List(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("age", IntegerType, true)
      )
    )
    //将RDD映射到rowRDD
    val rowRDD = personRDD.map(p => Row(p(0).toInt, p(1).trim, p(2).toInt))

    //将schema信息应用到rowRDD上
    val personDataFrame = spark.createDataFrame(rowRDD, schema)

    //注册表
    personDataFrame.createOrReplaceTempView("t_person")

    //执行SQL
    val df = spark.sql("select * from t_person order by age desc limit 4")

    //显示结果
    df.show()

    //停止Spark Context
    spark.stop()
  }
}