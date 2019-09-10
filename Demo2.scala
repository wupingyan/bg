package demo

import org.apache.spark.sql.SparkSession

//使用case class
object Demo2 {

  def main(args: Array[String]): Unit = {
    //创建SparkSession
    val spark = SparkSession.builder().master("local").appName("My Demo 1").getOrCreate()

    //从指定的文件中读取数据，生成对应的RDD
    val lineRDD = spark.sparkContext.textFile("d:\\temp\\student.txt").map(_.split(" "))

    //将RDD和case class 关联
    val studentRDD = lineRDD.map( x => Student(x(0).toInt,x(1),x(2).toInt))

    //生成 DataFrame，通过RDD 生成DF,导入隐式转换
    import spark.sqlContext.implicits._
    val studentDF = studentRDD.toDF

    //注册表 视图
    studentDF.createOrReplaceTempView("student")

    //执行SQL
    spark.sql("select * from student").show()

    spark.stop()
  }
}

//case class 一定放在外面
case class Student(stuID:Int,stuName:String,stuAge:Int)
