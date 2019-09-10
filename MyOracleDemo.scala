package mydemo

import java.sql.{Connection, DriverManager}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object MyOracleDemo {
  val connection = () =>{
    Class.forName("oracle.jdbc.OracleDriver").newInstance()
    DriverManager.getConnection("jdbc:oracle:thin:@192.168.88.101:1521/orcl.example.com", "scott", "tiger")
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","D:\\vidoes")

    //创建spacrkconf对象
    val conf = new SparkConf().setAppName("My WebCount Demo").setMaster("local")

    //创建sparkcontext
    val sc = new SparkContext(conf)

    val oracleRDD = new JdbcRDD(sc,connection,"select * from emp where sal>? and deptno=?",2000,10,2,r =>{
      val ename = r.getString(2)
     val sal = r.getInt(6)
      (ename,sal)
    })

    val result = oracleRDD.collect()
    println(result.toBuffer)
    sc.stop()
  }
}
