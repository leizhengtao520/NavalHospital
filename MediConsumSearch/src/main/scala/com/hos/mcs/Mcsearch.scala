package com.hos.mcs

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File
import scala.util.control.Breaks



object Mcsearch extends App {
  override def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Spark").setMaster("local").set("spark.testing.memory", "2147480000")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc: SparkContext = spark.sparkContext
    /**
     * 用两个dataframe join
     * 将查询的数据作为dataframe 存储的数据作为dataframe 去join
     */
    val search_file = "E:\\test\\searchFile.csv"
    val database1 = "F:\\tmp\\第11批耗材\\csv1\\医保医用耗材代码_"
//    val data_case ="E:\\case\\"
//    val file_data_case = new File(data_case)
//    deleteDir(file_data_case)






    val search_csv: DataFrame = spark.read.format("com.databricks.spark.csv")
      .option("encoding", "utf8")
      .option("header", "false")
      .option("inferSch'/l; l, ema", true.toString)
      .load(search_file)

    /**
     * 将输入的20位或者27位代码做分类
     */
    import spark.implicits._

    val rdd_key = search_csv.rdd.map(line => {
      val a = line.toString().subSequence(1, 4).toString //c03
      (a, line.toString()) //c03 C03160923400002043450000010
    })
    val rdd_distinct_to_rdd_20 = rdd_key.map(_._1).distinct().collect()   //c13 C03160923400002043450000010

    for(distinct_data<-rdd_distinct_to_rdd_20){

      val rdd_database = spark.read.format("com.databricks.spark.csv")
        .option("encoding", "utf8")
        .option("header", "true")
        .option("inferSchema", true.toString)
        .load(database1+distinct_data+".csv")



      val rdd_search_data = rdd_key.filter(_._1 == distinct_data)

      val broadcast_rdd_search = sc.broadcast(rdd_search_data.collect())
       rdd_database.rdd.filter(line=>{

        val broadcast_rdd_v = broadcast_rdd_search.value
        line_result(broadcast_rdd_v,line.toString())
      }).foreach(println(_))
    }
  }

  def line_result(broadcast_rdd:Array[(String,String)],row_line:String): Boolean ={
    val br_length = broadcast_rdd.length
    val row_l1 = row_line.split(",")(12)
    val row_l2 = row_line.split(",")(14).split("]")(0)
    var bool_result = false
    val loop = new Breaks;
    loop.breakable{
      for( i <- 0 until  br_length){
        val value = broadcast_rdd(i)._2.length-2
        val key = broadcast_rdd(i)._2.subSequence(1,value+1)
        if(value ==27 && row_l2==key){
          bool_result=true
          loop.break
        }else if(value==20 && row_l1==key){
          bool_result=true
          loop.break()
        }
      }

    }
    bool_result
  }
  /**
   * 删除一个文件夹,及其子目录
   *
   * @param dir
   */
  def deleteDir(dir: File): Unit = {
    val files = dir.listFiles()
    files.foreach(f => {
      if (f.isDirectory) {
        deleteDir(f)
      } else {
        f.delete()
        println("delete file " + f.getAbsolutePath)
      }
    })
    dir.delete()
    println("delete dir " + dir.getAbsolutePath)
  }
}

