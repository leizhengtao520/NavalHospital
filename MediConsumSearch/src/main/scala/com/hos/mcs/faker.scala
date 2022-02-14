package com.hos.mcs

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object faker extends App {
  override def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Spark").setMaster("local").set("spark.testing.memory", "2147480000")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc:SparkContext = spark.sparkContext
    /**
     * 用两个dataframe join
     * 将查询的数据作为dataframe 存储的数据作为dataframe 去join
     */
    val search_file = "E:\\test\\searchFile.csv"
    val database1 = "F:\\tmp\\第11批耗材\\csv1\\医保医用耗材代码_"  //除去c03所有的数据
    val database2 = "F:\\tmp\\第11批耗材\\csv3\\"  //c03所有的数据


    val search_csv :DataFrame = spark.read.format("com.databricks.spark.csv")
      .option("encoding","utf8")
      .option("header","false")
      .option("inferSchema", true.toString)
      .load(search_file)

    /**
     * 将输入的20位或者27位代码做分类
     */
    //20位
    val rdd_20 = search_csv.rdd.filter(_.toString().length==20).map(line=>{
      val a = line.toString().subSequence(0,3).toString //c13
      (a,line,20)
    })
    if(rdd_20.count()>0){
      val result_rdd_20 = rdd_20.map(line=>{
        val tail = line._1
        val path =  database1+tail






      })







    }




    //27位
    val rdd_27 = search_csv.rdd.filter(_.toString().length==27)map(line=>{
      val a = line.toString().subSequence(0,3).toString
      (a,line,20)
    })







    val database_file="E:\\test\\databaseFile.csv"
    val database_csv :DataFrame = spark.read.format("com.databricks.spark.csv")
      .option("encoding","utf8")
      .option("header","false")
      .option("inferSchema", true.toString)
      .load(database_file)


    /**
     * 将小表作为广播变量，广播到每个节点
     */
    val rdd_search_data = search_csv.collect()
    val rdd_search_data_broadcast = sc.broadcast(rdd_search_data)
    println(rdd_search_data_broadcast.value(0).get(0))
    val rd = database_csv.rdd.map(x=>{

      println(x)
      x.get(1)
    })
    rd.foreach(x=>println(x))
  }

}
