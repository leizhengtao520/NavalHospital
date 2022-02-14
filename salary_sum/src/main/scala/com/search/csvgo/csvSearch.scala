package com.search.csvgo

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.Source

object csvSearch extends App{
  override def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Spark").setMaster("local").set("spark.testing.memory", "2147480000")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()







    val path = "F:\\tmp\\第11批耗材\\csv1\\医保医用耗材代码_C14_心脏外科类材料_单件及规格型号信息.xlsx.csv"
    println("start read")
    val csvRdd : DataFrame = spark.read.format("com.databricks.spark.csv")
      .option("encoding","utf8")
      .option("header","false")
      .option("inferSchema", true.toString)
      .load(path)
    println("end read")
//    println("start write--------------------")
//    csvRdd.repartition(1).write
//      .option("header","true")
//      .csv("F:\\tmp\\第11批耗材\\csv3\\all")
//    println(s"end")
    csvRdd.show(10,false)
    //c12是医保耗材代码
    csvRdd.filter("_c12='C0502011270000104254'").show()

  }

}
