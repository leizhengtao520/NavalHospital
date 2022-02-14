package com.hos.datacheck

import java.io.File
import com.hos.process.ProcessApp.getFile
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CheckData extends App {
  override def main(args: Array[String]): Unit = {
    println("检查任务开始")
    val sparkConf = new SparkConf().setAppName("Spark").setMaster("local").set("spark.testing.memory", "2147480000")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val path ="D:\\project\\奖金数据"
    val files:Array[File] = getFile(new File(path+ "\\分表\\"));//这里获取的所有和科室相关的数据
    val  month_sum = spark.read.format("com.crealytics.spark.excel")
      .option("useHeader", "true") // 是否将第一行作为表头
      .option("inferSchema", "false") // 是否推断schema
      .option("workbookPassword", "None") // excel文件的打开密码
      .load(path+"\\绩效奖励.xlsx") //excel文件路径 + 文件名
    for (file <- files){
      println("正在检查 "+file.getName)
      var tmp_room = spark.read.format("com.crealytics.spark.excel")
        .option("useHeader", "true") // 是否将第一行作为表头
        .option("inferSchema", "false") // 是否推断schema
        .option("workbookPassword", "None") // excel文件的打开密码
        .load(path + "\\分表\\" + file.getName) //excel文件路径 + 文件名
      tmp_room=tmp_room.drop("_c3")

      /**
       * 分表卡号正确，分表姓名但与母表姓名不同
       */
      val tmp_person_name = tmp_room.join(month_sum,tmp_room("储蓄账号")===month_sum("储蓄账号"),"left")
      val rdd_rome_name = tmp_person_name.select(tmp_room("姓名")).rdd
      val rdd_month_name = tmp_person_name.select(month_sum("姓名")).rdd
      for(r1<-tmp_person_name){
        if(r1(0)!=r1(5)){
          println("********************************************************")
          println("分表卡号正确，分表姓名但与母表姓名不同，显示如下")
          println(r1)
        }
      }
      val rdd_name_substract= rdd_rome_name.subtract(rdd_month_name) //分表中有但是母表中没有的姓名
      val d = tmp_person_name.collect()
      val a =rdd_rome_name.collect()
      val b =rdd_month_name.collect()
      val c =rdd_name_substract.collect()
      if(rdd_name_substract.count()>0){
          println("********************************************************")
          println(file.getName+"中储蓄卡号正确，但是该科室的姓名与绩效奖励中的姓名不一致，具体在该科室中的姓名如下")
          rdd_name_substract.collect().foreach(println)
          println("********************************************************")
      }


      /**
         * 检查分表中存在但是母表中不存在的账号
       */
      val rdd0=  month_sum.select("储蓄账号").rdd
      val rdd3 = tmp_room.select("储蓄账号").rdd
      val rdd1 = rdd3.subtract(rdd0)
      if(rdd1.count()>0){
        println("---------------------------------------------------------")
        println(file.getName+"中存在有一些储蓄账号在绩效奖励.xlxs中没有或者没有填上,显示如下：")
        rdd1.collect().foreach {println}
        println("---------------------------------------------------------")
      }

      /**
       * 分表内有姓名，但卡号未填
       */
      val rdd2 = tmp_room.select("姓名","储蓄账号").rdd

      rdd2.filter(x=>(x(0)!=null && x(1)==null)).collect().map(x=>{
        println("===========================================================")
        println(file.getName+"中有的只填姓名，但储蓄账号未填,姓名如下：")
        println(x(0))
        println("===========================================================")
        null
      })
      /**
       * 分表内有卡号，但姓名未填
       */
      rdd2.filter(x=>(x(1)!=null && x(0)==null)).collect().map(x=>{
        println("===========================================================")
        println(file.getName+"中有的只填储蓄账号，但姓名未填,储蓄账号如下：")
        println(x(0))
        println("===========================================================")
        null
      })
      val no_distinct = tmp_room.select("储蓄账号").orderBy(tmp_room("储蓄账号"))
      val rdd_no_distinct = no_distinct.rdd //去重之前
      val rdd_distinct = rdd_no_distinct.distinct() //去重之后
      if(rdd_distinct.count() != rdd_no_distinct.count()){
        println("###########################################################")
        println(file.getName+"中存在重复储蓄账号，重复储蓄账号和重复次数显示如下")
        rdd_no_distinct.map(x=>(x,1)).reduceByKey(_+_).filter(_._2>1).collect().foreach(println)
        println("###########################################################")
      }
      
      /**
       * 检查分表的金额不能小于0
       */
      val money_rdd = tmp_room.select("金额").rdd
      money_rdd.filter(x=>x(0).toString().toDouble<0).collect().map(x=>{
        println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
        println(file.getName+"存在金额小于0,其具体金额为"+x)
        println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
      })
    }
    println()
    println("检查任务完成")
    spark.stop()
  }

}
