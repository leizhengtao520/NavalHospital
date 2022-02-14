package com.hos.process

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.types.DoubleType
object ProcessApp extends App {
  override def main(args: Array[String]): Unit = {
    println("-------------------------任务开始-------------------------")
    val sparkConf = new SparkConf().setAppName("Spark").setMaster("local").set("spark.testing.memory", "2147480000")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val path ="D:\\project\\奖金数据"
    var month_sum = spark.read.format("com.crealytics.spark.excel")
        .option("useHeader", "true") // 是否将第一行作为表头
        .option("inferSchema", "false") // 是否推断schema
        .option("workbookPassword", "None") // excel文件的打开密码
        .load(path+"\\绩效奖励.xlsx") //excel文件路径 + 文件名
    month_sum=month_sum.drop("_c4")
    month_sum=month_sum.withColumn("id",monotonically_increasing_id())
    val files:Array[File] = getFile(new File(path+"\\分表"));//这里获取的所有和科室相关的数据

    for(file<-files){
      println(s"正在处理 "+file.getName)
      val name = file.getName.split("\\.")(0)
      var tmp_room = spark.read.format("com.crealytics.spark.excel")
        .option("useHeader", "true") // 是否将第一行作为表头
        .option("inferSchema", "false") // 是否推断schema
        .option("workbookPassword", "None") // excel文件的打开密码
        .load(path + "\\分表\\" + file.getName) //excel文件路径 + 文件名
      tmp_room=tmp_room.drop("_c3")
      month_sum = month_sum.join(tmp_room,month_sum("储蓄账号")===tmp_room("储蓄账号"),"left")

      /**
       * 在这里判断分表卡号正确 但是分表中姓名和母表姓名不同做出提示
       */

      month_sum=month_sum.drop(tmp_room("储蓄账号"))
      month_sum=month_sum.drop(tmp_room("姓名"))
      month_sum=month_sum.withColumnRenamed("金额",name)
      month_sum = month_sum.na.fill("0")
      import DFHelper._
      month_sum=castColumnTo(month_sum,name,DoubleType)
//      if(i>95){
//        i=100
//      }
//      printSchedule(i)
//      i=i+(100/length)

    }
    println()
    month_sum=month_sum.orderBy("id")
    month_sum=month_sum.drop("id")
    judgeFile(path+"\\"+timeName()+".xlsx")
    month_sum.write.format("com.crealytics.spark.excel")
      .option("useHeader", "true")
      .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")
      .option("inferSchema", "false")
      .option("workbookPassword", "None")
      .save(path+"\\"+timeName()+".xlsx") //落地文件路径，需提前创建路径
    println("-------------------------任务结束-------------------------")
    deleteFile(new File(path))
    spark.stop()

  }


  /**
   * 读取科室A B C ... 所有和科室相关的数据
   */
  def getFile(file:File):Array[File]={
    file.listFiles().filter(! _.isDirectory).filter(file=> file.getName.endsWith("xls")||file.getName.endsWith("xlsx")).filter(! _.getName.startsWith("绩效奖励")).filter(! _.getName.startsWith("2020年"))
  }

  def deleteFile(file:File): Unit ={
    file.listFiles().filter(file=>file.getName.endsWith("crc")).map(_.delete())
  }
  /**
   *
   * 判断文件是否存在，若存在先删除
   */
  def judgeFile(path:String){
    val file = new File(path)
    if(file.isFile){
      file.delete()
    }
  }

  def timeName():String={
    val df = new SimpleDateFormat("yyyy年MM月dd日HH时mm分ss秒");//设置日期格式
    df.format(new Date())
  }

  /**
   * 进度条显示
   * @param percent
   */

  def printSchedule(percent:Int): Unit ={
    val TOTLE_LENGTH=30
    for (i<- 0 to TOTLE_LENGTH+10) {
            print("\b");
    }
    val now:Int = TOTLE_LENGTH*percent/100
    for(j<-0 to now){
      print(">")
    }
    for(k<-0 to TOTLE_LENGTH - now){
      print(" ")
    }
    print("  " + percent + "%")
  }


}
