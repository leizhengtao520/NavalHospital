import java.io.File

import com.hos.process.ProcessApp.getFile

object Test {
//  def main(args: Array[String]): Unit = {
//    val path = "D:\\奖金数据";
//    val file = new File(path)
//    val files:Array[File] = getFile(file)
//    for(file<-files){
//
//      println(file.getName.split("\\.")(0))
//    }
//
//
//
//  }
//  def getFile(file:File):Array[File]={
//    file.listFiles().filter(! _.isDirectory).filter(file=> file.getName.endsWith("xls")||file.getName.endsWith("xlsx")).filter(! _.getName.startsWith("绩效奖励")).filter(! _.getName.startsWith("2020年"))
//  }
  def main(args: Array[String]): Unit = {
      val s = "/upfile/2018/05/20180510162031_812.xls\n/upfile/2018/05/20180510162053_111.xls\n/upfile/2018/05/20180510161119_671.xls\n/upfile/2018/05/20180510155852_178.xls\n/upfile/2018/05/20180510155939_509.xls\n/upfile/2018/05/20180510160003_716.xls\n/upfile/2018/05/20180510160020_534.xls\n/upfile/2018/05/20180510160902_641.xls\n/upfile/2018/05/20180510160925_833.xls\n/upfile/2018/05/20180510160943_648.xls\n/upfile/2018/05/20180510161003_619.xls\n/upfile/2018/05/20180510161018_798.xls\n/upfile/2018/05/20180510161036_453.xls\n/upfile/2018/05/20180510161205_232.xls\n/upfile/2018/05/20180510161222_479.xls\n/upfile/2018/05/20180510162119_884.xls\n/upfile/2018/05/20180510162148_825.xls\n/upfile/2018/05/20180510162235_222.xls\n/upfile/2018/05/20180510162257_727.xls\n/upfile/2018/05/20180510162321_782.xls\n/upfile/2018/05/20180510162351_603.xls\n/upfile/2018/05/20180510162417_686.xls\n/upfile/2018/05/20180510162509_307.xls\n/upfile/2018/05/20180510162528_842.xls\n/upfile/2018/05/20180510162648_528.xls\n/upfile/2018/05/20180510162706_734.xls\n/upfile/2018/05/20180510162753_247.xls\n/upfile/2018/05/20180510162812_825.xls\n/upfile/2018/05/20180510162837_956.xls\n/upfile/2018/05/20180510162854_715.xls\n/upfile/2018/05/20180510162954_302.xls\n/upfile/2018/05/20180510163014_198.xls"
      val s1:Array[String]=s.split("\n")
      println("a")
}
}
