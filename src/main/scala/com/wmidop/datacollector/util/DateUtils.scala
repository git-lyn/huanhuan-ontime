package com.wmidop.datacollector.util

import java.io.File
import java.util.Properties

import org.joda.time.DateTime

object DateUtils {
  val timestamps = System.currentTimeMillis();
  val times = new DateTime(timestamps)
  val stream = DateUtils.getClass.getClassLoader.getResourceAsStream("kafka-config.properties")
  val prop = new Properties()
  prop.load(stream)

  /**
   * 获取相应的年月日
   * @return
   */
  def dayOfYear() ={
    // yyyy-MM-dd HH:mm:ss
    val day = times.toString("yyyy-MM-dd")
    day
  }

  /**
   * 获取checkpoint dir
   * @return
   */
  def getCheckpoingDir(): String = {
    val dir = prop.getProperty("streaming.checkpoing.dir") + dayOfYear()
//    val files = new File(dir)
//    if(!files.exists()) {
//      files.mkdir();
//    }
    println("dir:$$$$$$$$$$$$$: " + dir)
    dir
  }

}
