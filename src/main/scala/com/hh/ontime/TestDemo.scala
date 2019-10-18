package com.hh.ontime

import java.util.Date

import com.alibaba.fastjson.JSON
import org.joda.time.DateTime

object TestDemo {
  def main(args: Array[String]): Unit = {
    val a = "1568190191"
    val times = new DateTime(a.toLong * 1000)
    val res = times.formatted("yyyy-MM-dd")
    println(times.toString("yyyy-MM-dd HH:mm:ss"))
    println(new Date().getTime())
    val str = "{\"user_id\":2279,\"user_ipaddress\":\"223.73.131.17\",\"timestamp\":1568190112,\"referer\":\"\",\"url\":\"https:\\/\\/m.huanhuanyiwu.com\\/h5\\/#\\/product?id=10930\",\"location\":\"app\",\"device\":\"ANDROID\",\"content\":{\"user_id\":2279,\"click_position\":\"app_restart\",\"deviceID\":\"9d62528edc909ba9\",\"gid\":\"8d2c0f04-42ad-46ba-b2dd-96daa23d9e20\",\"ver\":\"0.4.1\"}}"
    val jsons = JSON.parseObject(str)
    val timestamps = jsons.get("timestamp").toString
    val b = new DateTime(timestamps.toLong * 1000)
    println("timestamp: " + times + "  b: " + b.toString("yyyy-MM-dd HH:mm:ss"))
    val content = jsons.getJSONObject("content")
    val user_id = content.get("user_id")
    val deviceID = content.get("deviceID")
    val gid = content.get("gid")
    println("user_id: " + user_id + " deviceID: " + deviceID + " gid: " + gid)
    val ed = new DateTime(System.currentTimeMillis())
    println(ed.toString("yyyy-MM-dd"))

    val c = "aa|bb|cc"
    val arr =  c.split("\\|")
    println(arr(0) + " " + arr(1))
  }

}
