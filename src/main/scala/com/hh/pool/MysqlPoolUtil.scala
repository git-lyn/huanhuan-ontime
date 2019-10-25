package com.hh.pool

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.Properties

import org.apache.commons.dbcp.BasicDataSource
import org.apache.log4j.LogManager

/**
 *jdbc mysql 连接池工具类
 * @author keguang
 */
object MysqlPoolUtil {

  val logger = LogManager.getLogger(MysqlPoolUtil.getClass.getSimpleName)
  val stream = MysqlPoolUtil.getClass.getClassLoader.getResourceAsStream("commerce.properties")
  val prop = new Properties()
  prop.load(stream)
  private var bs:BasicDataSource = null

  /**
   * 创建数据源
   * @return
   */
  def getDataSource():BasicDataSource={
    if(bs==null){
      bs = new BasicDataSource()
      bs.setDriverClassName("com.mysql.jdbc.Driver")
      bs.setUrl(prop.getProperty("jdbc.url"))
      bs.setUsername(prop.getProperty("jdbc.user"))
      bs.setPassword(prop.getProperty("jdbc.password"))
      bs.setMaxActive(50)           // 设置最大并发数
      bs.setInitialSize(20)          // 数据库初始化时，创建的连接个数
      bs.setMinIdle(20)              // 在不新建连接的条件下，池中保持空闲的最少连接数。
      bs.setMaxIdle(20)             // 池里不会被释放的最多空闲连接数量。设置为0时表示无限制。
      bs.setMaxWait(5000)             // 在抛出异常之前，池等待连接被回收的最长时间（当没有可用连接时）。设置为-1表示无限等待。
      bs.setMinEvictableIdleTimeMillis(10*1000)     // 空闲连接5秒中后释放
      bs.setTimeBetweenEvictionRunsMillis(1*60*1000)      //1分钟检测一次是否有死掉的线程
      bs.setTestOnBorrow(true)
    }
    bs
  }

  /**
   * 释放数据源
   */
  def shutDownDataSource(){
    if(bs!=null){
      bs.close()
    }
  }

  /**
   * 获取数据库连接
   * @return
   */
  def getConnection():Connection={
    var con:Connection = null
    try {
      if(bs!=null){
        con = bs.getConnection()
      }else{
        con = getDataSource().getConnection()
      }
    } catch{
      case e:Exception => logger.error(e)
    }
    con
  }

  /**
   * 关闭连接
   */
  def closeCon(rs:ResultSet ,ps:PreparedStatement,con:Connection){
    if(rs!=null){
      try {
        rs.close()
      } catch{
        case e:Exception => println(e.getMessage)
      }
    }
    if(ps!=null){
      try {
        ps.close()
      } catch{
        case e:Exception => println(e.getMessage)
      }
    }
    if(con!=null){
      try {
        con.close()
      } catch{
        case e:Exception => println(e.getMessage)
      }
    }
  }
}
