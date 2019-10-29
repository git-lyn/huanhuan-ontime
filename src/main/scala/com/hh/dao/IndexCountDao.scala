package com.hh.dao

import java.sql.ResultSet

import com.hh.model.IndexClickCount
import com.hh.pool.{CreateMySqlPool, QueryCallback}

import scala.collection.mutable.ArrayBuffer

object IndexCountDao {
  def updateBatch(indexClickCounts: Array[IndexClickCount]) {
    // 获取对象池单例对象
    val mySqlPool = CreateMySqlPool()
    // 从对象池中提取对象
    val client = mySqlPool.borrowObject()


    // 区分开来哪些是要插入的，哪些是要更新的
    val insertIndexClickCounts = ArrayBuffer[IndexClickCount]()
    val updateIndexClickCounts = ArrayBuffer[IndexClickCount]()

    val selectSQL = "SELECT count(*) " +
      "FROM index_gid_click_count " +
      "WHERE date=? " +
      "AND position=? " +
      "AND userid=?"

    for (indexClickCount <- indexClickCounts) {

      val params = Array[Any](indexClickCount.date, indexClickCount.position, indexClickCount.userid)
      // 通过查询结果判断当前项时待插入还是待更新
      client.executeQuery(selectSQL, params, new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          if (rs.next() && rs.getInt(1) > 0) {
            updateIndexClickCounts += indexClickCount
          } else {
            insertIndexClickCounts += indexClickCount
          }
        }
      })
    }

    // 对于需要插入的数据，执行批量插入操作
    val insertSQL = "INSERT INTO index_gid_click_count(date,position,userid,clickCount) VALUES(?,?,?,?)"

    val insertParamsList: ArrayBuffer[Array[Any]] = ArrayBuffer[Array[Any]]()

    for (indexClickCount <- insertIndexClickCounts) {
      insertParamsList += Array[Any](indexClickCount.date, indexClickCount.position, indexClickCount.userid, indexClickCount.clickCount)
    }

    client.executeBatch(insertSQL, insertParamsList.toArray)

    // 对于需要更新的数据，执行批量更新操作
    // 此处的UPDATE是进行覆盖
    val updateSQL = "UPDATE index_gid_click_count SET clickCount=? " +
      "WHERE date=? " +
      "AND position=? " +
      "AND userid=?"

    val updateParamsList: ArrayBuffer[Array[Any]] = ArrayBuffer[Array[Any]]()

    for (indexClickCount <- updateIndexClickCounts) {
      updateParamsList += Array[Any](indexClickCount.clickCount, indexClickCount.date, indexClickCount.position, indexClickCount.userid)
    }

    client.executeBatch(updateSQL, updateParamsList.toArray)

    // 使用完成后将对象返回给对象池
    mySqlPool.returnObject(client)
  }


  // 向mysql插入数据
  def insertBatch(indexClickCounts: Array[IndexClickCount]) {
    // 获取对象池单例对象
    val mySqlPool = CreateMySqlPool()
    // 从对象池中提取对象
    val client = mySqlPool.borrowObject()


    // 区分开来哪些是要插入的，哪些是要更新的
    val insertIndexClickCounts = ArrayBuffer[IndexClickCount]()
    val updateIndexClickCounts = ArrayBuffer[IndexClickCount]()

    val selectSQL = "SELECT count(*) " +
      "FROM index_gid_click_count " +
      "WHERE date=? " +
      "AND position=? " +
      "AND userid=?"

//    for (indexClickCount <- indexClickCounts) {
//
//      val params = Array[Any](indexClickCount.date, indexClickCount.position, indexClickCount.userid)
//      // 通过查询结果判断当前项时待插入还是待更新
//      client.executeQuery(selectSQL, params, new QueryCallback {
//        override def process(rs: ResultSet): Unit = {
//          if (rs.next() && rs.getInt(1) > 0) {
//            updateIndexClickCounts += indexClickCount
//          } else {
//            insertIndexClickCounts += indexClickCount
//          }
//        }
//      })
//    }

    // 对于需要插入的数据，执行批量插入操作
    val insertSQL = "INSERT INTO index_gid_click_count(date,position,userid,clickCount) VALUES(?,?,?,?)"

    val insertParamsList: ArrayBuffer[Array[Any]] = ArrayBuffer[Array[Any]]()

    for (indexClickCount <- indexClickCounts) {
      insertParamsList += Array[Any](indexClickCount.date, indexClickCount.position, indexClickCount.userid, indexClickCount.clickCount)
    }

//    client.executeBatch(insertSQL, insertParamsList.toArray)
    client.executeBatch(insertSQL, insertParamsList.toArray)

    // 对于需要更新的数据，执行批量更新操作
    // 此处的UPDATE是进行覆盖
//    val updateSQL = "UPDATE index_gid_click_count SET clickCount=? " +
//      "WHERE date=? " +
//      "AND position=? " +
//      "AND userid=?"
//
//    val updateParamsList: ArrayBuffer[Array[Any]] = ArrayBuffer[Array[Any]]()
//
//    for (indexClickCount <- updateIndexClickCounts) {
//      updateParamsList += Array[Any](indexClickCount.clickCount, indexClickCount.date, indexClickCount.position, indexClickCount.userid)
//    }
//
//    client.executeBatch(updateSQL, updateParamsList.toArray)

    // 使用完成后将对象返回给对象池
    mySqlPool.returnObject(client)
  }

  def dataFlag(date: String, position: String, userid: String,clickCount: Long): Boolean = {
    // 获取对象池单例对象
    val mySqlPool = CreateMySqlPool()
    // 从对象池中提取对象
    val client = mySqlPool.borrowObject()
    var flag = false;
    val selectSQL = "SELECT count(*) " +
      "FROM index_gid_click_count " +
      "WHERE date=? " +
      "AND position=? " +
      "AND userid=?" +
      "AND clickCount=?"

    val params = Array[Any](date, position,userid, clickCount)

    // 根据多个条件查询指定用户的点击量，将查询结果累加到clickCount中
    client.executeQuery(selectSQL, params, new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        if (rs.next() && rs.getInt(1) > 0) {
          // 如果有数据，就返回false，数据会被过滤掉
          flag = false
        } else {
          // 如果没有数据,返回false
          flag = true
        }
      }
    })
    // 使用完成后将对象返回给对象池
    mySqlPool.returnObject(client)
    flag
  }

}


