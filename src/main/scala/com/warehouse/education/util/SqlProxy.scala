package com.warehouse.education.util

import java.sql.{Connection, PreparedStatement, ResultSet}

trait QueryCallback {
  def process(rs: ResultSet)
}

class SqlProxy {
  private var rs: ResultSet = _
  private var psmt: PreparedStatement = _

  /**
    * 执行修改语句
    *
    * @param conn
    * @param sql
    * @param params
    * @return
    */
  def executeUpdate(conn: Connection, sql: String, params: Array[Any]): Int = {
    var rtn = 0
    try {
      psmt = conn.prepareStatement(sql)
      if (params != null && params.length > 0) {
        for (i <- 0 until params.length) {
          psmt.setObject(i + 1, params(i))
        }
      }
      rtn = psmt.executeUpdate()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    rtn
  }

  /**
    * 执行查询语句
    * 执行查询语句
    *
    * @param conn
    * @param sql
    * @param params
    * @return
    */
  def executeQuery(conn: Connection, sql: String, params: Array[Any], queryCallback: QueryCallback) = {
        rs = null
        try {
          psmt = conn.prepareStatement(sql)
          if (params != null && params.length > 0) {
            for (i <- 0 until params.length) {
              psmt.setObject(i + 1, params(i))
            }
          }
          rs = psmt.executeQuery()
      queryCallback.process(rs)
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def shutdown(conn: Connection): Unit = DataSourceUtil.closeResource(rs, psmt, conn)
}
