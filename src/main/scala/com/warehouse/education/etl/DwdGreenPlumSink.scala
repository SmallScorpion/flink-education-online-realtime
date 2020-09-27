package com.warehouse.education.etl

import java.sql.{Connection, ResultSet}

import com.google.gson.Gson
import com.warehouse.education.model.{BaseAd, BaseViplevel, BaseWebSite, TopicAndValue}
import com.warehouse.education.util.{DataSourceUtil, QueryCallback, SqlProxy}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

class DwdGreenPlumSink extends RichSinkFunction[TopicAndValue] {
  var connection: Connection = _
  var sqlProxy: SqlProxy = _

  //打开greenplum链接
  override def open(parameters: Configuration): Unit = {
    connection = DataSourceUtil.getConnection
    sqlProxy = new SqlProxy
  }

  //写入greenplum
  override def invoke(value: TopicAndValue, context: SinkFunction.Context[_]): Unit = {
    value.topic match {
      case "basewebsite" => invokeBaseWebSite(connection, value)
      case "basead" => invokeBaseBaseAd(connection, value)
      case _ => invokeBaseVipLevel(connection, value)
    }
  }


  override def close(): Unit = {
    connection.close()
  }

  def invokeBaseWebSite(connection: Connection, value: TopicAndValue) = {
    val gson = new Gson()
    val basewebsite = gson.fromJson(value.value, classOf[BaseWebSite])
    val st = connection.createStatement()
    var siteid = ""
    sqlProxy.executeQuery(connection, "select siteid from dwd_basewebsite where siteid=? and dn=?", Array(basewebsite.siteid, basewebsite.dn), new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while (rs.next()) {
          siteid = String.valueOf(rs.getInt(1))
        }
        rs.close()
      }
    })
    if ("".equals(siteid)) {
      //新增
      sqlProxy.executeUpdate(connection, "insert into dwd_basewebsite values(?,?,?,?,?,?,?)", Array(basewebsite.siteid, basewebsite.sitename, basewebsite.siteurl,
        basewebsite.delete, basewebsite.createtime, basewebsite.creator, basewebsite.dn))
    } else {
      //修改
      sqlProxy.executeUpdate(connection, "update dwd_basewebsite set sitename=?,siteurl=?,delete=? where siteid=? and dn=?",
        Array(basewebsite.sitename, basewebsite.siteurl, basewebsite.delete, basewebsite.siteid, basewebsite.dn))
    }
  }

  def invokeBaseBaseAd(connection: Connection, value: TopicAndValue): Unit = {
    val gson = new Gson()
    val basead = gson.fromJson(value.value, classOf[BaseAd])
    val st = connection.createStatement()
    var adid = ""
    sqlProxy.executeQuery(connection, "select adid from dwd_basead where adid=? and dn=?", Array(basead.adid, basead.dn), new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while (rs.next()) {
          adid = String.valueOf(rs.getInt(1))
        }
        rs.close()
      }
    })
    if ("".equals(adid)) {
      //新增
      sqlProxy.executeUpdate(connection, "insert into dwd_basead values(?,?,?) ", Array(basead.adid, basead.adname, basead.dn))
    } else {
      //修改
      sqlProxy.executeUpdate(connection, "update dwd_basead set adname=? where adid=? and dn=?", Array(basead.adname, basead.adid, basead.dn))
    }
  }

  def invokeBaseVipLevel(connection: Connection, value: TopicAndValue) = {
    val gson = new Gson()
    val baseViplevel = gson.fromJson(value.value, classOf[BaseViplevel])
    val st = connection.createStatement()
    var vipid = ""
    sqlProxy.executeQuery(connection, "select vip_id from dwd_membervip where vip_id=? and dn=?", Array(baseViplevel.vip_id, baseViplevel.dn), new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while (rs.next()) {
          vipid = String.valueOf(rs.getInt(1))
        }
        rs.close()
      }
    })
    if ("".equals(vipid)) {
      //新增
      sqlProxy.executeUpdate(connection, "insert into dwd_membervip values(?,?,?,?,?,?,?,?,?,?)", Array(baseViplevel.vip_id, baseViplevel.vip_level,
        baseViplevel.start_time, baseViplevel.end_time, baseViplevel.last_modify_time, baseViplevel.max_free, baseViplevel.min_free, baseViplevel.next_level,
        baseViplevel.operator, baseViplevel.dn))
    } else {
      //修改
      sqlProxy.executeUpdate(connection, "update dwd_membervip set vip_level=?,start_time=?,end_time=?,last_modify_time=?,max_free=?,min_free=?,next_level=?," +
        "operator=? where vip_id=? and dn=?", Array(baseViplevel.vip_level, baseViplevel.start_time, baseViplevel.end_time, baseViplevel.last_modify_time,
        baseViplevel.max_free, baseViplevel.min_free, baseViplevel.next_level, baseViplevel.operator, baseViplevel.vip_id, baseViplevel.dn))
    }

  }
}
