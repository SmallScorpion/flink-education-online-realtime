package com.warehouse.education.etl

import com.warehouse.education.model.{BaseAd, BaseViplevel, BaseWebSite, GlobalConfig, TopicAndValue}
import com.google.gson.{Gson, JsonObject}
import com.warehouse.education.model.{GlobalConfig, TopicAndValue}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes

class DwdHbaseSink extends RichSinkFunction[TopicAndValue] {
  var connection: Connection = _

  //打开hbase连接
  override def open(parameters: Configuration): Unit = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", GlobalConfig.HBASE_ZOOKEEPER_QUORUM)
    conf.set("hbase.zookeeper.property.clientPort", GlobalConfig.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT)
    connection = ConnectionFactory.createConnection(conf)
  }

  //写入hbase
  override def invoke(value: TopicAndValue, context: SinkFunction.Context[_]): Unit = {
    value.topic match {
      case "basewebsite" => invokeBaseWebSite("education:dwd_basewebsite", value)
      case "basead" => invokeBaseBaseAd("education:dwd_basead", value)
      case _ => invokeBaseVipLevel("education:dwd_membervip", value)
    }
  }


  //关闭hbase连接
  override def close(): Unit = {
    super.close()
    connection.close()
  }

  def invokeBaseWebSite(tableNamae: String, value: TopicAndValue): Unit = {
    val gson = new Gson()
    val basewebsite = gson.fromJson(value.value, classOf[BaseWebSite])
    val table = connection.getTable(TableName.valueOf(tableNamae))
    val put = new Put(Bytes.toBytes(basewebsite.siteid + "_" + basewebsite.dn))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("siteid"), Bytes.toBytes(basewebsite.siteid))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sitename"), Bytes.toBytes(basewebsite.sitename))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("siteurl"), Bytes.toBytes(basewebsite.siteurl))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("delete"), Bytes.toBytes(basewebsite.delete))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("createtime"), Bytes.toBytes(basewebsite.createtime))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("creator"), Bytes.toBytes(basewebsite.creator))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("dn"), Bytes.toBytes(basewebsite.dn))
    table.put(put)
    table.close()
  }

  def invokeBaseBaseAd(tableName: String, value: TopicAndValue) = {
    val gson = new Gson()
    val basead = gson.fromJson(value.value, classOf[BaseAd])
    val table = connection.getTable(TableName.valueOf(tableName))
    val put = new Put(Bytes.toBytes(basead.adid + "_" + basead.dn))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("adid"), Bytes.toBytes(basead.adid))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("adname"), Bytes.toBytes(basead.adname))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("dn"), Bytes.toBytes(basead.dn))
    table.put(put)
    table.close()
  }

  def invokeBaseVipLevel(tableName: String, value: TopicAndValue) = {
    val gson = new Gson()
    val baseViplevel = gson.fromJson(value.value, classOf[BaseViplevel])
    val table = connection.getTable(TableName.valueOf(tableName))
    val put = new Put(Bytes.toBytes(baseViplevel.vip_id + "_" + baseViplevel.dn))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("vip_id"), Bytes.toBytes(baseViplevel.vip_id))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("vip_level"), Bytes.toBytes(baseViplevel.vip_level))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("start_time"), Bytes.toBytes(baseViplevel.start_time))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("end_time"), Bytes.toBytes(baseViplevel.end_time))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("last_modify_time"), Bytes.toBytes(baseViplevel.last_modify_time))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("max_free"), Bytes.toBytes(baseViplevel.max_free))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("min_free"), Bytes.toBytes(baseViplevel.min_free))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("next_level"), Bytes.toBytes(baseViplevel.next_level))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("operator"), Bytes.toBytes(baseViplevel.operator))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("dn"), Bytes.toBytes(baseViplevel.dn))
    table.put(put)
    table.close()
  }
}
