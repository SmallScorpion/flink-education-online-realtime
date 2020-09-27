package com.warehouse.education.etl

import java.sql.Timestamp

import com.google.gson.Gson
import com.warehouse.education.model.{BaseAd, BaseViplevel, BaseWebSite, GlobalConfig, TopicAndValue}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.kudu.client.{KuduClient, KuduSession, KuduTable}

class DwdKuduSink extends RichSinkFunction[TopicAndValue] {
  var kuduClient: KuduClient = _
  var kuduSession: KuduSession = _
  var dwdBaseadTable: KuduTable = _
  var dwdBaseWebSiteTable: KuduTable = _
  var dwdVipLevelTable: KuduTable = _

  override def open(parameters: Configuration): Unit = {
    kuduClient = new KuduClient.KuduClientBuilder(GlobalConfig.KUDU_MASTER).build()
    kuduSession = kuduClient.newSession()
    kuduSession.setTimeoutMillis(60000)
    dwdBaseadTable = kuduClient.openTable(GlobalConfig.KUDU_TABLE_DWDBASEAD)
    dwdBaseWebSiteTable = kuduClient.openTable(GlobalConfig.KUDU_TABLE_DWDBASEWEBSITE)
    dwdVipLevelTable = kuduClient.openTable(GlobalConfig.KUDU_TABLE_DWDVIPLEVEL)
  }

  override def close(): Unit = {
    kuduSession.close()
    kuduClient.close()
  }

  override def invoke(value: TopicAndValue, context: SinkFunction.Context[_]): Unit = {
    value.topic match {
      case "basewebsite" => invokeBaseWebSite(value)
      case "basead" => invokeBaseBaseAd(value)
      case _ => invokeBaseVipLevel(value)
    }
  }

  def invokeBaseWebSite(value: TopicAndValue): Unit = {
    val gson = new Gson()
    val basewebsite = gson.fromJson(value.value, classOf[BaseWebSite])
    val upsert = dwdBaseWebSiteTable.newUpsert()
    val row = upsert.getRow
    row.addInt("site_id", basewebsite.siteid)
    row.addString("sitename", basewebsite.sitename)
    row.addString("siteurl", basewebsite.siteurl)
    row.addInt("delete", basewebsite.delete.toInt)
    row.addString("createtime", basewebsite.createtime)
    row.addString("creator", basewebsite.creator)
    row.addString("dn", basewebsite.dn)
    kuduSession.apply(upsert)
  }

  def invokeBaseBaseAd(value: TopicAndValue): Unit = {
    val gson = new Gson()
    val basead = gson.fromJson(value.value, classOf[BaseAd])
    val upsert = dwdBaseadTable.newUpsert()
    val row = upsert.getRow
    row.addInt("adid", basead.adid)
    row.addString("adname", basead.adname)
    row.addString("dn", basead.dn)
    kuduSession.apply(upsert)
  }

  def invokeBaseVipLevel(value: TopicAndValue): Unit = {
    val gson = new Gson()
    val baseViplevel = gson.fromJson(value.value, classOf[BaseViplevel])
    val upsert = dwdVipLevelTable.newUpsert()
    val row = upsert.getRow
    row.addInt("vip_id", baseViplevel.vip_id)
    row.addString("vip_level", baseViplevel.vip_level)
    row.addTimestamp("start_time", Timestamp.valueOf(baseViplevel.start_time))
    row.addTimestamp("end_time", Timestamp.valueOf(baseViplevel.end_time))
    row.addTimestamp("last_modify_time", Timestamp.valueOf(baseViplevel.last_modify_time))
    row.addString("max_free", baseViplevel.max_free)
    row.addString("min_free", baseViplevel.min_free)
    row.addString("next_level", baseViplevel.next_level)
    row.addString("operator", baseViplevel.operator)
    row.addString("dn", baseViplevel.dn)
    kuduSession.apply(upsert)
  }

}
