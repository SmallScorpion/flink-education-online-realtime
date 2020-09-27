package com.warehouse.education.etl

import java.math.BigInteger
import java.security.MessageDigest
import java.sql.Timestamp

import com.alibaba.fastjson.JSONObject
import com.warehouse.education.model.GlobalConfig
import com.warehouse.education.util.ParseJsonData
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.kudu.client.{KuduClient, KuduSession, KuduTable}

class DwsMemberKuduSink extends RichSinkFunction[String] {
  var kuduClient: KuduClient = _
  var kuduSession: KuduSession = _
  var dwsMemberTable: KuduTable = _

  override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {
    val jsonObject = ParseJsonData.getJsonData(value)
    inserintoDwsMember(jsonObject)
  }

  override def open(parameters: Configuration): Unit = {
    kuduClient = new KuduClient.KuduClientBuilder(GlobalConfig.KUDU_MASTER).build()
    kuduSession = kuduClient.newSession()
    dwsMemberTable = kuduClient.openTable(GlobalConfig.KUDU_TABLE_DWSMEMBER)
  }

  override def close(): Unit = {
    kuduSession.close()
    kuduClient.close()
  }

  def inserintoDwsMember(jsonObject: JSONObject): JSONObject = {
    val upsert = dwsMemberTable.newUpsert()
    val row = upsert.getRow
    val uid = jsonObject.getInteger("uid")
    val dn = jsonObject.getString("dn")
    val id = generateHash(uid.toString).substring(0, 5) + uid + dn
    row.addString("id", id)
    row.addInt("uid", uid);
    row.addInt("ad_id", jsonObject.getInteger("ad_id"))
    row.addString("fullname", jsonObject.getString("fullname"))
    row.addString("iconurl", jsonObject.getString("iconurl"))
    row.addString("mailaddr", jsonObject.getString("mailaddr"))
    row.addString("memberlevel", jsonObject.getString("memberlevel"))
    row.addString("password", jsonObject.getString("password"))
    row.addString("phone", jsonObject.getString("phone"))
    row.addString("qq", jsonObject.getString("qq"))
    row.addString("register", jsonObject.getString("register"))
    row.addString("regupdatetime", jsonObject.getString("regupdatetime"))
    row.addString("unitname", jsonObject.getString("unitname"))
    row.addString("userip", jsonObject.getString("userip"))
    row.addString("zipcode", jsonObject.getString("zipcode"))
    row.addString("appkey", jsonObject.getString("appkey"))
    row.addString("appregurl", jsonObject.getString("appregurl"))
    row.addString("bdp_uuid", jsonObject.getString("bdp_uuid"))
    row.addString("regsource", jsonObject.getString("regsource"))
    row.addString("adname", jsonObject.getString("adname"))
    row.addInt("siteid", jsonObject.getIntValue("siteid"))
    row.addString("sitename", jsonObject.getString("sitename"))
    row.addString("siteurl", jsonObject.getString("siteurl"))
    row.addString("site_delete", jsonObject.getString("delete"))
    row.addString("site_createtime", jsonObject.getString("site_createtime"))
    row.addString("site_creator", jsonObject.getString("site_creator"))
    row.addInt("vip_id", jsonObject.getIntValue("vip_id"))
    row.addString("vip_level", jsonObject.getString("vip_level"))
    if (jsonObject.getString("vip_start_time") != null && !"".equals(jsonObject.getString("vip_start_time"))) {
      row.addTimestamp("vip_start_time", Timestamp.valueOf(jsonObject.getString("vip_start_time")))
    }
    if (jsonObject.getString("vip_end_time") != null && !"".equals(jsonObject.getString("vip_end_time"))) {
      row.addTimestamp("vip_end_time", Timestamp.valueOf(jsonObject.getString("vip_end_time")))
    }
    if (jsonObject.getString("last_modify_time") != null && !"".equals(jsonObject.getString("last_modify_time"))) {
      row.addTimestamp("vip_last_modify_time", Timestamp.valueOf(jsonObject.getString("last_modify_time")))
    }
    row.addString("vip_max_free", jsonObject.getString("max_free"))
    row.addString("vip_min_free", jsonObject.getString("min_free"))
    row.addString("vip_operator", jsonObject.getString("operator"))
    if (jsonObject.getString("paymoney") != null && !"".equals(jsonObject.getString("paymoney"))) {
      row.addDecimal("paymoney", new java.math.BigDecimal(jsonObject.getString("paymoney")))
    }
    row.addString("dt", jsonObject.getString("dt"))
    row.addString("dn", jsonObject.getString("dn"))
    kuduSession.apply(upsert)
    jsonObject
  }

  /**
    * 对字符串进行MD5加密
    *
    * @param input
    * @return
    */
  def generateHash(input: String): String = {
    try {
      if (input == null) {
        null
      }
      val md = MessageDigest.getInstance("MD5")
      md.update(input.getBytes());
      val digest = md.digest();
      val bi = new BigInteger(1, digest);
      var hashText = bi.toString(16);
      while (hashText.length() < 32) {
        hashText = "0" + hashText;
      }
      hashText
    } catch {
      case e: Exception => e.printStackTrace(); null
    }
  }
}
