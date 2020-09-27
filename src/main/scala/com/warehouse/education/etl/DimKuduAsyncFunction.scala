package com.warehouse.education.etl

import java.math.BigInteger
import java.security.MessageDigest
import java.sql.Timestamp
import java.util
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import com.alibaba.fastjson.JSONObject
import com.google.common.cache.{Cache, CacheBuilder}
import com.warehouse.education.model.GlobalConfig
import com.warehouse.education.util.ParseJsonData
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import org.apache.flink.util.ExecutorUtils
import org.apache.kudu.client.KuduPredicate.ComparisonOp
import org.apache.kudu.client.SessionConfiguration.FlushMode
import org.apache.kudu.client.{AsyncKuduClient, AsyncKuduSession, KuduClient, KuduPredicate, KuduSession, KuduTable, RowResultIterator}

/**
  * flink异步io join kudu
  * 根据官方推荐 异步io 使用异步客户端 如果没有可以自己实现多线程客户端访问
  */
class DimKuduAsyncFunction extends RichAsyncFunction[String, String] {
  var executorService: ExecutorService = _
  var cache: Cache[String, String] = _
  var kuduClient: KuduClient = _
  var kuduSession: KuduSession = _
  var dwdBaseadTable: KuduTable = _
  var dwdBaseWebSiteTable: KuduTable = _
  var dwdVipLevelTable: KuduTable = _

  override def open(parameters: Configuration): Unit = {
    kuduClient = new KuduClient.KuduClientBuilder(GlobalConfig.KUDU_MASTER).build()
    kuduSession = kuduClient.newSession()
    kuduSession.setFlushMode(FlushMode.AUTO_FLUSH_SYNC)
    dwdBaseadTable = kuduClient.openTable(GlobalConfig.KUDU_TABLE_DWDBASEAD)
    dwdBaseWebSiteTable = kuduClient.openTable(GlobalConfig.KUDU_TABLE_DWDBASEWEBSITE)
    dwdVipLevelTable = kuduClient.openTable(GlobalConfig.KUDU_TABLE_DWDVIPLEVEL)
    executorService = Executors.newFixedThreadPool(12)
    cache = CacheBuilder.newBuilder()
      .concurrencyLevel(12) //设置并发级别 允许12个线程同时访问
      .expireAfterAccess(2, TimeUnit.HOURS) //设置过期时间
      .maximumSize(10000) //设置缓存大小
      .build()
  }

  override def close(): Unit = {
    kuduSession.close()
    kuduClient.close()
    ExecutorUtils.gracefulShutdown(100, TimeUnit.MILLISECONDS, executorService);
  }

  override def asyncInvoke(input: String, resultFuture: ResultFuture[String]): Unit = {
    executorService.submit(new Runnable {
      override def run(): Unit = {
        try {
          val baseAdResult = getBaseAdData(input)
          val baseWebSiteResult = getBaseWebSite(baseAdResult)
          val result = getBaseVipLevel(baseWebSiteResult)
          resultFuture.complete(Array(result.toJSONString))
        } catch {
          case e: ExceptionInInitializerError => resultFuture.complete(Array("error:" + e.printStackTrace()))
        }
      }
    })
  }

  /**
    * 查询广告表
    *
    * @param input
    * @return
    */
  def getBaseAdData(input: String): JSONObject = {
    val jsonObject = ParseJsonData.getJsonData(input)
    //通过adid  siteid vipid 去kudu维度表查询数据
    val ad_id = jsonObject.getInteger("ad_id")
    val dn = jsonObject.getString("dn")
    var adname: String = ""
    if (null != ad_id) {
      //查询广告关联表 adname 先去缓存中查询 如果缓存中没有再去kudu中查询
      adname = cache.getIfPresent("adname:" + ad_id + "_" + dn)
      if (adname == null || "".equals(adname)) {
        val kuduTable = dwdBaseadTable
        val schema = kuduTable.getSchema
        //声明查询条件 等于adid的值
        val eqadidPred = KuduPredicate.newComparisonPredicate(schema.getColumn("adid"), ComparisonOp.EQUAL, ad_id)
        //什么查询条件 等于dn的值
        val eqdnPred = KuduPredicate.newComparisonPredicate(schema.getColumn("dn"), ComparisonOp.EQUAL, dn)
        //将查询条件放入scann中进行查询  值查询adnmae列
        val list = new util.ArrayList[String]()
        list.add("adname")
        val kuduScanner = kuduClient.newScannerBuilder(kuduTable).setProjectedColumnNames(list).addPredicate(eqadidPred)
          .addPredicate(eqdnPred)
          .build()
        while (kuduScanner.hasMoreRows) {
          val results = kuduScanner.nextRows()
          while (results.hasNext) {
            adname = results.next().getString("adname")
            cache.put("adname:" + ad_id + "_" + dn, adname) //放入缓存
          }
        }
      }
    }
    jsonObject.put("adname", adname)
    jsonObject
  }

  /**
    * 查询网站表
    *
    * @param jsonObject
    * @return
    */
  def getBaseWebSite(jsonObject: JSONObject) = {
    val siteid = jsonObject.getInteger("siteid")
    val dn = jsonObject.getString("dn")
    var sitename: String = ""
    var siteurl: String = ""
    var delete: String = ""
    var site_createtime: String = ""
    var site_creator: String = ""
    //查询网站关联表 sitename siteurl等信息
    if (null != siteid) {
      //先从缓存取数据
      val siteDetail = cache.getIfPresent("siteDetail:" + siteid + "_" + dn)
      if (siteDetail == null || "".equals(siteDetail)) {
        //查询kudu
        val kuduTable = dwdBaseWebSiteTable
        val schema = kuduTable.getSchema
        //声明查询条件  site_id相等
        val eqsiteidPred = KuduPredicate.newComparisonPredicate(schema.getColumn("site_id"), ComparisonOp.EQUAL, siteid)
        //声明查询条件  dn相等
        val eqdnPred = KuduPredicate.newComparisonPredicate(schema.getColumn("dn"), ComparisonOp.EQUAL, dn)
        //声明查询字段
        val list = new util.ArrayList[String]()
        list.add("sitename")
        list.add("siteurl")
        list.add("delete")
        list.add("createtime")
        list.add("creator")
        //查询
        val kuduScanner = kuduClient.newScannerBuilder(kuduTable).setProjectedColumnNames(list).addPredicate(eqsiteidPred).addPredicate(eqdnPred).build()
        while (kuduScanner.hasMoreRows) {
          val results = kuduScanner.nextRows()
          while (results.hasNext) {
            val result = results.next()
            sitename = result.getString("sitename")
            siteurl = result.getString("siteurl")
            delete = result.getInt("delete").toString
            site_createtime = result.getString("createtime")
            site_creator = result.getString("creator")
          }
        }
        //将查询到的数据拼装成json格式 存入缓存
        val jsonObject = new JSONObject()
        jsonObject.put("sitename", sitename)
        jsonObject.put("siteurl", siteurl)
        jsonObject.put("delete", delete)
        jsonObject.put("site_createtime", site_createtime)
        jsonObject.put("site_creator", site_creator)
        cache.put("siteDetail:" + siteid + "_" + dn, jsonObject.toJSONString)
      } else {
        //如果缓存中有数据 则解析缓存中的json数据
        val jsonObject = ParseJsonData.getJsonData(siteDetail)
        sitename = jsonObject.getString("sitename")
        siteurl = jsonObject.getString("siteurl")
        delete = jsonObject.getString("delete")
        site_createtime = jsonObject.getString("site_createtime")
        site_creator = jsonObject.getString("site_creator")
      }
    }
    jsonObject.put("sitename", sitename)
    jsonObject.put("siteurl", siteurl)
    jsonObject.put("delete", delete)
    jsonObject.put("site_createtime", site_createtime)
    jsonObject.put("site_creator", site_creator)
    jsonObject
  }

  /**
    * 查询vip信息
    *
    * @param jsonObject
    * @return
    */
  def getBaseVipLevel(jsonObject: JSONObject): JSONObject = {
    val vip_id = jsonObject.getInteger("vip_id")
    val dn = jsonObject.getString("dn")
    var vip_level: String = ""
    var vip_start_time: String = ""
    var vip_end_tiem: String = ""
    var last_modify_time = ""
    var max_free = ""
    var min_free = ""
    var next_level = ""
    var operator = ""
    //查询vip表关联数据
    if (null != vip_id) {
      val vipDetail = cache.getIfPresent("vipDetail:" + vip_id + "_" + dn)
      if (vipDetail == null || "".equals(vipDetail)) {
        val kuduTable = dwdVipLevelTable
        val schma = kuduTable.getSchema
        //声明查询条件  vip_id相等
        val eqvipidPred = KuduPredicate.newComparisonPredicate(schma.getColumn("vip_id"), ComparisonOp.EQUAL, vip_id)
        //声明查询条件 dn相等
        val eqdnPred = KuduPredicate.newComparisonPredicate(schma.getColumn("dn"), ComparisonOp.EQUAL, dn)
        //声明查询字段
        val list = new util.ArrayList[String]()
        list.add("vip_level")
        list.add("start_time")
        list.add("end_time")
        list.add("last_modify_time")
        list.add("max_free")
        list.add("min_free")
        list.add("next_level")
        list.add("operator")
        //查询
        val kuduScanner = kuduClient.newScannerBuilder(kuduTable).setProjectedColumnNames(list).addPredicate(eqvipidPred)
          .addPredicate(eqdnPred).build()
        while (kuduScanner.hasMoreRows) {
          val results = kuduScanner.nextRows()
          while (results.hasNext) {
            val result = results.next()
            vip_level = result.getString("vip_level")
            vip_start_time = result.getTimestamp("start_time").toString
            vip_end_tiem = result.getTimestamp("end_time").toString
            last_modify_time = result.getTimestamp("last_modify_time").toString
            max_free = result.getString("max_free")
            min_free = result.getString("min_free")
            next_level = result.getString("next_level")
            operator = result.getString("operator")
          }
        }
        //将查询到的数据拼装成json 存入缓存
        val jsonObject = new JSONObject()
        jsonObject.put("vip_level", vip_level)
        jsonObject.put("vip_start_time", vip_start_time)
        jsonObject.put("vip_end_tiem", vip_end_tiem)
        jsonObject.put("last_modify_time", last_modify_time)
        jsonObject.put("max_free", max_free)
        jsonObject.put("min_free", min_free)
        jsonObject.put("next_level", next_level)
        jsonObject.put("operator", operator)
        cache.put("vipDetail:" + vip_id + "_" + dn, jsonObject.toJSONString)
      } else {
        //如果缓存中有值 就解析缓存中的数据
        val jsonObject = ParseJsonData.getJsonData(vipDetail)
        vip_level = jsonObject.getString("vip_level")
        vip_start_time = jsonObject.getString("vip_start_time")
        vip_end_tiem = jsonObject.getString("vip_end_tiem")
        last_modify_time = jsonObject.getString("last_modify_time")
        max_free = jsonObject.getString("max_free")
        min_free = jsonObject.getString("min_free")
        next_level = jsonObject.getString("next_level")
        operator = jsonObject.getString("operator")
      }
    }
    jsonObject.put("vip_level", vip_level)
    jsonObject.put("vip_start_time", vip_start_time)
    jsonObject.put("vip_end_time", vip_end_tiem)
    jsonObject.put("last_modify_time", last_modify_time)
    jsonObject.put("max_free", max_free)
    jsonObject.put("min_free", min_free)
    jsonObject.put("next_level", next_level)
    jsonObject.put("operator", operator)
    jsonObject
  }


}
