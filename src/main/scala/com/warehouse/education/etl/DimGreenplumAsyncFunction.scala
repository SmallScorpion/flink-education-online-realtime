package com.warehouse.education.etl

import java.sql.{Connection, ResultSet}
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import com.alibaba.fastjson.JSONObject
import com.google.common.cache.{Cache, CacheBuilder}
import com.warehouse.education.util.{DataSourceUtil, ParseJsonData, QueryCallback, SqlProxy}
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.curator.org.apache.curator.framework.recipes.locks.InterProcessMutex
import org.apache.flink.shaded.curator.org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.flink.shaded.curator.org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import org.apache.flink.util.ExecutorUtils

class DimGreenplumAsyncFunction extends RichAsyncFunction[String, String] {
  var executorService: ExecutorService = _
  var cache: Cache[String, String] = _
  var sqlProxy: SqlProxy = _
  var client: CuratorFramework = _
  var connection: Connection = _

  //初始化线程池 初始化缓存
  override def open(parameters: Configuration): Unit = {
    //创建分布式锁客户端
    val retryPolicy = new ExponentialBackoffRetry(1000, 3)
    client = CuratorFrameworkFactory.newClient("cdh02:2181,cdh03:2181,cdh04:2181", retryPolicy)
    client.start()

    //创建线程池
    executorService = Executors.newFixedThreadPool(12)
    cache = CacheBuilder.newBuilder()
      .concurrencyLevel(12) //设置并发级别  允许12个线程使用
      .expireAfterAccess(2, TimeUnit.HOURS) //设置缓存2小时
      .maximumSize(10000) //设置缓存大小
      .build()
    connection = DataSourceUtil.getConnection
    sqlProxy = new SqlProxy

  }

  override def close(): Unit = {
    connection.close()
    client.close()
    ExecutorUtils.gracefulShutdown(100, TimeUnit.MILLISECONDS, executorService)
  }


  override def asyncInvoke(input: String, resultFuture: ResultFuture[String]): Unit = {
    executorService.submit(new Runnable {
      override def run(): Unit = {
        try {
          val resultJsonObject = getGreenplumJsonObject(input, connection, sqlProxy, cache)
          //创建分布式锁
          val mutex = new InterProcessMutex(client, "/curator/lock/")
          mutex.acquire()
          writeDtaToHbase(connection, resultJsonObject)
          //释放锁操作
          mutex.release()
          resultFuture.complete(Array(resultJsonObject.toJSONString))
        } catch {
          case e: Exception => resultFuture.complete(Array("error:" + e.printStackTrace()))
        }
      }
    })
  }

  /**
    * 关联greenplum维度数据
    *
    * @param input
    * @param connection
    * @param sqlProxy
    * @param cache
    */
  def getGreenplumJsonObject(input: String, connection: Connection, sqlProxy: SqlProxy, cache: Cache[String, String]): JSONObject = {
    val jsonObject = ParseJsonData.getJsonData(input)
    //通过adid siteid vipid 去greenplum维度表查询数据
    val ad_id = jsonObject.getString("ad_id")
    val siteid = jsonObject.getString("siteid")
    val vip_id = jsonObject.getString("vip_id")
    val dn = jsonObject.getString("dn")
    var adname: String = ""
    var sitename: String = ""
    var siteurl: String = ""
    var delete: String = ""
    var site_createtime: String = ""
    var site_creator: String = ""
    var vip_level: String = ""
    var vip_start_time: String = ""
    var vip_end_tiem: String = ""
    var last_modify_time = ""
    var max_free = ""
    var min_free = ""
    var next_level = ""
    var operator = ""
    //查询广告关联表adname 先去缓存中查询 如果缓存中没有再去greenplum中查询
    if (!"".equals(ad_id)) {
      adname = cache.getIfPresent("adname:" + ad_id + "_" + dn)
      if (adname == null || "".equals(adname)) {
        sqlProxy.executeQuery(connection, "select adname from dwd_basead where adid=" + ad_id.toInt + " and dn= '" + dn + "'", null, new QueryCallback {
          def process(rs: ResultSet): Unit = {
            while (rs.next()) {
              adname = rs.getString(1)
            }
            rs.close()
          }
        })
        cache.put("adname:" + ad_id + "_" + dn, adname) //放入缓存
      }
    }
    //查询网站关联表sitename siteurl等信息
    if (!"".equals(siteid)) {
      val siteDetail = cache.getIfPresent("siteDetail:" + siteid + "_" + dn) //先从缓存中取
      if (siteDetail == null || "".equals(siteDetail)) {
        sqlProxy.executeQuery(connection, "select sitename,siteurl,delete,createtime,creator from dwd_basewebsite where siteid=" + siteid.toInt + " and dn='" + dn + "'", null, new QueryCallback {
          def process(rs: ResultSet): Unit = {
            while (rs.next()) {
              sitename = rs.getString(1)
              siteurl = rs.getString(2)
              delete = rs.getString(3)
              site_createtime = rs.getString(4)
              site_creator = rs.getString(5)
            }
            rs.close()
          }
        })
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
    //关联vip表数据
    if (!"".equals(vip_id)) {
      val vipDetail = cache.getIfPresent("vipDetail:" + vip_id + "_" + dn) //先查询缓存
      if (vipDetail == null || "".equals(vipDetail)) {
        sqlProxy.executeQuery(connection, "select vip_level,start_time,end_time,last_modify_time,max_free,min_free,next_level,operator from dwd_membervip where vip_id=" + vip_id.toInt + " and dn='" + dn + "'"
          , null, new QueryCallback {
            def process(rs: ResultSet): Unit = {
              while (rs.next()) {
                vip_level = rs.getString(1)
                vip_start_time = rs.getString(2)
                vip_end_tiem = rs.getString(3)
                last_modify_time = rs.getString(4)
                max_free = rs.getString(5)
                min_free = rs.getString(6)
                next_level = rs.getString(7)
                operator = rs.getString(8)
              }
            }
          })
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
    jsonObject.put("adname", adname)
    jsonObject.put("sitename", sitename)
    jsonObject.put("siteurl", siteurl)
    jsonObject.put("delete", delete)
    jsonObject.put("site_createtime", site_createtime)
    jsonObject.put("site_creator", site_creator)
    jsonObject.put("vip_level", vip_level)
    jsonObject.put("vip_start_time", vip_start_time)
    jsonObject.put("vip_end_tiem", vip_end_tiem)
    jsonObject.put("last_modify_time", last_modify_time)
    jsonObject.put("max_free", max_free)
    jsonObject.put("min_free", min_free)
    jsonObject.put("next_level", next_level)
    jsonObject.put("operator", operator)
    jsonObject
  }

  def writeDtaToHbase(connection: Connection, resultJsonObject: JSONObject) = {

    //获得锁执行业务逻辑
    //根据用户id和dn先去greenplum查询 数据 是否存在 不存在执行新增操作 存在执行修改操作
    var uid = ""
    var paymoney = "0"
    sqlProxy.executeQuery(connection, "select uid,paymoney from dim_member where uid=? and dn=?", Array(resultJsonObject.getIntValue("uid"), resultJsonObject.getString("dn")), new QueryCallback {
      def process(rs: ResultSet): Unit = {
        while (rs.next()) {
          uid = rs.getString(1)
          if (!"".equals(rs.getString(2).trim))
            paymoney = rs.getString(2).trim
        }
        rs.close()
      }
    })
    if ("".equals(uid)) {
      //执行新增语句
      sqlProxy.executeUpdate(connection, "insert into dim_member values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?," +
        "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", Array(resultJsonObject.getIntValue("uid"), resultJsonObject.getIntValue("ad_id")
        , resultJsonObject.getString("birthday"), resultJsonObject.getString("email"), resultJsonObject.getString("fullname")
        , resultJsonObject.getString("iconurl"), resultJsonObject.getString("lastlogin"), resultJsonObject.getString("mailaddr")
        , resultJsonObject.getString("memberlevel"), resultJsonObject.getString("password"), resultJsonObject.getString("phone")
        , resultJsonObject.getString("qq"), resultJsonObject.getString("register"), resultJsonObject.getString("regupdatetime")
        , resultJsonObject.getString("unitname"), resultJsonObject.getString("userip"), resultJsonObject.getString("zipcode")
        , resultJsonObject.getString("appkey"), resultJsonObject.getString("bdp_uuid"), resultJsonObject.getString("regtype_createtime")
        , resultJsonObject.getString("isrange"), resultJsonObject.getString("regsource"), resultJsonObject.getString("websiteid")
        , resultJsonObject.getString("adname"), resultJsonObject.getString("siteid"), resultJsonObject.getString("sitename")
        , resultJsonObject.getString("siteurl"), resultJsonObject.getString("site_createtime"), resultJsonObject.getString("paymoney")
        , resultJsonObject.getString("vip_id"), resultJsonObject.getString("vip_level"), resultJsonObject.getString("vip_start_time")
        , resultJsonObject.getString("vip_end_time"), resultJsonObject.getString("last_modify_time"), resultJsonObject.getString("max_free")
        , resultJsonObject.getString("min_free"), resultJsonObject.getString("next_level"), resultJsonObject.getString("operator")
        , resultJsonObject.getString("dn"), resultJsonObject.getString("dt")))
    } else {
      //有数据则进行 金额累加操作
      val resultmoney = if ("".equals(resultJsonObject.getString("paymoney"))) "0" else resultJsonObject.getString("paymoney")
      val sumpaymoney = BigDecimal(paymoney) + BigDecimal(resultmoney)
      sqlProxy.executeUpdate(connection, "update dim_member set paymoney=? where uid=? and dn=?", Array(sumpaymoney, uid.toInt, resultJsonObject.getString("dn")))
    }
  }
}
