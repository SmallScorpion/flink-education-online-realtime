package com.warehouse.education.etl

import java.lang
import java.util.Properties

import com.alibaba.fastjson.JSONObject
import com.warehouse.education.model.{DwdMember, DwdMemberDeserializationSchema, DwdMemberPayMoney, DwdMemberPayMoneyDeserializationSchema, DwdMemberRegtype, DwdMemberRegtypeDeserializationSchema}
import com.warehouse.education.util.ParseJsonData
import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.util.Collector

//flink run -m yarn-cluster -ynm dimetl -p 12 -ys 4 -yjm 1024 -ytm 2048m -d -c com.atguigu.education.etl.DImJoinData  -yqu flink ./education-flink-online-1.0-SNAPSHOT-jar-with-dependencies.jar --group.id test --bootstrap.servers hadoop101:9092,hadoop102:9092,hadoop103:9092
//--bootstrap.servers hadoop101:9092,hadoop102:9092,hadoop103:9092 --group.id test

object DImJoinData {
  val BOOTSTRAP_SERVERS = "bootstrap.servers"
  val GROUP_ID = "group.id"
  val RETRIES = "retries"
  val TOPIC = "topic"

  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    env.getConfig.setGlobalJobParameters(params)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //设置时间模式为事件时间
    //checkpoint设置
    env.enableCheckpointing(60000l) //1分钟做一次checkpoint
    val checkpointConfig = env.getCheckpointConfig
    checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE) //仅仅一次
    checkpointConfig.setMinPauseBetweenCheckpoints(30000l) //设置checkpoint间隔时间30秒
//    checkpointConfig.setCheckpointTimeout(100000l) //设置checkpoint超时时间
    checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION) //cancel时保留checkpoint
    //设置statebackend 为rockdb
    val stateBackend: StateBackend = new RocksDBStateBackend("hdfs://nameservice1/flink/checkpoint")
    env.setStateBackend(stateBackend)
    //设置重启策略   重启3次 间隔10秒
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, org.apache.flink.api.common.time.Time.seconds(10)))

    val consumerProps = new Properties()
    consumerProps.setProperty(BOOTSTRAP_SERVERS, params.get(BOOTSTRAP_SERVERS))
    consumerProps.setProperty(GROUP_ID, params.get(GROUP_ID))
    val dwdmemberSource = new FlinkKafkaConsumer010[DwdMember]("dwdmember", new DwdMemberDeserializationSchema, consumerProps)
    val dwdmemberpaymoneySource = new FlinkKafkaConsumer010[DwdMemberPayMoney]("dwdmemberpaymoney", new DwdMemberPayMoneyDeserializationSchema, consumerProps)
    val dwdmemberregtypeSource = new FlinkKafkaConsumer010[DwdMemberRegtype]("dwdmemberregtype", new DwdMemberRegtypeDeserializationSchema, consumerProps)
    dwdmemberSource.setStartFromEarliest()
    dwdmemberpaymoneySource.setStartFromEarliest()
    dwdmemberregtypeSource.setStartFromEarliest()

    //注册时间作为 事件时间 水位线设置为10秒
    val dwdmemberStream = env.addSource(dwdmemberSource).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[DwdMember](Time.seconds(10)) {
      override def extractTimestamp(element: DwdMember): Long = {
        element.register.toLong
      }
    })

    //创建时间作为 事件时间  水位线设置10秒
    val dwdmemberpaymoneyStream = env.addSource(dwdmemberpaymoneySource).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[DwdMemberPayMoney](Time.seconds(10)) {
      override def extractTimestamp(element: DwdMemberPayMoney): Long = {
        element.createtime.toLong
      }
    })
    //
    val dwdmemberregtypeStream = env.addSource(dwdmemberregtypeSource).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[DwdMemberRegtype](Time.seconds(10)) {
      override def extractTimestamp(element: DwdMemberRegtype): Long = {
        element.createtime.toLong
      }
    })

    //    用户表先关联注册表 以用户表为主表 用cogroup 实现left join
    val dwdmemberLeftJoinRegtyeStream = dwdmemberStream.coGroup(dwdmemberregtypeStream)
      .where(item => item.uid + "_" + item.dn).equalTo(item => item.uid + "_" + item.dn)
      //      .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
      //      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
      .window(TumblingEventTimeWindows.of(Time.minutes(10)))
      .trigger(CountTrigger.of(1))
      .apply(new MemberLeftJoinRegtype)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(10)) {
        override def extractTimestamp(element: String): Long = {
          val register = ParseJsonData.getJsonData(element).getString("register")
          register.toLong
        }
      })
    //再根据用户信息跟消费金额进行关联 用户表为主表进行 left join 根据uid和dn进行join
    val resultStream = dwdmemberLeftJoinRegtyeStream.coGroup(dwdmemberpaymoneyStream)
      .where(item => {
        val jsonObject = ParseJsonData.getJsonData(item)
        val uid = jsonObject.getString("uid")
        val dn = jsonObject.getString("dn")
        uid + "_" + dn
      }).equalTo(item => item.uid + "_" + item.dn)
      .window(TumblingEventTimeWindows.of(Time.minutes(10)))
      .trigger(CountTrigger.of(1))
      .apply(new resultStreamCoGroupFunction)
    //       val resultDStream = AsyncDataStream.unorderedWait(resultStream, new DimHbaseAsyncFunction, 10l, java.util.concurrent.TimeUnit.MINUTES, 12)
    val resultDStream = AsyncDataStream.unorderedWait(resultStream, new DimKuduAsyncFunction, 10l, java.util.concurrent.TimeUnit.MINUTES, 12)
    resultDStream.print()
    resultDStream.addSink(new DwsMemberKuduSink)
    env.execute()
  }

  class MemberLeftJoinRegtype extends CoGroupFunction[DwdMember, DwdMemberRegtype, String] {
    override def coGroup(first: lang.Iterable[DwdMember], second: lang.Iterable[DwdMemberRegtype], out: Collector[String]): Unit = {
      var bl = false
      val leftIterator = first.iterator()
      val rightIterator = second.iterator()
      while (leftIterator.hasNext) {
        val dwdMember = leftIterator.next()
        val jsonObject = new JSONObject()
        jsonObject.put("uid", dwdMember.uid)
        jsonObject.put("ad_id", dwdMember.ad_id)
        jsonObject.put("birthday", dwdMember.birthday)
        jsonObject.put("email", dwdMember.email)
        jsonObject.put("fullname", dwdMember.fullname)
        jsonObject.put("iconurl", dwdMember.iconurl)
        jsonObject.put("lastlogin", dwdMember.lastlogin)
        jsonObject.put("mailaddr", dwdMember.mailaddr)
        jsonObject.put("memberlevel", dwdMember.memberlevel)
        jsonObject.put("password", dwdMember.password)
        jsonObject.put("phone", dwdMember.phone)
        jsonObject.put("qq", dwdMember.qq)
        jsonObject.put("register", dwdMember.register)
        jsonObject.put("regupdatetime", dwdMember.regupdatetime)
        jsonObject.put("unitname", dwdMember.unitname)
        jsonObject.put("userip", dwdMember.userip)
        jsonObject.put("zipcode", dwdMember.zipcode)
        jsonObject.put("dt", dwdMember.dt)
        jsonObject.put("dn", dwdMember.dn)
        while (rightIterator.hasNext) {
          val dwdMemberRegtype = rightIterator.next()
          jsonObject.put("appkey", dwdMemberRegtype.appkey)
          jsonObject.put("appregurl", dwdMemberRegtype.appregurl)
          jsonObject.put("bdp_uuid", dwdMemberRegtype.bdp_uuid)
          jsonObject.put("createtime", dwdMemberRegtype.createtime)
          jsonObject.put("isranreg", dwdMemberRegtype.isranreg)
          jsonObject.put("regsource", dwdMemberRegtype.regsource)
          jsonObject.put("websiteid", dwdMemberRegtype.websiteid)
          bl = true
          out.collect(jsonObject.toJSONString)
        }
        if (!bl) {
          jsonObject.put("appkey", "")
          jsonObject.put("appregurl", "")
          jsonObject.put("bdp_uuid", "")
          jsonObject.put("createtime", "")
          jsonObject.put("isranreg", "")
          jsonObject.put("regsource", "")
          jsonObject.put("websiteid", "")
          out.collect(jsonObject.toJSONString)
        }
      }
    }
  }

  class resultStreamCoGroupFunction extends CoGroupFunction[String, DwdMemberPayMoney, String] {
    override def coGroup(first: lang.Iterable[String], second: lang.Iterable[DwdMemberPayMoney], out: Collector[String]): Unit = {
      var bl = false
      val leftIterator = first.iterator()
      val rightIterator = second.iterator()
      while (leftIterator.hasNext) {
        val jsonObject = ParseJsonData.getJsonData(leftIterator.next())
        while (rightIterator.hasNext) {
          val dwdMemberPayMoney = rightIterator.next()
          jsonObject.put("paymoney", dwdMemberPayMoney.paymoney)
          jsonObject.put("siteid", dwdMemberPayMoney.siteid)
          jsonObject.put("vip_id", dwdMemberPayMoney.vip_id)
          bl = true
          out.collect(jsonObject.toJSONString)
        }
        if (!bl) {
          jsonObject.put("paymoney", "")
          jsonObject.put("siteid", "")
          jsonObject.put("vip_id", "")
          out.collect(jsonObject.toJSONString)
        }
      }
    }
  }

}
