package com.warehouse.education.etl

import java.util.Properties

import com.warehouse.education.etl.DImJoinData.MemberLeftJoinRegtype
import com.warehouse.education.model.{DwdMember, DwdMemberDeserializationSchema, DwdMemberPayMoney, DwdMemberPayMoneyDeserializationSchema, DwdMemberRegtype, DwdMemberRegtypeDeserializationSchema}
import com.warehouse.education.util.ParseJsonData
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger

object TestCogroup {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(60000l)
    val checkpointConfig = env.getCheckpointConfig
    checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE) //仅仅一次
    checkpointConfig.setMinPauseBetweenCheckpoints(30000l) //设置checkpoint间隔时间30秒
    checkpointConfig.setCheckpointTimeout(10000l) //设置checkpoint超时时间
    checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION) //cancel时保留checkpoint
    //设置重启策略   重启3次 间隔10秒
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, org.apache.flink.api.common.time.Time.seconds(10)))
    val consumerProps = new Properties()
    consumerProps.setProperty("bootstrap.servers", "hadoop101:9092,hadoop102:9092,hadoop103:9092")
    consumerProps.setProperty("group.id", "test1")
    val test1 = new FlinkKafkaConsumer010[DwdMember]("test1", new DwdMemberDeserializationSchema, consumerProps)
    val test2 = new FlinkKafkaConsumer010[DwdMemberRegtype]("test2", new DwdMemberRegtypeDeserializationSchema, consumerProps)
    test1.setStartFromEarliest()
    test2.setStartFromEarliest()
    val dwdmemberStream = env.addSource(test1).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[DwdMember](Time.seconds(10)) {
      override def extractTimestamp(element: DwdMember): Long = {
        element.register.toLong
      }
    })
    val dwdmemberregtypeStream = env.addSource(test2).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[DwdMemberRegtype](Time.seconds(10)) {
      override def extractTimestamp(element: DwdMemberRegtype): Long = {
        element.createtime.toLong
      }
    })
    val dwdmemberLeftJoinRegtyeStream = dwdmemberStream.coGroup(dwdmemberregtypeStream)
      .where(item => item.uid + "_" + item.dn).equalTo(item => item.uid + "_" + item.dn)
      //      .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
      //      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .allowedLateness(Time.minutes(2))
      .apply(new MemberLeftJoinRegtype)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(10)) {
        override def extractTimestamp(element: String): Long = {
          val register = ParseJsonData.getJsonData(element).getString("register")
          register.toLong
        }
      })
    dwdmemberLeftJoinRegtyeStream.print()
    env.execute()
  }
}
