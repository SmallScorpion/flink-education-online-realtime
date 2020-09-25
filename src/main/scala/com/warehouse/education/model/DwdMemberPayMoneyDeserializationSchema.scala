package com.warehouse.education.model

import com.google.gson.Gson
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

class DwdMemberPayMoneyDeserializationSchema extends KafkaDeserializationSchema[DwdMemberPayMoney] {
  override def isEndOfStream(nextElement: DwdMemberPayMoney): Boolean = false

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): DwdMemberPayMoney = {
    val gson = new Gson()
    gson.fromJson(new String(record.value(), "utf-8"), classOf[DwdMemberPayMoney])
  }

  override def getProducedType: TypeInformation[DwdMemberPayMoney] = {
    TypeInformation.of(new TypeHint[DwdMemberPayMoney] {})
  }
}

