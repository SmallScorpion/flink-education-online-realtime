package com.warehouse.education.model

import com.google.gson.Gson
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

class DwdMemberRegtypeDeserializationSchema extends KafkaDeserializationSchema[DwdMemberRegtype] {
  override def isEndOfStream(nextElement: DwdMemberRegtype): Boolean = false

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): DwdMemberRegtype = {
    val gson = new Gson()
    gson.fromJson(new String(record.value(), "utf-8"), classOf[DwdMemberRegtype])
  }

  override def getProducedType: TypeInformation[DwdMemberRegtype] = {
    TypeInformation.of(new TypeHint[DwdMemberRegtype] {})
  }
}

