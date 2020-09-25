package com.warehouse.education.model

import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

class TopicAndValueDeserializationSchema extends KafkaDeserializationSchema[TopicAndValue] {
  //表是流最后一条元素
  override def isEndOfStream(t: TopicAndValue): Boolean = {
    false
  }

  override def deserialize(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]): TopicAndValue = {
    new TopicAndValue(consumerRecord.topic(), new String(consumerRecord.value(), "utf-8"))
  }

  //告诉flink 数据类型
  override def getProducedType: TypeInformation[TopicAndValue] = {
    TypeInformation.of(new TypeHint[TopicAndValue] {})
  }
}

