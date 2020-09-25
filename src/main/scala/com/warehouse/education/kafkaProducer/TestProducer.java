package com.warehouse.education.kafkaProducer;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class TestProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "cdh02:9092,cdh03:9092,cdh04:9092");
        props.put("acks", "-1");
        props.put("batch.size", "16384");
        props.put("linger.ms", "10");
        props.put("buffer.memory", "33554432");
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 10; i++) {
            BaseMemberKafkaProducer.GdmMember gdmMember = BaseMemberKafkaProducer.MemberLog.generateLog(String.valueOf(i));
            String jsonString = JSON.toJSONString(gdmMember);
            producer.send(new ProducerRecord<String, String>("test1", jsonString));
        }
        for (int i = 0; i < 10; i++) {
            BaseMemberRegtypeProducer.GdmMemberRegType memberRegType = BaseMemberRegtypeProducer.GdmMemberRegTypeLog.generateLog(String.valueOf(i), "webA");
            String jsonString = JSON.toJSONString(memberRegType);
            producer.send(new ProducerRecord<String, String>("test2", jsonString));
        }
        producer.flush();
        producer.close();
    }
}
