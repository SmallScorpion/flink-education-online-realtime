package com.warehouse.education.kafkaProducer;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 广告信息基础表
 */
public class BaseAdLogKafkaProducer {
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
            GdmBaseAd gdmBaseAd = GdmBaseAdLog.generateLog(String.valueOf(i));
            String jsonString = JSON.toJSONString(gdmBaseAd);
            producer.send(new ProducerRecord<String, String>("basead", jsonString));
        }
        producer.flush();
        producer.close();
    }

    public static class GdmBaseAd {
        private String adid;
        private String adname;
        private String dn;

        public String getAdid() {
            return adid;
        }

        public void setAdid(String adid) {
            this.adid = adid;
        }

        public String getAdname() {
            return adname;
        }

        public void setAdname(String adname) {
            this.adname = adname;
        }

        public String getDn() {
            return dn;
        }

        public void setDn(String dn) {
            this.dn = dn;
        }
    }

    public static class GdmBaseAdLog {
        public static GdmBaseAd generateLog(String adid) {
            GdmBaseAd basead = new GdmBaseAd();
            basead.setAdid(adid);
            basead.setAdname("注册弹窗广告" + adid);
            basead.setDn("webA");
            return basead;
        }
    }
}

