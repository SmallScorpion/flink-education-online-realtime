package com.warehouse.education.kafkaProducer;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Properties;

/**
 * Vip级别基础表
 */
public class BaseMemberVipProducer {

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
        for (int i = 0; i < 5; i++) {
            GdmPcenterMemViplevel memViplevel= GdmPcenterMemViplevelLog.generateLog(String.valueOf(i));
            String jsonString = JSON.toJSONString(memViplevel);
            producer.send(new ProducerRecord<String, String>("membervip", jsonString));
        }
        producer.flush();
        producer.close();
    }
    public static  class GdmPcenterMemViplevelLog {

        private static String[] vipLevels = new String[]{"普通会员", "白金", "银卡", "金卡", "钻石"};

        public static GdmPcenterMemViplevel generateLog(String vipid) {
            GdmPcenterMemViplevel memViplevel = new GdmPcenterMemViplevel();
            memViplevel.setDiscountval("-");
            memViplevel.setDn("webA");
            String time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                    .format(RondomDate.randomDate("2015-01-01 00:00:00", "2016-06-30 00:00:00"));
            String time2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                    .format(RondomDate.randomDate("2016-01-01 00:00:00", "2019-06-30 00:00:00"));
            memViplevel.setLast_modify_time("");
            memViplevel.setStart_time(time);
            memViplevel.setEnd_time(time2);
            memViplevel.setLast_modify_time(time2);
            memViplevel.setMax_free("-");
            memViplevel.setMin_free("-");
            memViplevel.setNext_level("-");
            memViplevel.setVip_id(vipid);
            memViplevel.setOperator("update");
            memViplevel.setVip_level(vipLevels[Integer.parseInt(vipid)]);
            return memViplevel;
        }

    }
    public static class GdmPcenterMemViplevel {

        private String vip_id;
        private String vip_name;
        private String vip_level;
        private String min_free;
        private String max_free;
        private String start_time;
        private String end_time;
        private String next_level;
        private String discountval;
        private String last_modify_time;
        private String operator;
        private String siteid;
        private String dn;

        public String getVip_id() {
            return vip_id;
        }

        public void setVip_id(String vip_id) {
            this.vip_id = vip_id;
        }

        public String getVip_name() {
            return vip_name;
        }

        public void setVip_name(String vip_name) {
            this.vip_name = vip_name;
        }

        public String getVip_level() {
            return vip_level;
        }

        public void setVip_level(String vip_level) {
            this.vip_level = vip_level;
        }

        public String getMin_free() {
            return min_free;
        }

        public void setMin_free(String min_free) {
            this.min_free = min_free;
        }

        public String getMax_free() {
            return max_free;
        }

        public void setMax_free(String max_free) {
            this.max_free = max_free;
        }

        public String getStart_time() {
            return start_time;
        }

        public void setStart_time(String start_time) {
            this.start_time = start_time;
        }

        public String getEnd_time() {
            return end_time;
        }

        public void setEnd_time(String end_time) {
            this.end_time = end_time;
        }

        public String getNext_level() {
            return next_level;
        }

        public void setNext_level(String next_level) {
            this.next_level = next_level;
        }

        public String getDiscountval() {
            return discountval;
        }

        public void setDiscountval(String discountval) {
            this.discountval = discountval;
        }

        public String getLast_modify_time() {
            return last_modify_time;
        }

        public void setLast_modify_time(String last_modify_time) {
            this.last_modify_time = last_modify_time;
        }

        public String getOperator() {
            return operator;
        }

        public void setOperator(String operator) {
            this.operator = operator;
        }

        public String getSiteid() {
            return siteid;
        }

        public void setSiteid(String siteid) {
            this.siteid = siteid;
        }

        public String getDn() {
            return dn;
        }

        public void setDn(String dn) {
            this.dn = dn;
        }
    }

}
