package com.warehouse.education.kafkaProducer;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Random;

/**
 * 用户注册跳转地址表
 */
public class BaseMemberRegtypeProducer {

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
        for (int i = 0; i < 1000000; i++) {
            GdmMemberRegType memberRegType = GdmMemberRegTypeLog.generateLog(String.valueOf(i), "webA");
            String jsonString = JSON.toJSONString(memberRegType);
            producer.send(new ProducerRecord<String, String>("memberregtype", jsonString));
        }
        producer.flush();
        producer.close();
    }

    public static class GdmMemberRegTypeLog {

        private static String[] webAappregUrl = new String[]{
                "http:www.webA.com/product/register/index.html", "http:www.webA.com/sale/register/index.html",
                "http:www.webA.com/product10/register/aa/index.html",
                "http:www.webA.com/hhh/wwww/index.html"};
        private static String[] webBappregUrl = new String[]{
                "http:www.webB.com/product/register/index.html", "http:www.webB.com/sale/register/index.html",
                "http:www.webB.com/hhh/wwww/index.html"};
        private static String[] webcappregUrl = new String[]{
                "http:www.webC.com/product/register/index.html", "http:www.webB.com/sale/register/index.html",
                "http:www.webC.com/product52/register/ac/index.html"};


        public static GdmMemberRegType generateLog(String uid, String dn) {
            GdmMemberRegType memberRegType = new GdmMemberRegType();
            memberRegType.setAppkey("-");
            Random random = new Random();
            String url = "";
            int index = random.nextInt(4);
            switch (dn) {
                case "webA":
                    url = webAappregUrl[index];
                    break;
                case "webB":
                    url = webBappregUrl[index];
                    break;
                case "webC":
                    url = webcappregUrl[index];
                    break;
            }
            memberRegType.setAppregurl(url);
            memberRegType.setBdp_uuid("-");
//            String createTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//                    .format(RondomDate.randomDate("2019-06-30 10:00:00", "2019-06-30 11:00:00"));
            memberRegType.setCreatetime(String.valueOf(System.currentTimeMillis()));
            memberRegType.setDt(DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDateTime.now().minusDays(8)));
            memberRegType.setDn(dn);
            memberRegType.setDomain("-");
            memberRegType.setIsranreg("-");
            memberRegType.setDomain("-");
            memberRegType.setWebsiteid(String.valueOf(random.nextInt(5)));
            memberRegType.setRegsource(String.valueOf(random.nextInt(5)));
            memberRegType.setUid(uid);
            return memberRegType;
        }
    }

    public static class GdmMemberRegType {

        private String reflagid;
        private String uid;
        private String regsource; //注册来源 1.pc 2.mobile 3.app 4.wechat
        private String appkey;
        private String appregurl;
        private String websiteid;
        private String domain;
        private String isranreg;
        private String bdp_uuid;
        private String createtime;
        private String dt;
        private String dn;


        public String getDt() {
            return dt;
        }

        public void setDt(String dt) {
            this.dt = dt;
        }

        public String getReflagid() {
            return reflagid;
        }

        public void setReflagid(String reflagid) {
            this.reflagid = reflagid;
        }

        public String getUid() {
            return uid;
        }

        public void setUid(String uid) {
            this.uid = uid;
        }

        public String getRegsource() {
            return regsource;
        }

        public void setRegsource(String regsource) {
            this.regsource = regsource;
        }

        public String getAppkey() {
            return appkey;
        }

        public void setAppkey(String appkey) {
            this.appkey = appkey;
        }

        public String getAppregurl() {
            return appregurl;
        }

        public void setAppregurl(String appregurl) {
            this.appregurl = appregurl;
        }

        public String getWebsiteid() {
            return websiteid;
        }

        public void setWebsiteid(String websiteid) {
            this.websiteid = websiteid;
        }

        public String getDomain() {
            return domain;
        }

        public void setDomain(String domain) {
            this.domain = domain;
        }

        public String getIsranreg() {
            return isranreg;
        }

        public void setIsranreg(String isranreg) {
            this.isranreg = isranreg;
        }

        public String getBdp_uuid() {
            return bdp_uuid;
        }

        public void setBdp_uuid(String bdp_uuid) {
            this.bdp_uuid = bdp_uuid;
        }

        public String getCreatetime() {
            return createtime;
        }

        public void setCreatetime(String createtime) {
            this.createtime = createtime;
        }

        public String getDn() {
            return dn;
        }

        public void setDn(String dn) {
            this.dn = dn;
        }
    }

}
