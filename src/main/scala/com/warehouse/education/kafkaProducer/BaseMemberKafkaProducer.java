package com.warehouse.education.kafkaProducer;

/**
 * 用户基础表
 */
import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Random;

public class BaseMemberKafkaProducer {
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
            GdmMember gdmMember = MemberLog.generateLog(String.valueOf(i));
            String jsonString = JSON.toJSONString(gdmMember);
            producer.send(new ProducerRecord<String, String>("member", jsonString));
        }
        producer.flush();
        producer.close();
    }

    public static class GdmMember {

        private String uid;  //用户id
        private String password;  //密码
        private String email;
        private String username; //用户名
        private String fullname; //用户名
        private String birthday;
        private String phone;
        private String qq;
        private String ad_id;
        private String unitname;
        private String mailaddr;
        private String zipcode;
        private String kjlevel;
        private String register;
        private String memberlevel;
        private String paymoney;
        private String userip;
        private String regupdatetime;
        private String lastlogin;
        private String iconurl;
        private String dt;
        private String dn;


        public String getFullname() {
            return fullname;
        }

        public void setFullname(String fullname) {
            this.fullname = fullname;
        }

        public String getUid() {
            return uid;
        }

        public void setUid(String uid) {
            this.uid = uid;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getEmail() {
            return email;
        }

        public void setEmail(String email) {
            this.email = email;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getBirthday() {
            return birthday;
        }

        public void setBirthday(String birthday) {
            this.birthday = birthday;
        }

        public String getPhone() {
            return phone;
        }

        public void setPhone(String phone) {
            this.phone = phone;
        }

        public String getQq() {
            return qq;
        }

        public void setQq(String qq) {
            this.qq = qq;
        }

        public String getAd_id() {
            return ad_id;
        }

        public void setAd_id(String ad_id) {
            this.ad_id = ad_id;
        }

        public String getUnitname() {
            return unitname;
        }

        public void setUnitname(String unitname) {
            this.unitname = unitname;
        }

        public String getMailaddr() {
            return mailaddr;
        }

        public void setMailaddr(String mailaddr) {
            this.mailaddr = mailaddr;
        }

        public String getZipcode() {
            return zipcode;
        }

        public void setZipcode(String zipcode) {
            this.zipcode = zipcode;
        }

        public String getKjlevel() {
            return kjlevel;
        }

        public void setKjlevel(String kjlevel) {
            this.kjlevel = kjlevel;
        }

        public String getRegister() {
            return register;
        }

        public void setRegister(String register) {
            this.register = register;
        }

        public String getMemberlevel() {
            return memberlevel;
        }

        public void setMemberlevel(String memberlevel) {
            this.memberlevel = memberlevel;
        }

        public String getPaymoney() {
            return paymoney;
        }

        public void setPaymoney(String paymoney) {
            this.paymoney = paymoney;
        }

        public String getUserip() {
            return userip;
        }

        public void setUserip(String userip) {
            this.userip = userip;
        }

        public String getRegupdatetime() {
            return regupdatetime;
        }

        public void setRegupdatetime(String regupdatetime) {
            this.regupdatetime = regupdatetime;
        }

        public String getLastlogin() {
            return lastlogin;
        }

        public void setLastlogin(String lastlogin) {
            this.lastlogin = lastlogin;
        }

        public String getIconurl() {
            return iconurl;
        }

        public void setIconurl(String iconurl) {
            this.iconurl = iconurl;
        }

        public String getDn() {
            return dn;
        }

        public void setDn(String dn) {
            this.dn = dn;
        }

        public String getDt() {
            return dt;
        }

        public void setDt(String dt) {
            this.dt = dt;
        }
    }

    public static class MemberLog {


        private static String[] dns = new String[]{"webA", "webB", "webC"};
        private static String[] type = new String[]{"insert", "update"};
        private static int[][] range = {{607649792, 608174079},//36.56.0.0-36.63.255.255
                {1038614528, 1039007743},//61.232.0.0-61.237.255.255
                {1783627776, 1784676351},//106.80.0.0-106.95.255.255
                {2035023872, 2035154943},//121.76.0.0-121.77.255.255
                {2078801920, 2079064063},//123.232.0.0-123.235.255.255
                {-1950089216, -1948778497},//139.196.0.0-139.215.255.255
                {-1425539072, -1425014785},//171.8.0.0-171.15.255.255
                {-1236271104, -1235419137},//182.80.0.0-182.92.255.255
                {-770113536, -768606209},//210.25.0.0-210.47.255.255
                {-569376768, -564133889}, //222.16.0.0-222.95.255.255
        };

        public static GdmMember generateLog(String uid) {
            GdmMember member = new GdmMember();
            Random rand = new Random();
            member.setAd_id(rand.nextInt(10) + "");
            String birthday = new SimpleDateFormat("yyyy-MM-dd")
                    .format(RondomDate.randomDate("1960-01-01 00:00:00", "2000-01-01 00:00:00"));
            member.setDt(DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDateTime.now().minusDays(8)));
            member.setDn(dns[0]);
            member.setUid(uid);
            member.setPassword("123456");
            member.setEmail("test@126.com");
            member.setFullname("王" + uid);
            member.setPhone("13711235451");
            member.setBirthday(birthday);
            member.setQq("10000");
            member.setUnitname("-");
            member.setMailaddr("-");
            member.setZipcode("-");
//            String registerdata = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//                    .format(RondomDate.randomDate("2019-06-30 10:00:00", "2019-06-30 11:00:00"));
            member.setRegister(String.valueOf(System.currentTimeMillis()));
            member.setMemberlevel((rand.nextInt(8) + 1) + "");
            member.setPaymoney("-");
            member.setUserip("-");
            int index = rand.nextInt(10);
            String ip = num2ip(range[index][0]
                    + new Random().nextInt(range[index][1] - range[index][0]));
            member.setUserip(ip);
            member.setRegupdatetime("-");
            member.setLastlogin("-");
            member.setIconurl("-");
            return member;
        }

        public static String num2ip(int ip) {
            int[] b = new int[4];
            String x = "";

            b[0] = (int) ((ip >> 24) & 0xff);
            b[1] = (int) ((ip >> 16) & 0xff);
            b[2] = (int) ((ip >> 8) & 0xff);
            b[3] = (int) (ip & 0xff);
            x = Integer.toString(b[0]) + "." + Integer.toString(b[1]) + "."
                    + Integer.toString(b[2]) + "." + Integer.toString(b[3]);

            return x;
        }
    }
}
