package com.warehouse.education.kafkaProducer;

import java.text.SimpleDateFormat;
import java.util.Date;


public class RondomDate {

  public void testRondomDate() {

  }

  public static Date randomDate(String beginDate, String endDate) {
    try {
      SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      Date start = format.parse(beginDate);  // 构造开始日期
      Date end = format.parse(endDate);  // 构造结束日期
      if (start.getTime() >= end.getTime()) {
        return null;
      }
      long date = random(start.getTime(), end.getTime());
      return new Date(date);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  private static long random(long begin, long end) {
    long rtn = begin + (long) (Math.random() * (end - begin));
    if (rtn == begin || rtn == end) {
      return random(begin, end);
    }
    return rtn;
  }
}
