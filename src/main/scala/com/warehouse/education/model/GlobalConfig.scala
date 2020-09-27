package com.warehouse.education.model

object GlobalConfig {
  val HBASE_ZOOKEEPER_QUORUM = "cdh02,cdh03,cdh04"
  val HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = "2181"

  val BOOTSTRAP_SERVERS = "cdh02:9092,cdh03:9092,cdh04:9092"
  val ACKS = "-1"

  val KUDU_MASTER = "cdh02"
  val KUDU_TABLE_DWDBASEAD = "impala::education.dwd_base_ad"
  val KUDU_TABLE_DWDBASEWEBSITE = "impala::education.dwd_base_website"
  val KUDU_TABLE_DWDVIPLEVEL = "impala::education.dwd_vip_level"
  val KUDU_TABLE_DWSMEMBER = "impala::education.dws_member"
}
