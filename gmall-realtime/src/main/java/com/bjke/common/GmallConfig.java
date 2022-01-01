package com.bjke.common;

public class GmallConfig {
    //Phoenix 库名
    public static final String HBASE_SCHEMA = "GMALL_REALTIME";
    //Phoenix 驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    //Phoenix 连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop1:2181";
}