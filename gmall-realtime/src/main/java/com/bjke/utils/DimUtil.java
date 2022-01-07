package com.bjke.utils;

import com.alibaba.fastjson.JSONObject;
import com.bjke.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

public class DimUtil {
    public static JSONObject getDimInfo(Connection connection, String tableName, String id) throws Exception {
        // 查询phoenix前先查询Redis
        Jedis jedis = RedisUtil.getJedis();
        // DIM:DIM_USER_INFO:143
        String redisKey = "DIM:" + tableName + ":" + id;
        String dimInfoJsonStr = jedis.get(redisKey);
        if (dimInfoJsonStr != null) {
            // 归还链接
            jedis.close();
            // 重置过期时间
            jedis.expire(redisKey, 24 * 60 * 60L);
            // 返回结果
            return JSONObject.parseObject(dimInfoJsonStr);
        }

        // 拼接查询语句
        String querySql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id='" + id + "'";
        // 查询phoenix
        List<JSONObject> queryList = JdbcUtil.queryList(connection, querySql, JSONObject.class, false);
        // 返回结果
        JSONObject dimInfoJson = queryList.get(0);
        // 在返回之前，数据写入redis
        jedis.set(redisKey, dimInfoJson.toJSONString());
        jedis.expire(redisKey, 24 * 60 * 60L);
        jedis.close();
        return dimInfoJson;
    }

    public static void delRedisDimInfo(String tableName, String id) {
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + id;
        System.out.println(redisKey);
        jedis.del(redisKey);
        jedis.close();
    }

    public static void main(String[] args) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
//        long start = System.currentTimeMillis();
        System.out.println(getDimInfo(connection, "DIM_BASE_TRADEMARK", "13"));
//        delRedisDimInfo("DIM_BASE_TRADEMARK", "13");
//        long end = System.currentTimeMillis();
//        System.out.println(getDimInfo(connection, "DIM_USER_INFO", "951"));
//        long end2 = System.currentTimeMillis();
//        System.out.println(getDimInfo(connection, "DIM_USER_INFO", "951"));
//        long end3 = System.currentTimeMillis();
//        System.out.println(end - start);
//        System.out.println(end2 - end);
//        System.out.println(end3 - end2);
        connection.close();
    }
}
