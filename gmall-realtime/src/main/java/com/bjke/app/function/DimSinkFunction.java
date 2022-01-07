package com.bjke.app.function;

import com.alibaba.fastjson.JSONObject;
import com.bjke.common.GmallConfig;
import com.bjke.utils.DimUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    private Connection connection = null;
    private PreparedStatement preparedStatement = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        connection.setAutoCommit(true);
    }

    /**
     * {"op":"u","sinkTable":"dwd_order_info","before":{"order_status":"1001","consignee_tel":"18956083728","id":26449,
     * "consignee":"sss","total_amount":"AIeh4A==","user_id":2021},"after":{"user_id":2021,"id":26449},
     * "source":{"server_id":1,"version":"1.5.4.Final","file":"mysql-bin.000003","connector":"mysql","pos":4377,
     * "name":"mysql_binlog_source","row":0,"ts_ms":1641104888000,"snapshot":"false","db":"gmall-flink",
     * "table":"order_info"},"ts_ms":1641104888159}
     * <p>
     */
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        try {
            String sinkTable = value.getString("sinkTable");
            // 获取sql语句
            JSONObject after = value.getJSONObject("after");
            String upsertSql = genUpsertSql(sinkTable, after);
            System.out.println(upsertSql);
            // 预编译SQL
            PreparedStatement preparedStatement = connection.prepareStatement(upsertSql);
            // 判断如果当前数据为更新操作，则先删除redis中的数据
            if ("u".equals(value.getString("op"))) {
                DimUtil.delRedisDimInfo(sinkTable.toUpperCase(), after.getString("id"));
            }
            // 执行插入操作
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }

    //  SQL: upsert into db.tn(id,tm_name) values('...','...')
    private String genUpsertSql(String sinkTable, JSONObject data) {
        Set<String> keySet = data.keySet();
        Collection<Object> values = data.values();
        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "(" +
                StringUtils.join(keySet, ",") + ") values ('" +
                StringUtils.join(values, "','") + "')";
    }

    @Override
    public void close() throws Exception {
        if (connection != null) connection.close();
        if (preparedStatement != null) preparedStatement.close();
    }
}
