package com.bjke.app.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bjke.bean.TableProcess;
import com.bjke.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    private Connection connection;
    private OutputTag<JSONObject> outputTag;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    private PreparedStatement preparedStatement;

    public TableProcessFunction(OutputTag<JSONObject> outputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.outputTag = outputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        //TODO: 读取MySQL配置表放入内存【Map】！
    }

    // value:{"db":"","ts_ms":{},"before":{},"after":{},"op":{},"source":{}}
    // table_process的数据
    @Override
    public void processBroadcastElement(String value, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
        // 1. 获取并解析数据
        JSONObject jsonObject = JSON.parseObject(value);
        String data = jsonObject.getString("after");
        TableProcess tableProcess = JSON.parseObject(data, TableProcess.class);
        // 2。建表
        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
            checkTable(tableProcess.getSinkTable(), tableProcess.getSinkColumns(), tableProcess.getSinkPk(), tableProcess.getSinkExtend());
        }
        // 3。写入状态，广播出去
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();
        broadcastState.put(key, tableProcess);
    }


    // value:{"db":"","ts_ms":{},"before":{},"after":{},"op":{},"source":{}}
    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        // 1. 获取状态数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        // 获取表名和操作类型
        String key = value.getJSONObject("source").getString("table") + "-" + value.getString("op");
        // 取出对应的配置信息数据
        TableProcess tableProcess = broadcastState.get(key);
        if (tableProcess != null) {
            // 2。过滤字段
            JSONObject data = value.getJSONObject("after");
            filterColumn(data, tableProcess.getSinkColumns());
            // 3。分流
            // 将输出表/主题写入value
            value.put("sinkTable", tableProcess.getSinkTable());
            String sinkType = tableProcess.getSinkType();
            if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)) {
                // kafka数据写入主流
                out.collect(value);
            } else if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)) {
                // Hbase数据，写入侧输出流
                ctx.output(outputTag, value);
            }
        } else {
            //TODO: 从内存的Map中尝试获取数据
            System.out.println("该组合key:" + key + "不存在！");
        }
    }

    /**
     * @param data        {"id": 12,"tm_name": "1123","logo_url": null}
     * @param sinkColumns {"id": 12,"tm_name": "1123"}
     */
    private void filterColumn(JSONObject data, String sinkColumns) {
        //保留的数据字段
        String[] fields = sinkColumns.split(",");
        List<String> fieldList = Arrays.asList(fields);
//        Iterator<Map.Entry<String, Object>> iterator = data.entrySet().iterator();
//        while (iterator.hasNext()) {
//            Map.Entry<String, Object> next = iterator.next();
//            if (!fieldList.contains(next.getKey())) {
//                iterator.remove();
//            }
//        }
        data.entrySet().removeIf(next -> !fieldList.contains(next.getKey()));
    }

    /**
     * Phoenix 建表
     *
     * @param sinkTable   表名 test
     * @param sinkColumns 表名字段 id,name,sex
     * @param sinkPk      表主键 id
     * @param sinkExtend  表扩展字段 ""
     *                    create table if not exists db.tn(id varchar primary key,name
     *                    varchar,sex varchar) ...
     */
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        //给主键以及扩展字段赋默认值
        try {

            if (sinkPk == null) {
                sinkPk = "id";
            }
            if (sinkExtend == null) {
                sinkExtend = "";
            }
            StringBuilder createTableSQL = new StringBuilder("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");
            String[] fields = sinkColumns.split(",");
            for (int i = 0; i < fields.length; i++) {
                String filed = fields[i];
                // 判断是否为主键
                if (sinkPk.equals(filed)) {
                    createTableSQL.append(filed).append(" varchar primary key ");
                } else {
                    createTableSQL.append(filed).append(" varchar ");
                }
                // 判断是否为最后一个字段，如果不是，则添加","
                if (i < fields.length - 1) {
                    createTableSQL.append(",");
                }
            }
            createTableSQL.append(")").append(sinkExtend);
            System.out.println(createTableSQL);
            // 预编译SQL
            preparedStatement = connection.prepareStatement(createTableSQL.toString());
            // 执行
            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("创建 Phoenix 表" + sinkTable + "失败!");
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
