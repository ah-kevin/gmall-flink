package com.bjke;


import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkCDCWithSQL {
    public static void main(String[] args) throws Exception {
        // 1。获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 2。DDL方式建表
        tableEnv.executeSql("CREATE TABLE mysql_binlog (" +
                " id INT NOT NULL," +
                " tm_name STRING," +
                " logo_url STRING," +
                " PRIMARY KEY(id) NOT ENFORCED" +
                ") WITH (" +
                " 'connector' = 'mysql-cdc'," +
                " 'hostname' = 'localhost'," +
                " 'port' = '3306'," +
                " 'username' = 'root'," +
                " 'password' = '890728'," +
                " 'database-name' = 'gmall-flink'," +
                " 'table-name' = 'base_trademark'" +
                ")");

        // 3。查询数据
        Table table = tableEnv.sqlQuery("select * from mysql_binlog");
        // 4。将动态表转为流
        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(table, Row.class);
        retractStream.print();
        // 5。启动任务
        env.execute("FlinkCDCWithSQL");
    }
}