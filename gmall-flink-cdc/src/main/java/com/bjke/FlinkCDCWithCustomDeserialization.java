package com.bjke;


import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDCWithCustomDeserialization {
    public static void main(String[] args) throws Exception {
        // 1。获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2。通过flinkCDC构建sourceFunction并读取数据
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .databaseList("gmall-flink")
                .tableList("gmall-flink.base_trademark") // 如果不添加该参数，则消费制定数据库中所有表的数据，如果制定，指定方式为table.name
                .username("root")
                .password("890728")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        DataStreamSource<String> streamSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");

        // 3。 打印
        streamSource
                .print();
        // 4。启动
        env.execute("FlinkCDC");
    }
}
