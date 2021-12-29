package com.bjke;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDC {
    public static void main(String[] args) throws Exception {
        // 1。获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2。通过flinkCDC构建sourceFunction并读取数据
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .databaseList("gmall-flink")
                .tableList() // 如果不添加该参数，则消费制定数据库中所有表的数据，如果制定，指定方式为table.name
                .username("root")
                .password("890728")
                .deserializer(new StringDebeziumDeserializationSchema())
                .build();
//        DataStreamSource<String> streamSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql source");
        DataStreamSource<String> streamSource = env.addSource(sourceFunction);
        // 3。 打印
        streamSource.print();
        // 4。启动
        env.execute("FlinkCDC");
    }
}
