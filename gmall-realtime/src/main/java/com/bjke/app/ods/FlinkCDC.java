package com.bjke.app.ods;

import com.bjke.utils.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDC {
    public static void main(String[] args) throws Exception {
        // 1。获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 开启ck并指定状态后端为FS
        env.enableCheckpointing(3000);

        // 2。通过flinkCDC构建sourceFunction并读取数据
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("172.19.1.35")
                .port(3306)
                .databaseList("gmall-flink")
                .tableList() // 如果不添加该参数，则消费制定数据库中所有表的数据，如果制定，指定方式为table.name
                .username("root")
                .password("890728")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.latest())
                .build();
        DataStreamSource<String> streamSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");

        // 3。 打印数据并写入kafka
        streamSource.print();
        String sinkTopic = "ods_base_db";
        streamSource.addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));
        // 4。启动
        env.execute("FlinkCDC");
    }
}
