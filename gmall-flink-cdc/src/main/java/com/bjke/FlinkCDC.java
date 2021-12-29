package com.bjke;


import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDC {
    public static void main(String[] args) throws Exception {
        // 1。获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        Configuration configuration = new Configuration();
//        configuration.setInteger(RestOptions.PORT,18081);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);

        // 开启ck并指定状态后端为FS
//        System.setProperty("HADOOP_USER_NAME", "root");
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop1:8020/gmall-flink/ck");
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);
//        env.enableCheckpointing(5000L);

        env.enableCheckpointing(3000);

        // 2。通过flinkCDC构建sourceFunction并读取数据
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("172.19.1.35")
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
//                .setParallelism(4) // set 4 parallel source tasks
//                .print()
//                .setParallelism(1);// use parallelism 1 for sink to keep message ordering
        // 4。启动
        env.execute("FlinkCDC");
    }
}
