package com.bjke.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bjke.app.function.TableProcessFunction;
import com.bjke.bean.TableProcess;
import com.bjke.utils.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        // 1。获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  //生产环境应该设置为Kafka主题的分区数

        //2.Flink-CDC将读取binlog的位置信息以状态的方式保存在CK,如果想要做到断点续传,需要从Checkpoint或者Savepoint启动程序
        //2.1 开启Checkpoint,每隔5秒钟做一次CK
//        env.enableCheckpointing(5000L);
//        //2.2 指定CK的一致性语义
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        //2.3 设置任务关闭的时候保留最后一次CK数据
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        //2.4 指定从CK自动重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));
//        //2.5 设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop1:8020/flinkCDC"));
//        //2.6 设置访问HDFS的用户名
//        System.setProperty("HADOOP_USER_NAME", "root");

        // 2。消费kafka ods_base_db主题数据创建流
        String sourceTopic = "ods_base_db";
        String groupId = "base_db_app";
        DataStreamSource<String> kafka$ = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));
        // 3. 将每行数据转换为json对象并过滤（delete）主流
        SingleOutputStreamOperator<JSONObject> jsonObj$ = kafka$.map(JSON::parseObject)
                .filter(new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        // 取出数据的操作类型
                        String type = value.getString("op");
                        return !"d".equals(type);
                    }
                });
        // 4。使用flink-cdc消费配置表并处理完成 广播流
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .databaseList("gmall-realtime")
                .tableList("table_process") // 如果不添加该参数，则消费制定数据库中所有表的数据，如果制定，指定方式为table.name
                .username("root")
                .password("890728")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
        DataStreamSource<String> tableProcessSource$ = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "table_process_source");
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcast$ = tableProcessSource$.broadcast(mapStateDescriptor);

        // 5。连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connect$ = jsonObj$.connect(broadcast$);
        // 6。分流-> 处理数据 广播流数据，主流数据（根据广播流数据进行处理）
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbase-tag") {
        };
        SingleOutputStreamOperator<JSONObject> kafka = connect$.process(new TableProcessFunction(hbaseTag, mapStateDescriptor));
        // 7。提取kafka流数据和HBase流数据
        DataStream<JSONObject> hbase = kafka.getSideOutput(hbaseTag);
        // 8。将kafka数据写入kafka主题，将Hbase数据写入Phoenix表
        kafka.print("Kafka>>>>>>>");
        hbase.print("Hbase>>>>>>>");
        // 9。启动任务
        env.execute("BaseDBApp");
    }
}
