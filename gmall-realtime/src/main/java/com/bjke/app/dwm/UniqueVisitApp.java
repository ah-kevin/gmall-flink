package com.bjke.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.bjke.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;

/**
 * 数据流: web/app -> nginx -> SpringBoot -> kafka(ods) -> FlinkApp -> kafka(dwd)->FlinkApp -> kafka(dwm)
 * <p>
 * 程  序: mockLog -> nginx -> Logger.sh  -> kafka(zk) -> BaseLogApp -> kafka -> UniqueVisitApp -> kafka
 */
public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  //生产环境应该设置为Kafka主题的分区数

        //2.Flink-CDC将读取binlog的位置信息以状态的方式保存在CK,如果想要做到断点续传,需要从Checkpoint或者Savepoint启动程序
        //2.1 开启Checkpoint,每隔5秒钟做一次CK
        //  env.enableCheckpointing(5000L);
        //  //2.2 指定CK的一致性语义
        //  env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //  //2.3 设置任务关闭的时候保留最后一次CK数据
        //  env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //  //2.4 指定从CK自动重启策略
        //  env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));
        //  //2.5 设置状态后端
        //  env.setStateBackend(new FsStateBackend("hdfs://hadoop1:8020/flinkCDC"));
        //  //2.6 设置访问HDFS的用户名
        //  System.setProperty("HADOOP_USER_NAME", "root");

        // 2。读取kafka dwd_page_log 主题的数据
        String groupId = "unique_visit_app";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";
        DataStreamSource<String> kafkaDS$ = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));
        // 3。转换为json对象
        SingleOutputStreamOperator<JSONObject> jsonObj$ = kafkaDS$.map(JSON::parseObject);
        // 4。过滤数据 状态编程 只保留每个mid每天第一次登录的数据
        KeyedStream<JSONObject, String> keyedStream = jsonObj$.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> uvDS$ = keyedStream.filter(new RichFilterFunction<JSONObject>() {
            private ValueState<String> dataState;
            private SimpleDateFormat simpleDateFormat;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("data-state", String.class);
                // 设置状态的超时时间以及更新时间
                StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.hours(24))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                valueStateDescriptor.enableTimeToLive(stateTtlConfig);
                dataState = getRuntimeContext().getState(valueStateDescriptor);
                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                // 取出上一条页面信息
                String lastPageID = value.getJSONObject("page").getString("last_page_id");
                // 判断上一条耶main是否为null
                if (lastPageID == null || lastPageID.length() <= 0) {
                    // 取出状态数据
                    String lastDate = dataState.value();
                    // 取出今天的日期
                    String currentDate = simpleDateFormat.format(value.getLong("ts"));
                    // 判断两个日期是否相同
                    if (!currentDate.equals(lastDate)) {
                        // 更新状态
                        dataState.update(currentDate);
                        return true;
                    }
                }
                return false;
            }
        });
        // 5。将数据写入kafka
        uvDS$.print();
        uvDS$.map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));
        // 6。 启动任务
        env.execute("UniqueVisitApp");
    }
}
