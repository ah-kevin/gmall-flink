package com.bjke.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.bjke.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * 数据流: web/app -> nginx -> SpringBoot -> kafka(ods) -> FlinkApp -> kafka(dwd)->FlinkApp -> kafka(dwm)
 * <p>
 * 程  序: mockLog -> nginx -> Logger.sh  -> kafka(zk) -> BaseLogApp -> kafka -> UserJumpDetailApp -> kafka
 */
public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        // 1。 获取执行环境
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

        // 2。读取kafka主题的数据创建流
        String sourceTopic = "dwd_page_log";
        String groupId = "userJumpDetailApp";
        String sinkTopic = "dwm_user_jump_detail";
        DataStreamSource<String> kafkaDS$ = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));
        // 3。将每行数据转换为JSON对象并提取时间戳生成Watermark
        SingleOutputStreamOperator<JSONObject> jsonObjDS$ = kafkaDS$.map(JSON::parseObject)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                }));
        // 4。定义模式序列
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                return lastPageId == null || lastPageId.length() <= 0;
            }
        }).next("next").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                return lastPageId == null || lastPageId.length() <= 0;
            }
        }).within(Time.seconds(10));

        // 5。将模式序列作用到流上
        PatternStream<JSONObject> pattern$ = CEP.pattern(jsonObjDS$
                .keyBy(json -> json.getJSONObject("common").getString("mid")), pattern);
        // 6。提取匹配上的和超时事件
        OutputTag<JSONObject> timeOutTag = new OutputTag<JSONObject>("timeOut") {
        };
        SingleOutputStreamOperator<JSONObject> select$ = pattern$.select(timeOutTag,
                new PatternTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                        return map.get("start").get(0);
                    }
                }, new PatternSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                        return map.get("start").get(0);
                    }
                });
        // 7。UNION两种事件
        DataStream<JSONObject> timeOut$ = select$.getSideOutput(timeOutTag);
        DataStream<JSONObject> union$ = select$.union(timeOut$);
        // 8。将数据写入kafka
        union$.print();
        union$.map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));
        // 9。 启动任务
        env.execute("UserJumpDetailApp");
    }
}
