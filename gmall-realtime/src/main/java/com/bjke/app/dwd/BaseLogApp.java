package com.bjke.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.bjke.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 数据流: web/app -> nginx -> SpringBoot -> kafka(ods) -> FlinkApp -> kafka(dwd)
 * <p>
 * 程  序: mockLog -> nginx -> Logger.sh  -> kafka(zk) -> BaseLogApp -> kafka
 */
public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境应该设置为Kafka主题的分区数
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

        // 2。 消费ods_base_log 主题数据创建流
        String sourceTopic = "ods_base_log";
        String groupId = "base_log_app";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));
        // 3。将每行数据转为json对象
//        kafkaDS.map(JSON::parseObject);
        OutputTag<String> outputTag = new OutputTag<String>("Dirty") {

        };
        SingleOutputStreamOperator<JSONObject> jsonObj$ = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
//                    e.printStackTrace();
                    // 发生异常,数据写入到侧输出流
                    ctx.output(outputTag, value);
                }
            }
        });
        // 打印脏数据
        jsonObj$.getSideOutput(outputTag).print("Dirty>>>>>>>>");
        // 4。新老用户校验  状态编程
        SingleOutputStreamOperator<JSONObject> jsonWithNewFlag$ = jsonObj$.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"))
                .map(new RichMapFunction<JSONObject, JSONObject>() {
                    private ValueState<String> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("value-state", String.class));
                    }

                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        // 获取数据中到"is_new" 标记
                        String isNew = value.getJSONObject("common").getString("is_new");
                        // 判断isNew标记是非为"1"
                        if ("1".equals(isNew)) {
                            // 获取状态数据
                            String state = valueState.value();
                            if (state != null) {
                                // 修改isNew标记
                                value.getJSONObject("common").put("is_new", "0");
                            } else {
                                valueState.update("1");
                            }
                        }
                        return value;
                    }
                });
        // 5。分流 侧输出流 页面：主流 启动：侧输出流 曝光：侧输出流
        OutputTag<String> startTag = new OutputTag<String>("start") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("display") {
        };
        SingleOutputStreamOperator<String> pageDS$ = jsonWithNewFlag$.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                // 获取启动日志字段
                String start = value.getString("start");
                if (start != null && start.length() > 0) {
                    // 将数据写入启动日志侧输出流
                    ctx.output(startTag, value.toJSONString());
                } else {
                    // 将数据写入页面主流
                    out.collect(value.toJSONString());
                    // 去除数据中的曝光数据
                    JSONArray displays = value.getJSONArray("displays");
                    String pageId = value.getJSONObject("page").getString("page_id");
                    if (displays != null && displays.size() > 0) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            // 添加页面id
                            display.put("page_id", pageId);

                            // 将输出写出到曝光侧输出流
                            ctx.output(displayTag, display.toJSONString());
                        }
                    }
                }
            }
        });
        // 6。提取侧输出流
        DataStream<String> start$ = pageDS$.getSideOutput(startTag);
        DataStream<String> display$ = pageDS$.getSideOutput(displayTag);

        // 7。将三个流进行打印并输出到对应到kafka到主题中
        start$.print("Start>>>>>>");
        pageDS$.print("Page>>>>>>");
        display$.print("display>>>>>>");

        start$.addSink(MyKafkaUtil.getKafkaProducer("dwd_start_log"));
        pageDS$.addSink(MyKafkaUtil.getKafkaProducer("dwd_page_log"));
        display$.addSink(MyKafkaUtil.getKafkaProducer("dwd_display_log"));

        // 8。启动任务
        env.execute("BaseLogApp");
    }
}
