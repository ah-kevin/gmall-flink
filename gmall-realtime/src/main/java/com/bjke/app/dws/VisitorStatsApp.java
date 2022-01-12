package com.bjke.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bjke.bean.VisitorStats;
import com.bjke.utils.DateTimeUtil;
import com.bjke.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;

/**
 * Desc: 访客主题宽表计算
 * <p>
 * ?要不要把多个明细的同样的维度统计在一起?
 * 因为单位时间内 mid 的操作数据非常有限不能明显的压缩数据量(如果是数据量够大，
 * 或者单位时间够长可以)
 * 所以用常用统计的四个维度进行聚合 渠道、新老用户、app 版本、省市区域
 * 度量值包括 启动、日活(当日首次启动)、访问页面数、新增用户数、跳出数、平均页
 * 面停留时长、总访问时长
 * 聚合窗口: 10秒
 * <p>
 * 各个数据在维度聚合前不具备关联性，所以先进行维度聚合 * 进行关联 这是一个 fulljoin
 * 可以考虑使用 flinksql 完成
 * 数据流: web/app -> nginx -> SpringBoot -> kafka(ods) -> FlinkApp -> kafka(dwd) -> FlinkApp -> kafka(dwm)-> FlinkApp-> ClickHouse
 * <p>
 * 程  序: mockLog -> nginx -> Logger.sh  -> kafka(zk) -> BaseLogApp -> kafka -> uv/uj -> kafka -> VisitorStatsApp -> ClickHouse
 */
public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  //生产环境应该设置为Kafka主题的分区数

        /*
        2.Flink-CDC将读取binlog的位置信息以状态的方式保存在CK,如果想要做到断点续传,需要从Checkpoint或者Savepoint启动程序
        2.1 开启Checkpoint,每隔5秒钟做一次CK
                env.enableCheckpointing(5000L);
                //2.2 指定CK的一致性语义
                env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
                //2.3 设置任务关闭的时候保留最后一次CK数据
                env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
                //2.4 指定从CK自动重启策略
                env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));
                //2.5 设置状态后端
                env.setStateBackend(new FsStateBackend("hdfs://hadoop1:8020/flinkCDC"));
                //2.6 设置访问HDFS的用户名
                System.setProperty("HADOOP_USER_NAME", "root");
        */
        // 2. 读取kafka数据创建流
        String groupId = "visitor_stats_app";

        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";

        DataStreamSource<String> uv$ = env.addSource(MyKafkaUtil.getKafkaConsumer(uniqueVisitSourceTopic, groupId));
        DataStreamSource<String> uj$ = env.addSource(MyKafkaUtil.getKafkaConsumer(userJumpDetailSourceTopic, groupId));
        DataStreamSource<String> pv$ = env.addSource(MyKafkaUtil.getKafkaConsumer(pageViewSourceTopic, groupId));

        // 3. 将每个流处理成相同的数据类型
        // 3.1 处理uv数据
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUv$ = uv$.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            // 提取公共字段
            JSONObject common = jsonObject.getJSONObject("common");
            return new VisitorStats("", "", common.getString("vc"), common.getString("ch"),
                    common.getString("ar"), common.getString("is_new"), 1L, 0L, 0L,
                    0L, 0L, jsonObject.getLong("ts"));
        });
        // 3.2 处理uj数据
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUj$ = uj$.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            // 提取公共字段
            JSONObject common = jsonObject.getJSONObject("common");
            return new VisitorStats("", "", common.getString("vc"), common.getString("ch"),
                    common.getString("ar"), common.getString("is_new"), 0L, 0L, 0L,
                    1L, 0L, jsonObject.getLong("ts"));
        });
        // 3.3 处理pv数据
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithPv$ = pv$.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            // 提取公共字段
            JSONObject common = jsonObject.getJSONObject("common");
            // 获取页面信息
            JSONObject page = jsonObject.getJSONObject("page");
            String last_page_id = page.getString("last_page_id");
            long sv = 0L;
            if (last_page_id == null || last_page_id.length() <= 0) {
                sv = 1L;
            }
            return new VisitorStats("", "", common.getString("vc"), common.getString("ch"),
                    common.getString("ar"), common.getString("is_new"), 0L, 1L, sv,
                    0L, page.getLong("during_time"), jsonObject.getLong("ts"));
        });
        // 4. union 几个流
        DataStream<VisitorStats> union$ = visitorStatsWithUv$.union(visitorStatsWithUj$, visitorStatsWithPv$);
        // 5. 提取时间戳生成waterMark
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWaterMark$ = union$.assignTimestampsAndWatermarks(WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(11))
                .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                    @Override
                    public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));
        // 6. 按照维度信息分组
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedStream = visitorStatsWithWaterMark$.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                return new Tuple4<>(value.getAr(), value.getCh(), value.getIs_new(), value.getVc());
            }
        });
        // 7. 开窗聚合 10s的滚动窗口
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));
        SingleOutputStreamOperator<VisitorStats> result = windowedStream.reduce(new ReduceFunction<VisitorStats>() {
            @Override
            public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                value1.setUv_ct(value1.getUv_ct() + value2.getUv_ct());
                value1.setPv_ct(value1.getPv_ct() + value2.getPv_ct());
                value1.setSv_ct(value1.getSv_ct() + value2.getSv_ct());
                value1.setUj_ct(value1.getUj_ct() + value2.getUj_ct());
                value1.setDur_sum(value1.getDur_sum() + value2.getDur_sum());
                return value1;
            }
        }, new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<VisitorStats> input, Collector<VisitorStats> out) throws Exception {
                long start = window.getStart();
                long end = window.getEnd();
                VisitorStats visitorStats = input.iterator().next();
                // 补充窗口信息
                visitorStats.setStt(DateTimeUtil.toYMDhms(new Date(start)));
                visitorStats.setEdt(DateTimeUtil.toYMDhms(new Date(end)));
                out.collect(visitorStats);
            }
        });
        // 8. 将数据写入clickHouse
        result.print(">>>>>>");
        // 9. 启动任务
        env.execute("VisitorStatsApp");

    }
}
