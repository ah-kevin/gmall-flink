package com.bjke.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bjke.bean.OrderWide;
import com.bjke.bean.PaymentInfo;
import com.bjke.bean.PaymentWide;
import com.bjke.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
/**
 * 数据流: web/app -> nginx -> springboot -> Mysql -> FlinkApp -> kafka(ods)-> FlinkApp -> kafka/Hbase(dwd-dim)->FlinkApp(redis) ->kafka(dwm) -> flinkApp-> kafka(dwm)
 * <p>
 * 程  序: --------------------mockDb  -> Mysql -> FlinkCDC -> kafka(zk) -> BaseLogApp -> kafka/Phoenix(zk/hdfs/hbase) -> OrderWideApp -> kafka ->PaymentWideApp->kafka
 */
public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        // 1。获取执行环境
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
        // 2. 读取kafka主题的数据 dwd_payment_info dwm_order_wide 创建流 并转换为JavaBean对象 提取时间戳生成watermark
        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";

        SingleOutputStreamOperator<OrderWide> orderWide$ = env.addSource(MyKafkaUtil.getKafkaConsumer(orderWideSourceTopic, groupId))
                .map(line -> JSON.parseObject(line, OrderWide.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                    @Override
                    public long extractTimestamp(OrderWide element, long recordTimestamp) {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        try {
                            return sdf.parse(element.getCreate_time()).getTime();
                        } catch (ParseException e) {
                            e.printStackTrace();
                            return recordTimestamp;
                        }
                    }
                }));
        SingleOutputStreamOperator<PaymentInfo> paymentInfo$ = env.addSource(MyKafkaUtil.getKafkaConsumer(paymentInfoSourceTopic, groupId))
                .map(line -> JSON.parseObject(line, PaymentInfo.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                            @Override
                            public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                try {
                                    return sdf.parse(element.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    return recordTimestamp;
                                }
                            }
                        }));
        // 3. 双流JOIN
        SingleOutputStreamOperator<PaymentWide> paymentWide$ = paymentInfo$.keyBy(PaymentInfo::getOrder_id)
                .intervalJoin(orderWide$.keyBy(OrderWide::getOrder_id))
                .between(Time.minutes(-15), Time.seconds(5))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo left, OrderWide right, ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>.Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(left, right));
                    }
                });
        // 4. 将数据写入kafka
        paymentWide$.print(">>>>>");
        paymentWide$.map(JSONObject::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer(paymentWideSinkTopic));
        // 5. 启动任务
        env.execute("PaymentWideApp");
    }
}
