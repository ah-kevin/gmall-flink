package com.bjke.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bjke.app.function.DimAsyncFunction;
import com.bjke.bean.OrderDetail;
import com.bjke.bean.OrderInfo;
import com.bjke.bean.OrderWide;
import com.bjke.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

/**
 * 数据流: web/app -> nginx -> springboot -> Mysql -> FlinkApp -> kafka(ods)-> FlinkApp -> kafka/Hbase(dwd-dim)->FlinkApp(redis) ->kafka(dwm)
 * <p>
 * 程  序: --------------------mockDb  -> Mysql -> FlinkCDC -> kafka(zk) -> BaseLogApp -> kafka/Phoenix(zk/hdfs/hbase) -> OrderWideApp -> kafka
 */
public class OrderWideApp {
    public static void main(String[] args) throws Exception {
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

        //2.读取 Kafka 订单和订单明细主题数据 dwd_order_info dwd_order_detail。转换为javabean对象&提取时间戳生成watermark
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group";
        SingleOutputStreamOperator<OrderInfo> orderInfoDS$ = env.addSource(MyKafkaUtil.getKafkaConsumer(orderInfoSourceTopic, groupId))
                .map(line -> {
                    OrderInfo orderInfo = JSON.parseObject(line, OrderInfo.class);
                    String create_time = orderInfo.getCreate_time();
                    String[] dateTimeArr = create_time.split(" ");
                    orderInfo.setCreate_date(dateTimeArr[0]);
                    orderInfo.setCreate_hour(dateTimeArr[1].split(":")[0]);
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    orderInfo.setCreate_ts(sdf.parse(create_time).getTime());
                    return orderInfo;
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                    @Override
                    public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                }));
        SingleOutputStreamOperator<OrderDetail> orderDetailDS$ = env.addSource(MyKafkaUtil.getKafkaConsumer(orderDetailSourceTopic, groupId))
                .map(line -> {
                    OrderDetail orderDetail = JSON.parseObject(line, OrderDetail.class);
                    String create_time = orderDetail.getCreate_time();
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    orderDetail.setCreate_ts(sdf.parse(create_time).getTime());
                    return orderDetail;
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                    @Override
                    public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                }));

        // 3 双流join
        SingleOutputStreamOperator<OrderWide> orderWideWithNoDimDS = orderInfoDS$.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailDS$.keyBy(OrderDetail::getOrder_id))
                .between(Time.seconds(-5), Time.seconds(5)) // 生成环境中给的时间给的最大延迟时间
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo left, OrderDetail right, ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>.Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(left, right));
                    }
                });
        orderWideWithNoDimDS.print("orderWideWithNoDimDS>>>>>>");

        // 4 关联维度信息 hbase phoenix
//        orderWideWithNoDimDS.map(orderWide -> {
//            // 关联用户维度
//            Long user_id = orderWide.getUser_id();
//            // 根据user_id查询Phoenix用户信息
//
//            // 将用户信息补充至orderWide
//            // 地区
//            // SKU
//            // SPU
//            // 。。。
//            // 返回结果
//            return orderWide;
//        });

        // 4.1 关联用户维度
        SingleOutputStreamOperator<OrderWide> orderWideWIthUserDS$ = AsyncDataStream.unorderedWait(orderWideWithNoDimDS,
                new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public String getKey(OrderWide input) {
                        return input.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws Exception {
                        orderWide.setUser_gender(dimInfo.getString("GENDER"));
                        String birthday = dimInfo.getString("BIRTHDAY");
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        long currentTimeMillis = System.currentTimeMillis();
                        long ts = sdf.parse(birthday).getTime();
                        long age = (currentTimeMillis - ts) / (1000 * 60 * 60 * 24 * 365L);
                        orderWide.setUser_age((int) age);
                    }
                },
                60, TimeUnit.SECONDS);
        // 4.2 关联地区维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS$ = AsyncDataStream.unorderedWait(orderWideWIthUserDS$, new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
            @Override
            public String getKey(OrderWide orderWide) {
                return orderWide.getProvince_id().toString();
            }

            @Override
            public void join(OrderWide orderWide, JSONObject dimInfo) throws Exception {
                orderWide.setProvince_name(dimInfo.getString("NAME"));
                orderWide.setProvince_area_code(dimInfo.getString("AREA_CODE"));
                orderWide.setProvince_iso_code(dimInfo.getString("ISO_CODE"));
                orderWide.setProvince_3166_2_code(dimInfo.getString("ISO_3166_2"));
            }
        }, 60, TimeUnit.SECONDS);
        // 4.3 关联SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS$ = AsyncDataStream.unorderedWait(
                orderWideWithProvinceDS$, new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setSku_name(jsonObject.getString("SKU_NAME"));
                        orderWide.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                        orderWide.setSpu_id(jsonObject.getLong("SPU_ID"));
                        orderWide.setTm_id(jsonObject.getLong("TM_ID"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSku_id());
                    }
                }, 60, TimeUnit.SECONDS);
        // 4.4 关联SPU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS$ = AsyncDataStream.unorderedWait(
                orderWideWithSkuDS$, new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setSpu_name(jsonObject.getString("SPU_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSpu_id());
                    }
                }, 60, TimeUnit.SECONDS);
        // 4.5 关联TM品牌维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS$ = AsyncDataStream.unorderedWait(
                orderWideWithSpuDS$, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }
                }, 60, TimeUnit.SECONDS);
        // 4.6 关联Category维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(
                orderWideWithTmDS$, new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setCategory3_name(jsonObject.getString("NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getCategory3_id());
                    }
                }, 60, TimeUnit.SECONDS);
        orderWideWithCategory3DS.print("orderWideWithCategory3DS>>>>>");
        // 5 写入kafka
        orderWideWithCategory3DS.map(JSONObject::toJSONString)
                        .addSink(MyKafkaUtil.getKafkaProducer(orderWideSinkTopic));
        env.execute("OrderWideApp");
    }
}
