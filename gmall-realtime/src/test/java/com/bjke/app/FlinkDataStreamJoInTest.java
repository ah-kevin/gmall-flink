package com.bjke.app;


import com.bjke.bean.Bean1;
import com.bjke.bean.Bean2;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import scala.Tuple2;

public class FlinkDataStreamJoInTest {
    public static void main(String[] args) throws Exception {
        //1 读取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境应该设置为Kafka主题的分区数
        //2 读取两个端口数据创建流并提取时间戳生成WaterMark
        SingleOutputStreamOperator<Bean1> stream1$ = env.socketTextStream("hadoop1", 8888)
                .map(line -> {
                    String[] strings = line.split(",");
                    return new Bean1(strings[0], strings[1], Long.parseLong(strings[2]));
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<Bean1>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Bean1>() {
                    @Override
                    public long extractTimestamp(Bean1 element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                }));

        SingleOutputStreamOperator<Bean2> stream2$ = env.socketTextStream("hadoop2", 8888)
                .map(line -> {
                    String[] strings = line.split(",");
                    return new Bean2(strings[0], strings[1], Long.parseLong(strings[2]));
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<Bean2>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Bean2>() {
                    @Override
                    public long extractTimestamp(Bean2 element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                }));

        //3 双流join
        SingleOutputStreamOperator<Tuple2<Bean1, Bean2>> join$ = stream1$.keyBy(Bean1::getId)
                .intervalJoin(stream2$.keyBy(Bean2::getId))
                .between(Time.seconds(-5), Time.seconds(5))
//                .lowerBoundExclusive()
//                .upperBoundExclusive()
                .process(new ProcessJoinFunction<Bean1, Bean2, Tuple2<Bean1, Bean2>>() {
                    @Override
                    public void processElement(Bean1 left, Bean2 right, ProcessJoinFunction<Bean1, Bean2, Tuple2<Bean1, Bean2>>.Context ctx, Collector<Tuple2<Bean1, Bean2>> out) throws Exception {
                        out.collect(new Tuple2<>(left, right));
                    }
                });
        //4 打印
        join$.print();
        //5 启动任务
        env.execute();
    }

}
