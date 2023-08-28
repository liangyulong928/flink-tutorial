package ac.sict.reid.leo.Join;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/*
* 针对有迟到数据情况的间接联结
*
* */
public class IntervalJoinWithLateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Integer>> ds1 = environment.socketTextStream("localhost", 7777).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] split = s.split(",");
                return Tuple2.of(split[0], Integer.valueOf(split[1]));
            }
        }).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(3)).
                        withTimestampAssigner((value, ts) -> value.f1 * 1000L)
        );

        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> ds2 = environment.socketTextStream("localhost", 8888).map(new MapFunction<String, Tuple3<String, Integer, Integer>>() {
            @Override
            public Tuple3<String, Integer, Integer> map(String s) throws Exception {
                String[] split = s.split(",");
                return Tuple3.of(split[0], Integer.valueOf(split[1]), Integer.valueOf(split[2]));
            }
        }).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, Integer, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(3)).
                        withTimestampAssigner(
                                (value, ts) -> value.f1 * 1000L
                        )
        );

        /*
        * TODO Interval join
        *  1. 只支持事件时间
        *  2. 指定上界、下界的偏移，负号表示时间往前，正号表示时间往后
        *  3. process中只能处理 join 上的数据
        *  4. 两条流关联后的watermark，以两条流中最小的为准
        *  5. 如果当前数据的事件时间小于当前watermark，就是迟到数据，主流的process不处理
        *                ----> 将迟到数据放入侧输出流
        * */

        KeyedStream<Tuple2<String, Integer>, String> ks1 = ds1.keyBy(r -> r.f0);
        KeyedStream<Tuple3<String, Integer, Integer>, String> ks2 = ds2.keyBy(r -> r.f0);

        OutputTag<Tuple2<String,Integer>> ks1LateTag = new OutputTag<>("ks1-late", Types.TUPLE(Types.STRING, Types.INT));
        OutputTag<Tuple3<String,Integer,Integer>> ks2LateTag = new OutputTag<>("ks2-late", Types.TUPLE(Types.STRING, Types.INT, Types.INT));

        SingleOutputStreamOperator<String> process = ks1.intervalJoin(ks2).between(Time.seconds(-2), Time.seconds(2)).
                sideOutputLeftLateData(ks1LateTag).sideOutputRightLateData(ks2LateTag).process(
                        new ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                            @Override
                            public void processElement(Tuple2<String, Integer> stringIntegerTuple2, Tuple3<String, Integer, Integer> stringIntegerIntegerTuple3, ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>.Context context, Collector<String> collector) throws Exception {
                                collector.collect(stringIntegerTuple2 + "<---------->" + stringIntegerIntegerTuple3);
                            }
                        }
                );

        process.print("主流");
        process.getSideOutput(ks1LateTag).printToErr("ks1 迟到数据");
        process.getSideOutput(ks2LateTag).printToErr("ks2 迟到数据");

        environment.execute();
    }
}
