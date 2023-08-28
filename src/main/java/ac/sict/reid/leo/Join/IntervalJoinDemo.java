package ac.sict.reid.leo.Join;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class IntervalJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Integer>> ds1 = environment.fromElements(
                Tuple2.of("a", 1), Tuple2.of("a", 2), Tuple2.of("b", 3), Tuple2.of("c", 4)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Integer>>forMonotonousTimestamps().
                withTimestampAssigner((value, ts) -> value.f1 * 1000L));

        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> ds2 = environment.fromElements(
                Tuple3.of("a", 1, 1), Tuple3.of("a", 11, 1), Tuple3.of("b", 2, 1),
                Tuple3.of("b", 12, 1), Tuple3.of("c", 14, 1), Tuple3.of("d", 15, 1)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Integer, Integer>>forMonotonousTimestamps().
                withTimestampAssigner((value, ts) -> value.f1 * 1000L));

        /*
        * TODO interval join
        *   1. 分别做keyBy, key ----> 解决关联条件
        *   2. 调用interval join
        * */
        KeyedStream<Tuple2<String, Integer>, String> ks1 = ds1.keyBy(r1 -> r1.f0);
        KeyedStream<Tuple3<String, Integer, Integer>, String> ks2 = ds2.keyBy(r2 -> r2.f0);

        ks1.intervalJoin(ks2).between(Time.seconds(-2),Time.seconds(2)).process(
                new ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                    /**
                     *
                     * 两条流数据匹配上调用该方法
                     *
                     * @param stringIntegerTuple2                   ks1 data
                     * @param stringIntegerIntegerTuple3            ks2 data
                     * @param context                               上下文
                     * @param collector                             采集器
                     * @throws Exception
                     */
                    @Override
                    public void processElement(Tuple2<String, Integer> stringIntegerTuple2, Tuple3<String, Integer, Integer> stringIntegerIntegerTuple3, ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>.Context context, Collector<String> collector) throws Exception{
                        collector.collect(stringIntegerTuple2 + "<------------>" + stringIntegerIntegerTuple3);
                    }
                }
        ).print();

        environment.execute();

    }
}
