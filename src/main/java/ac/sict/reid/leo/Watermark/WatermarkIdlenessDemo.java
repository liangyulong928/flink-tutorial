package ac.sict.reid.leo.Watermark;

import ac.sict.reid.leo.Computing.PhysicalPartition.Computing.MyPartitioner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class WatermarkIdlenessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);

        SingleOutputStreamOperator<Integer> sockerDS = environment.socketTextStream("localhost", 7777).
                partitionCustom(new MyPartitioner(), r -> r).
                map(r -> Integer.parseInt(r)).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Integer>forMonotonousTimestamps().
                                withTimestampAssigner((r, ts) -> r * 1000L).withIdleness(Duration.ofSeconds(5))
                );

        sockerDS.keyBy(r -> r % 2).window(TumblingEventTimeWindows.of(Time.seconds(10))).
                process(new ProcessWindowFunction<Integer, String, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer integer, ProcessWindowFunction<Integer, String, Integer, TimeWindow>.Context context, Iterable<Integer> iterable, Collector<String> collector) throws Exception {
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        long count = iterable.spliterator().estimateSize();
                        collector.collect("key = "+integer+" 的窗口[" + start+" , "+ end +")包含 " + count + " 条数据 ===>" +iterable.toString());
                    }
                }).print();

        environment.execute();

    }
}
