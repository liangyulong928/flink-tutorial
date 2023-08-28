package ac.sict.reid.leo.Watermark;

import ac.sict.reid.leo.Computing.Aggregation.WaterSensorMapFunction;
import ac.sict.reid.leo.POJO.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class WatermarkOutOfOrdernessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensor = environment.socketTextStream("localhost", 7777).map(new WaterSensorMapFunction());

        //TODO 定义 Watermark 策略
        // 指定时间戳分配器，从数据中提取
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy.
                <WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3)).         // 指定watermark生成：升序、没有等待时间
                withTimestampAssigner((element, recordTimestrap) -> {
                    System.out.println("数据=" + element + ",recordTs = " + recordTimestrap);
                    return element.getTs() * 1000L;
                }
        );

        // TODO 指定 watermark 策略
        SingleOutputStreamOperator<WaterSensor> sensorWithWatermark = sensor.assignTimestampsAndWatermarks(watermarkStrategy);

        sensorWithWatermark.keyBy(value -> value.getId()).window(TumblingProcessingTimeWindows.of(Time.seconds(10))).process(
                new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> iterable, Collector<String> collector) throws Exception {
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        long count = iterable.spliterator().estimateSize();
                        collector.collect("key = "+s+" 的窗口[" + start+" , "+ end +")包含 " + count + " 条数据 ===>" +iterable.toString());
                    }
                }
        ).print();

        environment.execute();

    }
}
