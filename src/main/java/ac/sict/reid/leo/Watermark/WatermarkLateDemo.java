package ac.sict.reid.leo.Watermark;

import ac.sict.reid.leo.Computing.Aggregation.WaterSensorMapFunction;
import ac.sict.reid.leo.POJO.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class WatermarkLateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensor = environment.socketTextStream("localhost", 7777).
                map(new WaterSensorMapFunction());

        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy.
                <WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(
                (element, recordTimestrap) -> (element.getTs() * 1000L)
        );
        SingleOutputStreamOperator<WaterSensor> sensorWithWatermark = sensor.assignTimestampsAndWatermarks(watermarkStrategy);

        OutputTag<WaterSensor> lateTag = new OutputTag<>("late-data", Types.POJO(WaterSensor.class));
        SingleOutputStreamOperator<String> process = sensorWithWatermark.keyBy(sensorDocker -> sensorDocker.getId()).
                window(TumblingEventTimeWindows.of(Time.seconds(2))).sideOutputLateData(lateTag).process(
                        new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                            @Override
                            public void process(String s,
                                                ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context,
                                                Iterable<WaterSensor> iterable, Collector<String> collector) throws Exception {
                                long start = context.window().getStart();
                                long end = context.window().getEnd();
                                long count = iterable.spliterator().estimateSize();
                                collector.collect("key = " + s + " 的窗口[" + start + " , " + end + ")包含 " + count + " 条数据 ===>" + iterable.toString());
                            }
                        }
                );

        process.print();
        process.getSideOutput(lateTag).printToErr("关窗后的迟到数据");
        environment.execute();

    }
}
