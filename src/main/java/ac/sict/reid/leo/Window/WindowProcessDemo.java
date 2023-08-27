package ac.sict.reid.leo.Window;

import ac.sict.reid.leo.Computing.Aggregation.WaterSensorMapFunction;
import ac.sict.reid.leo.POJO.WaterSensor;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowProcessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> sensor = environment.socketTextStream("localhost", 7777).map(new WaterSensorMapFunction());
        KeyedStream<WaterSensor, String> keyedStream = sensor.keyBy(value -> value.id);

        // 窗口
        WindowedStream<WaterSensor, String, TimeWindow> windowed = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<String> process = windowed.process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
            @Override
            // 分析参数：s => 窗口的key , context => 上下文信息 , iterable => 窗口包含的数据 , collector => 输出收集器
            public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> iterable, Collector<String> collector) throws Exception {
                long count = iterable.spliterator().estimateSize();
                long start = context.window().getStart();
                long end = context.window().getEnd();
                collector.collect("key = " + s + " 的窗口[" + start + "," + end + "]包含 " + count + " 条数据 ====> " + iterable.toString());
            }
        });

        process.print();

        environment.execute();

    }
}
