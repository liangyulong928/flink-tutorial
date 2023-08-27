package ac.sict.reid.leo.Window;

import ac.sict.reid.leo.Computing.Aggregation.WaterSensorMapFunction;
import ac.sict.reid.leo.POJO.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowAggregateAndProcessDemo {

    /**
     * @param args
     *
     * 思路：基于增量聚合函数处理窗口数据，每来一个数据就做一次聚合；等窗口需要触发计算，调用全窗口函数处理逻辑输出结果
     *
     * 此时，全窗口函数不再缓存所有数据，直接将增量聚合函数结果作为Iterable类型输入
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> sensor = environment.socketTextStream("localhost", 7777).map(new WaterSensorMapFunction());
        KeyedStream<WaterSensor, String> keyedStream = sensor.keyBy(value -> value.id);

        WindowedStream<WaterSensor, String, TimeWindow> windowed = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        /*
        * 窗口函数
        *
        * 增量聚合 Aggregate + 全窗口 process
        *    1、增量聚合函数处理数据：来一条计算一条
        *    2、窗口触发时，增量聚合的结果（只有一条）传递给全窗口函数
        *    3、经过全窗口函数的处理包装后，输出
        *
        * 结合两者优点：
        *   1、增量聚合：即时计算，存储中间结果，占用空间少
        *   2、全窗口函数：通过上下文实现灵活功能
        * */

        SingleOutputStreamOperator<String> aggregate = windowed.aggregate(new MyAgg(), new MyProcess());

        aggregate.print();

        environment.execute();


    }

    public static class MyAgg implements AggregateFunction<WaterSensor,Integer,String> {

        @Override
        public Integer createAccumulator() {
            System.out.println("创建累加器..");
            return 0;
        }

        @Override
        public Integer add(WaterSensor waterSensor, Integer integer) {
            System.out.println("调用add方法，value=" + waterSensor);
            return integer + waterSensor.getVc();
        }

        @Override
        public String getResult(Integer integer) {
            System.out.println("调用getResult方法：");
            return integer.toString();
        }

        @Override
        public Integer merge(Integer integer, Integer acc1) {
            System.out.println("调用merge方法..");
            return null;
        }
    }

    public static class MyProcess extends ProcessWindowFunction<String, String, String, TimeWindow> {

        @Override
        public void process(String s, ProcessWindowFunction<String, String, String, TimeWindow>.Context context, Iterable<String> iterable, Collector<String> collector) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            long count = iterable.spliterator().estimateSize();

            collector.collect("key = "+ s + "的窗口[" + start + "," + end + ") 包含 " + count + "条数据 ====>" + iterable.toString());
        }
    }
}
