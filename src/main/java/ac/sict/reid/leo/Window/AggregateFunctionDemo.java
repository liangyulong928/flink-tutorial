package ac.sict.reid.leo.Window;

import ac.sict.reid.leo.Computing.Aggregation.WaterSensorMapFunction;
import ac.sict.reid.leo.POJO.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class AggregateFunctionDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> stream = environment.socketTextStream("localhost", 7777).map(new WaterSensorMapFunction());
        KeyedStream<WaterSensor, String> keyedStream = stream.keyBy(sensor -> sensor.getId());
        WindowedStream<WaterSensor, String, TimeWindow> sensorKS = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        SingleOutputStreamOperator<String> aggregate = sensorKS.aggregate(new org.apache.flink.api.common.functions.AggregateFunction<WaterSensor, Integer, String>() {
            @Override
            public Integer createAccumulator() {
                System.out.println("创建累加器\n");
                return 0;
            }

            @Override
            public Integer add(WaterSensor waterSensor, Integer integer) {
                System.out.println("调用add方法，value = " + waterSensor + "\n");
                return integer + waterSensor.getVc();
            }

            @Override
            public String getResult(Integer integer) {
                System.out.println("调用getResult方法\n");
                return integer.toString();
            }

            @Override
            public Integer merge(Integer integer, Integer acc1) {
                System.out.println("调用merge方法\n");
                return null;
            }
        });

        aggregate.print();

        environment.execute();


    }

}
