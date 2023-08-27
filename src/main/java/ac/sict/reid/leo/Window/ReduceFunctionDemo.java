package ac.sict.reid.leo.Window;

import ac.sict.reid.leo.Computing.Aggregation.WaterSensorMapFunction;
import ac.sict.reid.leo.POJO.WaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.common.functions.ReduceFunction;

public class ReduceFunctionDemo<W> {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        environment.socketTextStream("localhost",7777).
                map(new WaterSensorMapFunction()).keyBy(r -> r.getId()).
                window(TumblingProcessingTimeWindows.of(Time.seconds(10))).
                reduce(new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor t0, WaterSensor t1) throws Exception {
                        System.out.println("调用reduce方法，之前结果："+t0+"，现在来的数据："+t1);
                        return new WaterSensor(t0.getId(),System.currentTimeMillis(),t0.getVc()+t1.getVc());
                    }
                }).print();

        environment.execute();

    }

}
