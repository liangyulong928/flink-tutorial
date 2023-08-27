package ac.sict.reid.leo.Computing.Transformation;

import ac.sict.reid.leo.POJO.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flatmap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaterSensor> stream = environment.fromElements(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_1", 2L, 2),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_3", 3L, 3)
        );
        stream.flatMap(new FlatMapFunction<WaterSensor, Object>() {
            @Override
            public void flatMap(WaterSensor waterSensor, Collector<Object> collector) throws Exception {
                if (waterSensor.id.equals("sensor_1")){
                    collector.collect(String.valueOf(waterSensor.vc));
                } else if (waterSensor.id.equals("sensor_2")) {
                    collector.collect(String.valueOf(waterSensor.ts));
                    collector.collect(String.valueOf(waterSensor.vc));
                }
            }
        }).print();
        environment.execute();
    }

//    public static class UserFlatmap implements FlatMapFunction<WaterSensor,String> {
//
//        @Override
//        public void flatMap(WaterSensor waterSensor, Collector<String> collector) throws Exception {
//            if (waterSensor.id.equals("sensor_1")){
//                collector.collect(String.valueOf(waterSensor.vc));
//            } else if (waterSensor.id.equals("sensor_2")) {
//                collector.collect(String.valueOf(waterSensor.ts));
//                collector.collect(String.valueOf(waterSensor.vc));
//            }
//        }
//    }
}
