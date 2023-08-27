package ac.sict.reid.leo.Computing.Transformation;

import ac.sict.reid.leo.POJO.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapImplClass {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaterSensor> stream = environment.fromElements(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_2", 2L, 2)
        );

        stream.map(new UserMap()).print();

        environment.execute();
    }

    public static class UserMap implements MapFunction<WaterSensor,String>{

        @Override
        public String map(WaterSensor waterSensor) throws Exception {
            return waterSensor.id;
        }
    }
}
