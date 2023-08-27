package ac.sict.reid.leo.Computing.Transformation;

import ac.sict.reid.leo.POJO.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FilterImplClass {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaterSensor> stream = environment.fromElements(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_1", 2L, 2),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_3", 3L, 3)
        );
        stream.filter(new UserFilter()).print();

        environment.execute();
    }

    public static class UserFilter implements FilterFunction<WaterSensor>{

        @Override
        public boolean filter(WaterSensor waterSensor) throws Exception {
            return waterSensor.id.equals("sensor_1");
        }
    }
}
