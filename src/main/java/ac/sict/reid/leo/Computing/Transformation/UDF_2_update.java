package ac.sict.reid.leo.Computing.Transformation;

import ac.sict.reid.leo.POJO.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UDF_2_update {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaterSensor> stream = environment.fromElements(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_1", 2L, 2),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_3", 3L, 3)
        );
        SingleOutputStreamOperator<WaterSensor> filter = stream.filter(new FilterFunctionImpl("sensor_1"));

        filter.print();
        environment.execute();
    }

    public static class FilterFunctionImpl implements FilterFunction<WaterSensor>{

        private String id;

        public FilterFunctionImpl(String id) {
            this.id = id;
        }

        @Override
        public boolean filter(WaterSensor waterSensor) throws Exception {
            return this.id.equals(waterSensor.id);
        }
    }
}
